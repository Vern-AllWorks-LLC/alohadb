/*-------------------------------------------------------------------------
 *
 * restapi_http.c
 *	  HTTP server background worker for the alohadb_restapi extension.
 *
 *	  Implements a non-blocking TCP HTTP/1.1 server that accepts
 *	  connections, parses HTTP requests, dispatches them to the
 *	  REST-to-SQL handler, and sends back JSON responses.
 *
 *	  Connection: close is used for simplicity (no keep-alive).
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_restapi/restapi_http.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

/* POSIX networking */
#include <sys/socket.h>
#include <netinet/in.h>
#include <poll.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

/* Background worker essentials */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/latch.h"

/* SPI and transaction management */
#include "access/xact.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

#include "alohadb_restapi.h"

/* ----------------------------------------------------------------
 * Forward declarations
 * ---------------------------------------------------------------- */

static void restapi_conn_init(RestApiConn *conn, int fd);
static void restapi_conn_close(RestApiConn *conn);
static bool restapi_conn_read(RestApiConn *conn);
static bool restapi_conn_write(RestApiConn *conn);
static bool restapi_request_complete(RestApiConn *conn);
static void restapi_process_conn(RestApiConn *conn);
static int	set_nonblocking(int fd);
static const char *http_status_reason(int status);

/* ----------------------------------------------------------------
 * http_status_reason
 *
 * Map an HTTP status code to its standard reason phrase.
 * ---------------------------------------------------------------- */
static const char *
http_status_reason(int status)
{
	switch (status)
	{
		case 200:
			return "OK";
		case 201:
			return "Created";
		case 204:
			return "No Content";
		case 400:
			return "Bad Request";
		case 404:
			return "Not Found";
		case 405:
			return "Method Not Allowed";
		case 500:
			return "Internal Server Error";
		default:
			return "Unknown";
	}
}

/* ----------------------------------------------------------------
 * http_parse_request
 *
 * Parse a raw HTTP/1.1 request buffer into a RestApiRequest struct.
 * Extracts the method (GET/POST/PUT/DELETE/OPTIONS), path,
 * Content-Length header, X-PG-Role header, and body.
 *
 * Returns true if a complete request was successfully parsed,
 * false if more data is needed (incomplete headers or body).
 * ---------------------------------------------------------------- */
bool
http_parse_request(const char *raw, int raw_len, RestApiRequest *req)
{
	const char *headers_end;
	const char *line_end;
	const char *body_start;
	const char *p;
	int			method_len;
	int			path_len;
	const char *path_start;
	const char *path_end;

	Assert(req != NULL);
	memset(req, 0, sizeof(RestApiRequest));

	if (raw_len < 10)
		return false;

	/*
	 * Find the end of the headers: the blank line "\r\n\r\n" separates
	 * headers from body.
	 */
	headers_end = strstr(raw, "\r\n\r\n");
	if (!headers_end)
		return false;			/* incomplete headers, need more data */

	body_start = headers_end + 4;

	/*
	 * Parse the request line: "METHOD /path HTTP/1.x\r\n"
	 */
	line_end = strstr(raw, "\r\n");
	if (!line_end)
		return false;

	/* Extract method (up to first space) */
	p = raw;
	while (p < line_end && *p != ' ')
		p++;
	method_len = p - raw;
	if (method_len <= 0 || method_len >= (int) sizeof(req->method))
		return false;
	memcpy(req->method, raw, method_len);
	req->method[method_len] = '\0';

	/* Skip space after method */
	if (*p != ' ')
		return false;
	p++;

	/* Extract path (up to next space; stop at '?' for query strings) */
	path_start = p;
	while (p < line_end && *p != ' ' && *p != '?')
		p++;
	path_end = p;
	path_len = path_end - path_start;
	if (path_len <= 0 || path_len >= (int) sizeof(req->path))
		return false;
	memcpy(req->path, path_start, path_len);
	req->path[path_len] = '\0';

	/*
	 * Parse headers line by line.  We only care about Content-Length
	 * and X-PG-Role (case-insensitive comparison via pg_strncasecmp).
	 */
	req->content_length = 0;
	req->role[0] = '\0';

	p = line_end + 2;			/* skip past "\r\n" of the request line */

	while (p < headers_end)
	{
		const char *next_line = strstr(p, "\r\n");

		if (!next_line || next_line > headers_end)
			break;

		/* Content-Length: (15 chars) */
		if (pg_strncasecmp(p, "Content-Length:", 15) == 0)
		{
			const char *val = p + 15;

			while (val < next_line && *val == ' ')
				val++;
			req->content_length = atoi(val);
		}
		/* X-PG-Role: (10 chars) */
		else if (pg_strncasecmp(p, "X-PG-Role:", 10) == 0)
		{
			const char *val = p + 10;
			int			vlen;

			while (val < next_line && *val == ' ')
				val++;
			vlen = next_line - val;
			if (vlen >= NAMEDATALEN)
				vlen = NAMEDATALEN - 1;
			memcpy(req->role, val, vlen);
			req->role[vlen] = '\0';
		}

		p = next_line + 2;		/* advance past "\r\n" */
	}

	/*
	 * If Content-Length > 0, verify the full body has arrived and
	 * copy it into a palloc'd buffer.
	 */
	if (req->content_length > 0)
	{
		int		body_available = raw_len - (int) (body_start - raw);

		if (body_available < req->content_length)
			return false;		/* body not fully received yet */

		req->body = palloc(req->content_length + 1);
		memcpy(req->body, body_start, req->content_length);
		req->body[req->content_length] = '\0';
		req->body_len = req->content_length;
	}
	else
	{
		req->body = NULL;
		req->body_len = 0;
	}

	return true;
}

/* ----------------------------------------------------------------
 * http_format_response
 *
 * Build a complete HTTP/1.1 response string from a RestApiResponse.
 * The result is palloc'd; its length is stored in *out_len.
 *
 * Format:
 *   HTTP/1.1 <status> <reason>\r\n
 *   Content-Type: application/json\r\n
 *   Content-Length: <len>\r\n
 *   Connection: close\r\n
 *   Access-Control-Allow-Origin: *\r\n
 *   Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS\r\n
 *   Access-Control-Allow-Headers: Content-Type, X-PG-Role\r\n
 *   \r\n
 *   <body>
 * ---------------------------------------------------------------- */
char *
http_format_response(RestApiResponse *resp, int *out_len)
{
	StringInfoData buf;
	const char *reason;
	int			body_len;

	Assert(resp != NULL);
	Assert(out_len != NULL);

	reason = http_status_reason(resp->status_code);
	body_len = (resp->body != NULL) ? resp->body_len : 0;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "HTTP/1.1 %d %s\r\n"
					 "Content-Type: application/json\r\n"
					 "Content-Length: %d\r\n"
					 "Connection: close\r\n"
					 "Access-Control-Allow-Origin: *\r\n"
					 "Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS\r\n"
					 "Access-Control-Allow-Headers: Content-Type, X-PG-Role\r\n"
					 "\r\n",
					 resp->status_code, reason, body_len);

	if (body_len > 0)
		appendBinaryStringInfo(&buf, resp->body, body_len);

	*out_len = buf.len;
	return buf.data;
}

/* ----------------------------------------------------------------
 * Connection management helpers
 * ---------------------------------------------------------------- */

static void
restapi_conn_init(RestApiConn *conn, int fd)
{
	conn->fd = fd;
	conn->state = RESTAPI_CONN_READING;
	conn->read_buf = palloc(RESTAPI_READ_BUF_SIZE);
	conn->read_len = 0;
	conn->read_cap = RESTAPI_READ_BUF_SIZE;
	conn->write_buf = NULL;
	conn->write_len = 0;
	conn->write_pos = 0;
}

static void
restapi_conn_close(RestApiConn *conn)
{
	if (conn->fd >= 0)
	{
		close(conn->fd);
		conn->fd = -1;
	}
	conn->state = RESTAPI_CONN_DONE;
	if (conn->read_buf)
	{
		pfree(conn->read_buf);
		conn->read_buf = NULL;
	}
	if (conn->write_buf)
	{
		pfree(conn->write_buf);
		conn->write_buf = NULL;
	}
}

/*
 * Read data from a connection.  Returns true if we should continue,
 * false if the connection was closed or an error occurred.
 */
static bool
restapi_conn_read(RestApiConn *conn)
{
	ssize_t		n;

	/* Grow buffer if needed */
	if (conn->read_len >= conn->read_cap - 1)
	{
		if (conn->read_cap >= RESTAPI_MAX_REQUEST_SIZE)
			return false;	/* request too large */

		conn->read_cap *= 2;
		if (conn->read_cap > RESTAPI_MAX_REQUEST_SIZE)
			conn->read_cap = RESTAPI_MAX_REQUEST_SIZE;
		conn->read_buf = repalloc(conn->read_buf, conn->read_cap);
	}

	n = read(conn->fd, conn->read_buf + conn->read_len,
			 conn->read_cap - conn->read_len - 1);

	if (n > 0)
	{
		conn->read_len += n;
		conn->read_buf[conn->read_len] = '\0';
		return true;
	}
	else if (n == 0)
	{
		/* Connection closed by client */
		return false;
	}
	else
	{
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
			return true;	/* try again later */
		return false;		/* real error */
	}
}

/*
 * Write pending data to a connection.  Returns true if we should
 * continue (more data to write or EAGAIN), false if done or error.
 */
static bool
restapi_conn_write(RestApiConn *conn)
{
	ssize_t		n;
	int			remaining;

	remaining = conn->write_len - conn->write_pos;
	if (remaining <= 0)
	{
		conn->state = RESTAPI_CONN_DONE;
		return false;
	}

	n = write(conn->fd, conn->write_buf + conn->write_pos, remaining);

	if (n > 0)
	{
		conn->write_pos += n;
		if (conn->write_pos >= conn->write_len)
		{
			conn->state = RESTAPI_CONN_DONE;
			return false;	/* all written */
		}
		return true;		/* more to write */
	}
	else if (n == 0)
	{
		return true;		/* try again */
	}
	else
	{
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
			return true;
		return false;		/* real error */
	}
}

/*
 * Check if we have received a complete HTTP request.
 * For requests without a body, we need \r\n\r\n.
 * For requests with Content-Length, we need that many bytes after headers.
 */
static bool
restapi_request_complete(RestApiConn *conn)
{
	const char *headers_end;
	int			content_length;
	int			header_size;
	int			total_needed;

	if (conn->read_len <= 0)
		return false;

	headers_end = strstr(conn->read_buf, "\r\n\r\n");
	if (!headers_end)
		return false;

	/* Search for Content-Length in the headers */
	{
		const char *p = conn->read_buf;

		content_length = 0;

		while (p < headers_end)
		{
			const char *next_line = strstr(p, "\r\n");

			if (!next_line || next_line > headers_end)
				break;

			if (pg_strncasecmp(p, "Content-Length:", 15) == 0)
			{
				const char *val = p + 15;

				while (val < next_line && *val == ' ')
					val++;
				content_length = atoi(val);
				break;
			}
			p = next_line + 2;
		}
	}

	header_size = (headers_end - conn->read_buf) + 4;	/* include \r\n\r\n */
	total_needed = header_size + content_length;

	return (conn->read_len >= total_needed);
}

/*
 * Process a complete request on a connection: parse it, call the
 * handler within an SPI transaction, format the response.
 */
static void
restapi_process_conn(RestApiConn *conn)
{
	MemoryContext work_cxt;
	MemoryContext old_cxt;
	RestApiRequest req;
	RestApiResponse *resp;
	RestApiResponse error_resp;

	conn->state = RESTAPI_CONN_PROCESSING;

	work_cxt = AllocSetContextCreate(TopMemoryContext,
									 "restapi request",
									 ALLOCSET_DEFAULT_SIZES);
	old_cxt = MemoryContextSwitchTo(work_cxt);

	/* Parse the HTTP request */
	if (!http_parse_request(conn->read_buf, conn->read_len, &req))
	{
		/* Bad request */
		error_resp.status_code = 400;
		error_resp.body = pstrdup("{\"error\": \"malformed HTTP request\"}");
		error_resp.body_len = strlen(error_resp.body);
		resp = &error_resp;
	}
	else
	{
		/*
		 * Handle OPTIONS preflight requests for CORS.
		 */
		if (strcmp(req.method, "OPTIONS") == 0)
		{
			error_resp.status_code = 204;
			error_resp.body = pstrdup("");
			error_resp.body_len = 0;
			resp = &error_resp;
		}
		else
		{
			/* Use default role if no X-PG-Role header */
			if (req.role[0] == '\0' && restapi_default_role &&
				restapi_default_role[0] != '\0')
				strlcpy(req.role, restapi_default_role, NAMEDATALEN);

			/*
			 * Process the request within an SPI transaction.
			 */
			PG_TRY();
			{
				SetCurrentStatementStartTimestamp();
				StartTransactionCommand();
				SPI_connect();
				PushActiveSnapshot(GetTransactionSnapshot());
				pgstat_report_activity(STATE_RUNNING,
									   "alohadb_restapi: handling request");

				resp = restapi_handle_request(&req);

				SPI_finish();
				PopActiveSnapshot();
				CommitTransactionCommand();
			}
			PG_CATCH();
			{
				/* On error, abort and return a 500 */
				EmitErrorReport();
				FlushErrorState();

				AbortCurrentTransaction();

				error_resp.status_code = 500;
				error_resp.body = pstrdup("{\"error\": \"internal server error\"}");
				error_resp.body_len = strlen(error_resp.body);
				resp = &error_resp;
			}
			PG_END_TRY();

			pgstat_report_activity(STATE_IDLE, NULL);
		}
	}

	/*
	 * Format the HTTP response and queue it for writing.
	 * Copy into TopMemoryContext so it survives context reset.
	 */
	{
		char   *formatted;
		int		formatted_len;

		formatted = http_format_response(resp, &formatted_len);

		MemoryContextSwitchTo(TopMemoryContext);
		conn->write_buf = palloc(formatted_len);
		memcpy(conn->write_buf, formatted, formatted_len);
		conn->write_len = formatted_len;
		conn->write_pos = 0;
	}

	conn->state = RESTAPI_CONN_WRITING;

	MemoryContextSwitchTo(old_cxt);
	MemoryContextDelete(work_cxt);
}

/*
 * Set a file descriptor to non-blocking mode.
 */
static int
set_nonblocking(int fd)
{
	int		flags;

	flags = fcntl(fd, F_GETFL, 0);
	if (flags < 0)
		return -1;
	return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/* ----------------------------------------------------------------
 * alohadb_restapi_worker_main
 *
 * Background worker entry point.  Creates a TCP listening socket
 * and runs an event loop using poll() to accept connections, read
 * HTTP requests, dispatch to the handler, and send responses.
 * ---------------------------------------------------------------- */
void
alohadb_restapi_worker_main(Datum main_arg)
{
	int					listen_fd = -1;
	struct sockaddr_in	addr;
	int					opt_val = 1;
	RestApiConn			conns[RESTAPI_MAX_CONNECTIONS];
	int					nconns = 0;
	struct pollfd		pollfds[RESTAPI_MAX_CONNECTIONS + 1];
	int					i;

	/* Establish signal handlers */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/* Connect to the configured database */
	BackgroundWorkerInitializeConnection(
		restapi_database ? restapi_database : "postgres", NULL, 0);

	elog(LOG, "alohadb_restapi: background worker started, "
		 "port = %d, database = \"%s\", schema = \"%s\"",
		 restapi_port,
		 restapi_database ? restapi_database : "postgres",
		 restapi_schema ? restapi_schema : "public");

	/* Initialize connection slots */
	for (i = 0; i < RESTAPI_MAX_CONNECTIONS; i++)
	{
		conns[i].fd = -1;
		conns[i].state = RESTAPI_CONN_DONE;
		conns[i].read_buf = NULL;
		conns[i].write_buf = NULL;
	}

	/*
	 * Create TCP listening socket.
	 */
	listen_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (listen_fd < 0)
		ereport(FATAL,
				(errmsg("alohadb_restapi: could not create socket: %m")));

	if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR,
				   &opt_val, sizeof(opt_val)) < 0)
		ereport(FATAL,
				(errmsg("alohadb_restapi: setsockopt SO_REUSEADDR failed: %m")));

	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_ANY);
	addr.sin_port = htons((uint16_t) restapi_port);

	if (bind(listen_fd, (struct sockaddr *) &addr, sizeof(addr)) < 0)
		ereport(FATAL,
				(errmsg("alohadb_restapi: could not bind to port %d: %m",
						restapi_port)));

	if (listen(listen_fd, RESTAPI_LISTEN_BACKLOG) < 0)
		ereport(FATAL,
				(errmsg("alohadb_restapi: listen() failed: %m")));

	if (set_nonblocking(listen_fd) < 0)
		ereport(FATAL,
				(errmsg("alohadb_restapi: could not set non-blocking: %m")));

	elog(LOG, "alohadb_restapi: HTTP server listening on port %d",
		 restapi_port);

	/*
	 * Main event loop.
	 */
	for (;;)
	{
		int		nfds;
		int		poll_result;
		int		slot;

		CHECK_FOR_INTERRUPTS();

		/* Reload configuration on SIGHUP */
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
			elog(LOG, "alohadb_restapi: configuration reloaded, "
				 "port = %d, schema = \"%s\"",
				 restapi_port,
				 restapi_schema ? restapi_schema : "public");
		}

		/*
		 * Build poll descriptor array.
		 * Slot 0 is always the listen socket.
		 */
		memset(pollfds, 0, sizeof(pollfds));
		pollfds[0].fd = listen_fd;
		pollfds[0].events = POLLIN;
		nfds = 1;

		for (i = 0; i < RESTAPI_MAX_CONNECTIONS; i++)
		{
			if (conns[i].fd < 0)
				continue;

			pollfds[nfds].fd = conns[i].fd;
			pollfds[nfds].events = 0;

			if (conns[i].state == RESTAPI_CONN_READING)
				pollfds[nfds].events |= POLLIN;
			else if (conns[i].state == RESTAPI_CONN_WRITING)
				pollfds[nfds].events |= POLLOUT;

			nfds++;
		}

		poll_result = poll(pollfds, nfds, RESTAPI_POLL_TIMEOUT_MS);

		if (poll_result < 0)
		{
			if (errno == EINTR)
				continue;
			ereport(LOG,
					(errmsg("alohadb_restapi: poll() error: %m")));
			continue;
		}

		if (poll_result == 0)
			continue;		/* timeout, check signals */

		/*
		 * Check for new connections on the listen socket.
		 */
		if (pollfds[0].revents & POLLIN)
		{
			struct sockaddr_in client_addr;
			socklen_t	client_len = sizeof(client_addr);
			int			client_fd;

			client_fd = accept(listen_fd,
							   (struct sockaddr *) &client_addr,
							   &client_len);

			if (client_fd >= 0)
			{
				/* Find a free connection slot */
				slot = -1;
				for (i = 0; i < RESTAPI_MAX_CONNECTIONS; i++)
				{
					if (conns[i].fd < 0)
					{
						slot = i;
						break;
					}
				}

				if (slot >= 0)
				{
					if (set_nonblocking(client_fd) < 0)
					{
						elog(WARNING,
							 "alohadb_restapi: could not set client non-blocking: %m");
						close(client_fd);
					}
					else
					{
						MemoryContext old = MemoryContextSwitchTo(TopMemoryContext);

						restapi_conn_init(&conns[slot], client_fd);
						nconns++;
						MemoryContextSwitchTo(old);
					}
				}
				else
				{
					/* No free slots */
					elog(WARNING,
						 "alohadb_restapi: max connections reached, rejecting");
					close(client_fd);
				}
			}
			else if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
			{
				elog(WARNING, "alohadb_restapi: accept() error: %m");
			}
		}

		/*
		 * Process active connections.
		 * We need to match pollfds back to connection slots.
		 */
		{
			int		pfd_idx = 1;	/* skip listen socket */

			for (i = 0; i < RESTAPI_MAX_CONNECTIONS && pfd_idx < nfds; i++)
			{
				if (conns[i].fd < 0)
					continue;

				/* This connection corresponds to pollfds[pfd_idx] */
				if (pollfds[pfd_idx].fd != conns[i].fd)
				{
					/*
					 * Safety check: pollfds should be in same order as
					 * conns iteration.  If not, skip.
					 */
					pfd_idx++;
					continue;
				}

				/* Handle readable connections */
				if ((pollfds[pfd_idx].revents & POLLIN) &&
					conns[i].state == RESTAPI_CONN_READING)
				{
					if (!restapi_conn_read(&conns[i]))
					{
						restapi_conn_close(&conns[i]);
						nconns--;
						pfd_idx++;
						continue;
					}

					/* Check if we have a complete request */
					if (restapi_request_complete(&conns[i]))
					{
						restapi_process_conn(&conns[i]);
					}
				}

				/* Handle writable connections */
				if ((pollfds[pfd_idx].revents & POLLOUT) &&
					conns[i].state == RESTAPI_CONN_WRITING)
				{
					if (!restapi_conn_write(&conns[i]))
					{
						if (conns[i].state == RESTAPI_CONN_DONE)
						{
							restapi_conn_close(&conns[i]);
							nconns--;
						}
					}
				}

				/* Handle errors */
				if (pollfds[pfd_idx].revents & (POLLERR | POLLHUP | POLLNVAL))
				{
					restapi_conn_close(&conns[i]);
					nconns--;
				}

				pfd_idx++;
			}
		}

		/*
		 * Clean up any connections in DONE state that weren't caught above.
		 */
		for (i = 0; i < RESTAPI_MAX_CONNECTIONS; i++)
		{
			if (conns[i].fd >= 0 && conns[i].state == RESTAPI_CONN_DONE)
			{
				restapi_conn_close(&conns[i]);
				nconns--;
			}
		}
	}

	/*
	 * Shutdown: close all connections and the listen socket.
	 * (Reached only via proc_exit callbacks, not normally.)
	 */
	for (i = 0; i < RESTAPI_MAX_CONNECTIONS; i++)
	{
		if (conns[i].fd >= 0)
			restapi_conn_close(&conns[i]);
	}

	if (listen_fd >= 0)
		close(listen_fd);
}
