/*-------------------------------------------------------------------------
 *
 * http_client.c
 *	  HTTP client functions for the alohadb_http extension.
 *
 *	  Implements synchronous HTTP/1.1 requests using POSIX sockets.
 *	  HTTPS is not supported; attempts to use https:// URLs will
 *	  raise an ERROR directing the user to use http:// instead.
 *
 *	  Each SQL function (http_get, http_post, etc.) returns a single
 *	  row via a materialized SRF containing (status, response_headers,
 *	  body).
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_http/http_client.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <poll.h>
#include <fcntl.h>
#include <errno.h>

#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"

#include "alohadb_http.h"

/* ----------------------------------------------------------------
 * PG_FUNCTION_INFO_V1 declarations -- MUST be in the same file
 * as the function implementations.
 * ---------------------------------------------------------------- */

PG_FUNCTION_INFO_V1(http_get);
PG_FUNCTION_INFO_V1(http_post);
PG_FUNCTION_INFO_V1(http_put);
PG_FUNCTION_INFO_V1(http_delete);
PG_FUNCTION_INFO_V1(http_request);

/* ----------------------------------------------------------------
 * Forward declarations for internal helpers
 * ---------------------------------------------------------------- */

static void http_check_enabled(void);
static int	http_effective_timeout(int requested_ms);
static void http_build_result(FunctionCallInfo fcinfo, HttpResponse *resp);
static int	http_connect_with_timeout(const char *host, int port, int timeout_ms);
static void http_set_nonblocking(int sockfd);
static char *http_read_response(int sockfd, int timeout_ms, int *response_len);
static void http_parse_response(const char *raw, int raw_len,
								int *status_code,
								StringInfo headers_json,
								char **body, int *body_len);
static char *http_parse_chunked_body(const char *raw, int raw_len, int *out_len);

/* ----------------------------------------------------------------
 * jsonb_out -- we need the external declaration to convert jsonb
 * arguments to C strings.
 * ---------------------------------------------------------------- */

extern Datum jsonb_out(PG_FUNCTION_ARGS);
extern Datum jsonb_in(PG_FUNCTION_ARGS);

/* ================================================================
 * URL PARSING
 * ================================================================ */

/*
 * http_parse_url
 *
 * Parse a URL string into its component parts.  Only http:// and https://
 * schemes are recognized.  The returned ParsedURL contains palloc'd strings.
 */
ParsedURL
http_parse_url(const char *url)
{
	ParsedURL	result;
	const char *p;
	const char *host_start;
	const char *host_end;
	const char *port_start;
	const char *path_start;
	char		port_buf[8];
	int			port_len;

	memset(&result, 0, sizeof(ParsedURL));
	result.port = 80;
	result.is_https = false;

	/* Check scheme */
	if (strncmp(url, "https://", 8) == 0)
	{
		result.is_https = true;
		result.port = 443;
		p = url + 8;
	}
	else if (strncmp(url, "http://", 7) == 0)
	{
		result.is_https = false;
		result.port = 80;
		p = url + 7;
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid URL scheme"),
				 errdetail("URL must start with http:// or https://")));
	}

	/* Extract host (and optional port) */
	host_start = p;

	/* Find end of host: either ':', '/', or end of string */
	host_end = p;
	while (*host_end != '\0' && *host_end != ':' && *host_end != '/')
		host_end++;

	if (host_end == host_start)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("empty hostname in URL")));

	result.host = pnstrdup(host_start, host_end - host_start);

	/* Parse optional port */
	if (*host_end == ':')
	{
		port_start = host_end + 1;
		p = port_start;
		while (*p >= '0' && *p <= '9')
			p++;

		port_len = p - port_start;
		if (port_len == 0 || port_len > 5)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid port number in URL")));

		memcpy(port_buf, port_start, port_len);
		port_buf[port_len] = '\0';
		result.port = atoi(port_buf);

		if (result.port <= 0 || result.port > 65535)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("port number out of range: %d", result.port)));

		path_start = p;
	}
	else
	{
		path_start = host_end;
	}

	/* Extract path (default to "/" if empty) */
	if (*path_start == '\0' || *path_start == '\0')
		result.path = pstrdup("/");
	else
		result.path = pstrdup(path_start);

	return result;
}

/* ================================================================
 * SOCKET HELPERS
 * ================================================================ */

/*
 * http_set_nonblocking
 *
 * Set a socket to non-blocking mode.
 */
static void
http_set_nonblocking(int sockfd)
{
	int			flags;

	flags = fcntl(sockfd, F_GETFL, 0);
	if (flags < 0)
		ereport(ERROR,
				(errcode(ERRCODE_SYSTEM_ERROR),
				 errmsg("fcntl(F_GETFL) failed: %m")));

	if (fcntl(sockfd, F_SETFL, flags | O_NONBLOCK) < 0)
		ereport(ERROR,
				(errcode(ERRCODE_SYSTEM_ERROR),
				 errmsg("fcntl(F_SETFL) failed: %m")));
}

/*
 * http_connect_with_timeout
 *
 * Resolve the hostname, create a TCP socket, and connect with the
 * given timeout.  Returns the connected socket fd, or raises ERROR.
 */
static int
http_connect_with_timeout(const char *host, int port, int timeout_ms)
{
	struct addrinfo hints;
	struct addrinfo *result_list;
	struct addrinfo *rp;
	char		port_str[8];
	int			sockfd = -1;
	int			rc;

	snprintf(port_str, sizeof(port_str), "%d", port);

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_protocol = IPPROTO_TCP;

	rc = getaddrinfo(host, port_str, &hints, &result_list);
	if (rc != 0)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not resolve hostname \"%s\": %s",
						host, gai_strerror(rc))));

	/* Try each address until one connects */
	for (rp = result_list; rp != NULL; rp = rp->ai_next)
	{
		struct pollfd pfd;

		sockfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
		if (sockfd < 0)
			continue;

		/* Set non-blocking for connect with timeout */
		http_set_nonblocking(sockfd);

		rc = connect(sockfd, rp->ai_addr, rp->ai_addrlen);
		if (rc == 0)
		{
			/* Connected immediately */
			break;
		}

		if (errno != EINPROGRESS)
		{
			close(sockfd);
			sockfd = -1;
			continue;
		}

		/* Wait for connect to complete */
		pfd.fd = sockfd;
		pfd.events = POLLOUT;
		pfd.revents = 0;

		rc = poll(&pfd, 1, timeout_ms);
		if (rc <= 0)
		{
			close(sockfd);
			sockfd = -1;
			if (rc == 0)
			{
				freeaddrinfo(result_list);
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg("connection to \"%s:%d\" timed out after %d ms",
								host, port, timeout_ms)));
			}
			continue;
		}

		/* Check for connect error via SO_ERROR */
		{
			int			so_error;
			socklen_t	so_len = sizeof(so_error);

			if (getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &so_error, &so_len) < 0
				|| so_error != 0)
			{
				close(sockfd);
				sockfd = -1;
				continue;
			}
		}

		/* Connected successfully */
		break;
	}

	freeaddrinfo(result_list);

	if (sockfd < 0)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not connect to \"%s:%d\"", host, port)));

	return sockfd;
}

/* ================================================================
 * REQUEST/RESPONSE HANDLING
 * ================================================================ */

/*
 * http_read_response
 *
 * Read the full HTTP response from the socket, using poll() for
 * timeout enforcement.  Returns a palloc'd buffer containing the
 * raw response bytes.  Sets *response_len to the number of bytes read.
 */
static char *
http_read_response(int sockfd, int timeout_ms, int *response_len)
{
	StringInfoData buf;
	struct pollfd pfd;
	int			rc;
	char		readbuf[HTTP_READ_BUFFER_SIZE];

	initStringInfo(&buf);

	pfd.fd = sockfd;
	pfd.events = POLLIN;

	for (;;)
	{
		pfd.revents = 0;
		rc = poll(&pfd, 1, timeout_ms);

		if (rc < 0)
		{
			if (errno == EINTR)
				continue;
			pfree(buf.data);
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("poll() failed while reading HTTP response: %m")));
		}

		if (rc == 0)
		{
			pfree(buf.data);
			ereport(ERROR,
					(errcode(ERRCODE_QUERY_CANCELED),
					 errmsg("HTTP request timed out after %d ms", timeout_ms)));
		}

		if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL))
		{
			/*
			 * POLLHUP: peer closed connection -- read remaining data then
			 * break.  POLLERR/POLLNVAL: socket error.
			 */
			if (pfd.revents & (POLLERR | POLLNVAL))
			{
				if (buf.len == 0)
				{
					pfree(buf.data);
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_FAILURE),
							 errmsg("socket error while reading HTTP response")));
				}
				break;
			}
		}

		rc = recv(sockfd, readbuf, sizeof(readbuf), 0);
		if (rc < 0)
		{
			if (errno == EINTR || errno == EAGAIN)
				continue;
			pfree(buf.data);
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("recv() failed: %m")));
		}

		if (rc == 0)
		{
			/* EOF -- connection closed by peer */
			break;
		}

		appendBinaryStringInfo(&buf, readbuf, rc);

		/* Safety limit to prevent unbounded memory growth */
		if (buf.len > HTTP_MAX_RESPONSE_SIZE)
		{
			pfree(buf.data);
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("HTTP response exceeds maximum size of %d bytes",
							HTTP_MAX_RESPONSE_SIZE)));
		}

		/*
		 * Check if we have received the complete response.  We look for the
		 * end of headers (\r\n\r\n), then check Content-Length or chunked
		 * encoding to determine if the body is complete.
		 */
		{
			char	   *hdr_end;

			hdr_end = strstr(buf.data, "\r\n\r\n");
			if (hdr_end != NULL)
			{
				int			hdr_len = (hdr_end - buf.data) + 4;
				int			body_received = buf.len - hdr_len;
				char	   *cl_hdr;

				/*
				 * Look for Content-Length header (case-insensitive search
				 * within the header region).
				 */
				cl_hdr = NULL;
				{
					char	   *search = buf.data;
					char	   *search_end = hdr_end;

					while (search < search_end)
					{
						if ((*search == 'C' || *search == 'c') &&
							pg_strncasecmp(search, "Content-Length:", 15) == 0)
						{
							cl_hdr = search + 15;
							break;
						}
						/* Advance to next line */
						while (search < search_end && *search != '\n')
							search++;
						if (search < search_end)
							search++;
					}
				}

				if (cl_hdr != NULL)
				{
					/* Skip whitespace after header name */
					while (*cl_hdr == ' ' || *cl_hdr == '\t')
						cl_hdr++;

					{
						long		content_length = strtol(cl_hdr, NULL, 10);

						if (content_length >= 0 && body_received >= content_length)
							break;		/* Complete response received */
					}
				}
				else
				{
					/*
					 * Check for chunked transfer encoding.  If chunked, we
					 * look for the terminal "0\r\n\r\n" chunk.
					 */
					char	   *te_hdr = NULL;
					char	   *search = buf.data;
					char	   *search_end = hdr_end;

					while (search < search_end)
					{
						if ((*search == 'T' || *search == 't') &&
							pg_strncasecmp(search, "Transfer-Encoding:", 18) == 0)
						{
							te_hdr = search + 18;
							break;
						}
						while (search < search_end && *search != '\n')
							search++;
						if (search < search_end)
							search++;
					}

					if (te_hdr != NULL)
					{
						/* Skip whitespace */
						while (*te_hdr == ' ' || *te_hdr == '\t')
							te_hdr++;

						if (pg_strncasecmp(te_hdr, "chunked", 7) == 0)
						{
							/*
							 * For chunked encoding, the response ends with
							 * "0\r\n\r\n" (possibly with trailers).
							 */
							if (buf.len >= 5 &&
								strstr(buf.data + hdr_len, "\r\n0\r\n") != NULL)
								break;
							if (body_received >= 5 &&
								memcmp(buf.data + buf.len - 5, "0\r\n\r\n", 5) == 0)
								break;
						}
						else
						{
							/*
							 * Unknown transfer encoding without
							 * Content-Length; read until connection close.
							 */
						}
					}
					else
					{
						/*
						 * No Content-Length and no Transfer-Encoding.
						 * For responses that allow a body (not HEAD,
						 * not 204/304), we read until connection close.
						 * Continue reading.
						 */
					}
				}
			}
		}
	}

	*response_len = buf.len;
	return buf.data;
}

/*
 * http_parse_chunked_body
 *
 * Decode a chunked transfer-encoded body.  Returns a palloc'd buffer
 * with the decoded content.  Sets *out_len to the decoded length.
 */
static char *
http_parse_chunked_body(const char *raw, int raw_len, int *out_len)
{
	StringInfoData decoded;
	const char *p = raw;
	const char *end = raw + raw_len;

	initStringInfo(&decoded);

	while (p < end)
	{
		long		chunk_size;
		char	   *size_end;

		/* Skip any leading \r\n from previous chunk */
		if (p + 1 < end && p[0] == '\r' && p[1] == '\n')
			p += 2;

		/* Parse hex chunk size */
		chunk_size = strtol(p, &size_end, 16);
		if (size_end == p)
			break;				/* Malformed: no hex digits */

		/* Skip to the \r\n after the chunk size line */
		p = size_end;
		while (p < end && *p != '\n')
			p++;
		if (p < end)
			p++;				/* skip \n */

		if (chunk_size == 0)
			break;				/* Terminal chunk */

		/* Copy chunk data */
		if (p + chunk_size <= end)
			appendBinaryStringInfo(&decoded, p, (int) chunk_size);
		else
			appendBinaryStringInfo(&decoded, p, (int) (end - p));

		p += chunk_size;
	}

	*out_len = decoded.len;
	return decoded.data;
}

/*
 * http_parse_response
 *
 * Parse a raw HTTP response into status code, headers (as JSON), and body.
 */
static void
http_parse_response(const char *raw, int raw_len,
					int *status_code,
					StringInfo headers_json,
					char **body, int *body_len)
{
	const char *p;
	const char *hdr_end;
	const char *line_start;
	int			hdr_section_len;
	bool		is_chunked = false;
	int			content_length = -1;
	bool		first_header = true;

	/* Parse status line: HTTP/1.x STATUS REASON\r\n */
	p = raw;
	if (raw_len < 12 || strncmp(p, "HTTP/", 5) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("invalid HTTP response: missing status line")));

	/* Skip to status code */
	while (p < raw + raw_len && *p != ' ')
		p++;
	if (p < raw + raw_len)
		p++;					/* skip space */

	*status_code = atoi(p);
	if (*status_code < 100 || *status_code > 999)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("invalid HTTP status code in response")));

	/* Skip to end of status line */
	while (p < raw + raw_len && *p != '\n')
		p++;
	if (p < raw + raw_len)
		p++;					/* skip \n */

	/* Find end of headers */
	hdr_end = strstr(p, "\r\n\r\n");
	if (hdr_end == NULL)
	{
		/* Try bare \n\n as fallback */
		hdr_end = strstr(p, "\n\n");
		if (hdr_end == NULL)
		{
			/* No body delimiter found; treat rest as headers */
			hdr_end = raw + raw_len;
			hdr_section_len = hdr_end - p;
		}
		else
		{
			hdr_section_len = hdr_end - p;
			hdr_end += 2;		/* skip \n\n */
		}
	}
	else
	{
		hdr_section_len = hdr_end - p;
		hdr_end += 4;			/* skip \r\n\r\n */
	}

	/* Build JSON object from headers */
	appendStringInfoChar(headers_json, '{');

	line_start = p;
	while (line_start < p + hdr_section_len)
	{
		const char *colon;
		const char *line_end;
		const char *val_start;
		char	   *hdr_name;
		char	   *hdr_value;
		int			name_len;
		int			val_len;

		/* Find end of this header line */
		line_end = line_start;
		while (line_end < p + hdr_section_len && *line_end != '\r' && *line_end != '\n')
			line_end++;

		/* Skip empty lines */
		if (line_end == line_start)
		{
			line_start = line_end;
			while (line_start < p + hdr_section_len &&
				   (*line_start == '\r' || *line_start == '\n'))
				line_start++;
			continue;
		}

		/* Find the colon separator */
		colon = line_start;
		while (colon < line_end && *colon != ':')
			colon++;

		if (colon >= line_end)
		{
			/* No colon found; skip this line */
			line_start = line_end;
			while (line_start < p + hdr_section_len &&
				   (*line_start == '\r' || *line_start == '\n'))
				line_start++;
			continue;
		}

		name_len = colon - line_start;
		hdr_name = pnstrdup(line_start, name_len);

		/* Value starts after colon + optional whitespace */
		val_start = colon + 1;
		while (val_start < line_end && (*val_start == ' ' || *val_start == '\t'))
			val_start++;

		val_len = line_end - val_start;
		hdr_value = pnstrdup(val_start, val_len);

		/* Check for Transfer-Encoding: chunked */
		if (pg_strncasecmp(hdr_name, "Transfer-Encoding", 17) == 0 &&
			pg_strncasecmp(hdr_value, "chunked", 7) == 0)
			is_chunked = true;

		/* Check for Content-Length */
		if (pg_strncasecmp(hdr_name, "Content-Length", 14) == 0)
			content_length = atoi(hdr_value);

		/* Append to JSON: escape special characters */
		if (!first_header)
			appendStringInfoString(headers_json, ", ");
		first_header = false;

		appendStringInfoChar(headers_json, '"');

		/* Escape header name for JSON */
		{
			const char *s;

			for (s = hdr_name; *s; s++)
			{
				if (*s == '"' || *s == '\\')
					appendStringInfoChar(headers_json, '\\');
				appendStringInfoChar(headers_json, *s);
			}
		}

		appendStringInfoString(headers_json, "\": \"");

		/* Escape header value for JSON */
		{
			const char *s;

			for (s = hdr_value; *s; s++)
			{
				if (*s == '"' || *s == '\\')
					appendStringInfoChar(headers_json, '\\');
				else if (*s == '\n')
				{
					appendStringInfoString(headers_json, "\\n");
					continue;
				}
				else if (*s == '\r')
				{
					appendStringInfoString(headers_json, "\\r");
					continue;
				}
				else if (*s == '\t')
				{
					appendStringInfoString(headers_json, "\\t");
					continue;
				}
				appendStringInfoChar(headers_json, *s);
			}
		}

		appendStringInfoChar(headers_json, '"');

		pfree(hdr_name);
		pfree(hdr_value);

		/* Advance to next header line */
		line_start = line_end;
		while (line_start < p + hdr_section_len &&
			   (*line_start == '\r' || *line_start == '\n'))
			line_start++;
	}

	appendStringInfoChar(headers_json, '}');

	/* Extract body */
	if (hdr_end >= raw + raw_len)
	{
		/* No body */
		*body = pstrdup("");
		*body_len = 0;
	}
	else if (is_chunked)
	{
		int			chunked_raw_len = (raw + raw_len) - hdr_end;

		*body = http_parse_chunked_body(hdr_end, chunked_raw_len, body_len);
	}
	else
	{
		int			remaining = (raw + raw_len) - hdr_end;

		if (content_length >= 0 && content_length < remaining)
			remaining = content_length;

		*body = pnstrdup(hdr_end, remaining);
		*body_len = remaining;
	}
}

/* ================================================================
 * HTTP REQUEST EXECUTION
 * ================================================================ */

/*
 * http_execute_request
 *
 * Perform a synchronous HTTP request and return the parsed response.
 * This is the core function that all SQL wrappers delegate to.
 */
HttpResponse
http_execute_request(const char *method,
					 const char *url,
					 const char *req_body,
					 const char *headers_json,
					 int timeout_ms)
{
	ParsedURL	parsed;
	int			sockfd;
	StringInfoData request;
	HttpResponse resp;
	char	   *raw_response;
	int			raw_len;
	StringInfoData resp_headers;
	ssize_t		sent;
	ssize_t		total_sent;

	memset(&resp, 0, sizeof(HttpResponse));

	/* Parse the URL */
	parsed = http_parse_url(url);

	/* Reject HTTPS */
	if (parsed.is_https)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("HTTPS is not supported without libcurl"),
				 errhint("Use http:// URLs instead of https://")));

	/* Connect to the server */
	sockfd = http_connect_with_timeout(parsed.host, parsed.port, timeout_ms);

	/* Build the HTTP request */
	initStringInfo(&request);

	appendStringInfo(&request, "%s %s HTTP/1.1\r\n", method, parsed.path);
	appendStringInfo(&request, "Host: %s", parsed.host);
	if (parsed.port != 80)
		appendStringInfo(&request, ":%d", parsed.port);
	appendStringInfoString(&request, "\r\n");

	/* Connection: close so we know when the response ends */
	appendStringInfoString(&request, "Connection: close\r\n");
	appendStringInfoString(&request, "User-Agent: AlohaDB-HTTP/1.0\r\n");

	/* Add custom headers if provided */
	if (headers_json != NULL)
	{
		/*
		 * The headers parameter is a JSON object like:
		 *   {"Content-Type": "application/json", "X-Custom": "value"}
		 *
		 * We do a simple parse: split on commas between key-value pairs.
		 * This is a pragmatic approach that handles the common case.
		 * We skip the outer braces and parse "key": "value" pairs.
		 */
		const char *p = headers_json;
		const char *end = headers_json + strlen(headers_json);

		/* Skip leading whitespace and '{' */
		while (p < end && (*p == ' ' || *p == '\t' || *p == '{'))
			p++;

		while (p < end && *p != '}')
		{
			const char *key_start;
			const char *key_end;
			const char *val_start;
			const char *val_end;

			/* Skip whitespace and commas */
			while (p < end && (*p == ' ' || *p == '\t' || *p == ',' || *p == '\n' || *p == '\r'))
				p++;

			if (p >= end || *p == '}')
				break;

			/* Expect opening quote for key */
			if (*p != '"')
				break;
			p++;

			key_start = p;
			while (p < end && *p != '"')
			{
				if (*p == '\\' && p + 1 < end)
					p++;		/* skip escaped char */
				p++;
			}
			key_end = p;
			if (p < end)
				p++;			/* skip closing quote */

			/* Skip colon and whitespace */
			while (p < end && (*p == ' ' || *p == '\t' || *p == ':'))
				p++;

			/* Expect opening quote for value */
			if (p >= end || *p != '"')
				break;
			p++;

			val_start = p;
			while (p < end && *p != '"')
			{
				if (*p == '\\' && p + 1 < end)
					p++;		/* skip escaped char */
				p++;
			}
			val_end = p;
			if (p < end)
				p++;			/* skip closing quote */

			/* Emit the header line */
			if (key_end > key_start)
			{
				appendBinaryStringInfo(&request, key_start,
									   (int) (key_end - key_start));
				appendStringInfoString(&request, ": ");
				appendBinaryStringInfo(&request, val_start,
									   (int) (val_end - val_start));
				appendStringInfoString(&request, "\r\n");
			}
		}
	}

	/* Content-Length for the body */
	if (req_body != NULL && strlen(req_body) > 0)
	{
		appendStringInfo(&request, "Content-Length: %d\r\n",
						 (int) strlen(req_body));
	}

	/* End of headers */
	appendStringInfoString(&request, "\r\n");

	/* Append body if present */
	if (req_body != NULL && strlen(req_body) > 0)
		appendStringInfoString(&request, req_body);

	/* Send the request */
	total_sent = 0;
	while (total_sent < request.len)
	{
		struct pollfd pfd;

		pfd.fd = sockfd;
		pfd.events = POLLOUT;
		pfd.revents = 0;

		if (poll(&pfd, 1, timeout_ms) <= 0)
		{
			close(sockfd);
			pfree(request.data);
			pfree(parsed.host);
			pfree(parsed.path);
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("timeout while sending HTTP request")));
		}

		sent = send(sockfd, request.data + total_sent,
					request.len - total_sent, 0);
		if (sent < 0)
		{
			if (errno == EINTR || errno == EAGAIN)
				continue;
			close(sockfd);
			pfree(request.data);
			pfree(parsed.host);
			pfree(parsed.path);
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("send() failed: %m")));
		}

		total_sent += sent;
	}

	pfree(request.data);

	/* Read the response */
	raw_response = http_read_response(sockfd, timeout_ms, &raw_len);

	close(sockfd);

	/* Parse the response */
	initStringInfo(&resp_headers);
	http_parse_response(raw_response, raw_len,
						&resp.status_code,
						&resp_headers,
						&resp.body,
						&resp.body_len);

	resp.headers_json = resp_headers.data;

	pfree(raw_response);
	pfree(parsed.host);
	pfree(parsed.path);

	return resp;
}

/* ================================================================
 * SHARED HELPERS FOR SQL FUNCTIONS
 * ================================================================ */

/*
 * http_check_enabled
 *
 * Raise an error if the HTTP functionality is disabled via GUC.
 */
static void
http_check_enabled(void)
{
	if (!http_enabled)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("HTTP functions are disabled"),
				 errhint("Set alohadb.http_enabled = true to enable "
						 "outbound HTTP requests.")));
}

/*
 * http_effective_timeout
 *
 * Return the effective timeout, clamped to the GUC maximum.
 */
static int
http_effective_timeout(int requested_ms)
{
	if (requested_ms <= 0)
		return http_max_timeout_ms;

	if (requested_ms > http_max_timeout_ms)
		return http_max_timeout_ms;

	return requested_ms;
}

/*
 * http_build_result
 *
 * Build a materialized SRF result containing a single row with
 * (status int, response_headers jsonb, body text).
 *
 * Uses InitMaterializedSRF with MAT_SRF_USE_EXPECTED_DESC since
 * the SQL function uses RETURNS TABLE (composite type), which means
 * the expectedDesc is available and matches our output columns.
 */
static void
http_build_result(FunctionCallInfo fcinfo, HttpResponse *resp)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Datum		values[3];
	bool		nulls[3];

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	memset(nulls, false, sizeof(nulls));

	/* Column 1: status (int4) */
	values[0] = Int32GetDatum(resp->status_code);

	/* Column 2: response_headers (jsonb) */
	if (resp->headers_json != NULL && strlen(resp->headers_json) > 0)
	{
		values[1] = DirectFunctionCall1(jsonb_in,
										CStringGetDatum(resp->headers_json));
	}
	else
	{
		values[1] = DirectFunctionCall1(jsonb_in,
										CStringGetDatum("{}"));
	}

	/* Column 3: body (text) */
	if (resp->body != NULL)
		values[2] = CStringGetTextDatum(resp->body);
	else
		values[2] = CStringGetTextDatum("");

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
}

/* ================================================================
 * SQL-CALLABLE FUNCTIONS
 * ================================================================ */

/*
 * http_get(url text, headers jsonb DEFAULT NULL, timeout_ms int DEFAULT 30000)
 * RETURNS TABLE(status int, response_headers jsonb, body text)
 */
Datum
http_get(PG_FUNCTION_ARGS)
{
	char	   *url;
	char	   *headers_json = NULL;
	int			timeout_ms;
	HttpResponse resp;

	http_check_enabled();

	/* Arg 0: url (text, required) */
	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("url must not be NULL")));
	url = text_to_cstring(PG_GETARG_TEXT_PP(0));

	/* Arg 1: headers (jsonb, optional) */
	if (!PG_ARGISNULL(1))
		headers_json = DatumGetCString(DirectFunctionCall1(jsonb_out,
														   PG_GETARG_DATUM(1)));

	/* Arg 2: timeout_ms (int, optional) */
	if (!PG_ARGISNULL(2))
		timeout_ms = http_effective_timeout(PG_GETARG_INT32(2));
	else
		timeout_ms = http_effective_timeout(HTTP_DEFAULT_TIMEOUT_MS);

	resp = http_execute_request("GET", url, NULL, headers_json, timeout_ms);

	http_build_result(fcinfo, &resp);

	PG_RETURN_NULL();
}

/*
 * http_post(url text, body text DEFAULT NULL, headers jsonb DEFAULT NULL,
 *           timeout_ms int DEFAULT 30000)
 * RETURNS TABLE(status int, response_headers jsonb, body text)
 */
Datum
http_post(PG_FUNCTION_ARGS)
{
	char	   *url;
	char	   *req_body = NULL;
	char	   *headers_json = NULL;
	int			timeout_ms;
	HttpResponse resp;

	http_check_enabled();

	/* Arg 0: url (text, required) */
	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("url must not be NULL")));
	url = text_to_cstring(PG_GETARG_TEXT_PP(0));

	/* Arg 1: body (text, optional) */
	if (!PG_ARGISNULL(1))
		req_body = text_to_cstring(PG_GETARG_TEXT_PP(1));

	/* Arg 2: headers (jsonb, optional) */
	if (!PG_ARGISNULL(2))
		headers_json = DatumGetCString(DirectFunctionCall1(jsonb_out,
														   PG_GETARG_DATUM(2)));

	/* Arg 3: timeout_ms (int, optional) */
	if (!PG_ARGISNULL(3))
		timeout_ms = http_effective_timeout(PG_GETARG_INT32(3));
	else
		timeout_ms = http_effective_timeout(HTTP_DEFAULT_TIMEOUT_MS);

	resp = http_execute_request("POST", url, req_body, headers_json, timeout_ms);

	http_build_result(fcinfo, &resp);

	PG_RETURN_NULL();
}

/*
 * http_put(url text, body text DEFAULT NULL, headers jsonb DEFAULT NULL,
 *          timeout_ms int DEFAULT 30000)
 * RETURNS TABLE(status int, response_headers jsonb, body text)
 */
Datum
http_put(PG_FUNCTION_ARGS)
{
	char	   *url;
	char	   *req_body = NULL;
	char	   *headers_json = NULL;
	int			timeout_ms;
	HttpResponse resp;

	http_check_enabled();

	/* Arg 0: url (text, required) */
	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("url must not be NULL")));
	url = text_to_cstring(PG_GETARG_TEXT_PP(0));

	/* Arg 1: body (text, optional) */
	if (!PG_ARGISNULL(1))
		req_body = text_to_cstring(PG_GETARG_TEXT_PP(1));

	/* Arg 2: headers (jsonb, optional) */
	if (!PG_ARGISNULL(2))
		headers_json = DatumGetCString(DirectFunctionCall1(jsonb_out,
														   PG_GETARG_DATUM(2)));

	/* Arg 3: timeout_ms (int, optional) */
	if (!PG_ARGISNULL(3))
		timeout_ms = http_effective_timeout(PG_GETARG_INT32(3));
	else
		timeout_ms = http_effective_timeout(HTTP_DEFAULT_TIMEOUT_MS);

	resp = http_execute_request("PUT", url, req_body, headers_json, timeout_ms);

	http_build_result(fcinfo, &resp);

	PG_RETURN_NULL();
}

/*
 * http_delete(url text, headers jsonb DEFAULT NULL, timeout_ms int DEFAULT 30000)
 * RETURNS TABLE(status int, response_headers jsonb, body text)
 */
Datum
http_delete(PG_FUNCTION_ARGS)
{
	char	   *url;
	char	   *headers_json = NULL;
	int			timeout_ms;
	HttpResponse resp;

	http_check_enabled();

	/* Arg 0: url (text, required) */
	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("url must not be NULL")));
	url = text_to_cstring(PG_GETARG_TEXT_PP(0));

	/* Arg 1: headers (jsonb, optional) */
	if (!PG_ARGISNULL(1))
		headers_json = DatumGetCString(DirectFunctionCall1(jsonb_out,
														   PG_GETARG_DATUM(1)));

	/* Arg 2: timeout_ms (int, optional) */
	if (!PG_ARGISNULL(2))
		timeout_ms = http_effective_timeout(PG_GETARG_INT32(2));
	else
		timeout_ms = http_effective_timeout(HTTP_DEFAULT_TIMEOUT_MS);

	resp = http_execute_request("DELETE", url, NULL, headers_json, timeout_ms);

	http_build_result(fcinfo, &resp);

	PG_RETURN_NULL();
}

/*
 * http_request(method text, url text, body text DEFAULT NULL,
 *              headers jsonb DEFAULT NULL, timeout_ms int DEFAULT 30000)
 * RETURNS TABLE(status int, response_headers jsonb, body text)
 */
Datum
http_request(PG_FUNCTION_ARGS)
{
	char	   *method;
	char	   *url;
	char	   *req_body = NULL;
	char	   *headers_json = NULL;
	int			timeout_ms;
	HttpResponse resp;

	http_check_enabled();

	/* Arg 0: method (text, required) */
	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("method must not be NULL")));
	method = text_to_cstring(PG_GETARG_TEXT_PP(0));

	/* Arg 1: url (text, required) */
	if (PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("url must not be NULL")));
	url = text_to_cstring(PG_GETARG_TEXT_PP(1));

	/* Arg 2: body (text, optional) */
	if (!PG_ARGISNULL(2))
		req_body = text_to_cstring(PG_GETARG_TEXT_PP(2));

	/* Arg 3: headers (jsonb, optional) */
	if (!PG_ARGISNULL(3))
		headers_json = DatumGetCString(DirectFunctionCall1(jsonb_out,
														   PG_GETARG_DATUM(3)));

	/* Arg 4: timeout_ms (int, optional) */
	if (!PG_ARGISNULL(4))
		timeout_ms = http_effective_timeout(PG_GETARG_INT32(4));
	else
		timeout_ms = http_effective_timeout(HTTP_DEFAULT_TIMEOUT_MS);

	resp = http_execute_request(method, url, req_body, headers_json, timeout_ms);

	http_build_result(fcinfo, &resp);

	PG_RETURN_NULL();
}
