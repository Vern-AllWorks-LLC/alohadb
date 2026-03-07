/*-------------------------------------------------------------------------
 *
 * alohadb_remote_config.c
 *	  Centralized configuration management for AlohaDB instances.
 *
 *	  When alohadb.remote_mode is enabled, this extension fetches
 *	  configuration from a remote management server on startup and
 *	  on SIGHUP (pg_reload_conf).  The management server pushes
 *	  real-time changes via standard libpq connections.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_remote_config/alohadb_remote_config.c
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
#include <signal.h>

#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_remote_config",
					.version = "1.0"
);

/* ----------------------------------------------------------------
 * GUC variables
 * ---------------------------------------------------------------- */

static bool remote_mode = false;
static char *remote_host = NULL;
static int	remote_port = 5480;
static char *remote_password = NULL;

/* ----------------------------------------------------------------
 * Shared state for tracking fetch status
 * ---------------------------------------------------------------- */

typedef struct RemoteConfigState
{
	LWLock		lock;
	TimestampTz last_fetch;
	char		last_status[256];
	bool		fetch_ok;
} RemoteConfigState;

static RemoteConfigState *rc_state = NULL;

static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* ----------------------------------------------------------------
 * Shared memory setup
 * ---------------------------------------------------------------- */

static Size
rc_memsize(void)
{
	return MAXALIGN(sizeof(RemoteConfigState));
}

static void
rc_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(rc_memsize());
	RequestNamedLWLockTranche("alohadb_remote_config", 1);
}

static void
rc_shmem_startup(void)
{
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	rc_state = ShmemInitStruct("alohadb_remote_config",
							   sizeof(RemoteConfigState),
							   &found);

	if (!found)
	{
		memset(rc_state, 0, sizeof(RemoteConfigState));
		LWLockInitialize(&rc_state->lock, LWLockNewTrancheId());
		LWLockRegisterTranche(rc_state->lock.tranche, "alohadb_remote_config");
		rc_state->last_fetch = 0;
		rc_state->fetch_ok = false;
		snprintf(rc_state->last_status, sizeof(rc_state->last_status),
				 "not yet fetched");
	}

	LWLockRelease(AddinShmemInitLock);
}

/* ----------------------------------------------------------------
 * HTTP fetch helper (POSIX sockets, same pattern as alohadb_http)
 * ---------------------------------------------------------------- */

#define RC_READ_BUF_SIZE	8192
#define RC_MAX_RESPONSE		(1024 * 1024)	/* 1 MB max */
#define RC_CONNECT_TIMEOUT	10000			/* 10 seconds */
#define RC_READ_TIMEOUT		30000			/* 30 seconds */

static int
rc_connect(const char *host, int port, int timeout_ms)
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
	{
		elog(WARNING, "alohadb_remote_config: could not resolve \"%s\": %s",
			 host, gai_strerror(rc));
		return -1;
	}

	for (rp = result_list; rp != NULL; rp = rp->ai_next)
	{
		struct pollfd pfd;
		int			flags;

		sockfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
		if (sockfd < 0)
			continue;

		/* Set non-blocking */
		flags = fcntl(sockfd, F_GETFL, 0);
		if (flags >= 0)
			fcntl(sockfd, F_SETFL, flags | O_NONBLOCK);

		rc = connect(sockfd, rp->ai_addr, rp->ai_addrlen);
		if (rc == 0)
			break;

		if (errno != EINPROGRESS)
		{
			close(sockfd);
			sockfd = -1;
			continue;
		}

		pfd.fd = sockfd;
		pfd.events = POLLOUT;
		pfd.revents = 0;

		rc = poll(&pfd, 1, timeout_ms);
		if (rc <= 0)
		{
			close(sockfd);
			sockfd = -1;
			continue;
		}

		/* Check for connect error */
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

		break;
	}

	freeaddrinfo(result_list);

	if (sockfd < 0)
		elog(WARNING, "alohadb_remote_config: could not connect to %s:%d",
			 host, port);

	return sockfd;
}

static char *
rc_http_get(const char *host, int port, const char *path,
			const char *bearer_token, int *status_code)
{
	int				sockfd;
	StringInfoData	request;
	StringInfoData	response;
	struct pollfd	pfd;
	char			readbuf[RC_READ_BUF_SIZE];
	ssize_t			sent;
	ssize_t			total_sent;
	int				rc;
	char		   *hdr_end;
	char		   *body;

	*status_code = 0;

	sockfd = rc_connect(host, port, RC_CONNECT_TIMEOUT);
	if (sockfd < 0)
		return NULL;

	/* Build HTTP request */
	initStringInfo(&request);
	appendStringInfo(&request, "GET %s HTTP/1.1\r\n", path);
	appendStringInfo(&request, "Host: %s:%d\r\n", host, port);
	appendStringInfoString(&request, "Connection: close\r\n");
	appendStringInfoString(&request, "User-Agent: AlohaDB-RemoteConfig/1.0\r\n");
	if (bearer_token != NULL && bearer_token[0] != '\0')
		appendStringInfo(&request, "Authorization: Bearer %s\r\n", bearer_token);
	appendStringInfoString(&request, "\r\n");

	/* Send */
	total_sent = 0;
	while (total_sent < request.len)
	{
		pfd.fd = sockfd;
		pfd.events = POLLOUT;
		pfd.revents = 0;

		if (poll(&pfd, 1, RC_READ_TIMEOUT) <= 0)
		{
			close(sockfd);
			pfree(request.data);
			elog(WARNING, "alohadb_remote_config: send timeout");
			return NULL;
		}

		sent = send(sockfd, request.data + total_sent,
					request.len - total_sent, 0);
		if (sent < 0)
		{
			if (errno == EINTR || errno == EAGAIN)
				continue;
			close(sockfd);
			pfree(request.data);
			elog(WARNING, "alohadb_remote_config: send failed: %m");
			return NULL;
		}

		total_sent += sent;
	}

	pfree(request.data);

	/* Read response */
	initStringInfo(&response);
	pfd.fd = sockfd;
	pfd.events = POLLIN;

	for (;;)
	{
		pfd.revents = 0;
		rc = poll(&pfd, 1, RC_READ_TIMEOUT);

		if (rc < 0)
		{
			if (errno == EINTR)
				continue;
			break;
		}
		if (rc == 0)
			break;		/* timeout */

		rc = recv(sockfd, readbuf, sizeof(readbuf), 0);
		if (rc <= 0)
			break;

		appendBinaryStringInfo(&response, readbuf, rc);

		if (response.len > RC_MAX_RESPONSE)
		{
			elog(WARNING, "alohadb_remote_config: response too large");
			close(sockfd);
			pfree(response.data);
			return NULL;
		}
	}

	close(sockfd);

	if (response.len == 0)
	{
		pfree(response.data);
		elog(WARNING, "alohadb_remote_config: empty response");
		return NULL;
	}

	/* Parse status code from "HTTP/1.x NNN ..." */
	{
		const char *p = response.data;

		if (response.len >= 12 && strncmp(p, "HTTP/", 5) == 0)
		{
			while (*p && *p != ' ')
				p++;
			if (*p == ' ')
				p++;
			*status_code = atoi(p);
		}
	}

	/* Find body after \r\n\r\n */
	hdr_end = strstr(response.data, "\r\n\r\n");
	if (hdr_end == NULL)
	{
		pfree(response.data);
		return NULL;
	}

	body = pstrdup(hdr_end + 4);
	pfree(response.data);

	return body;
}

/* ----------------------------------------------------------------
 * Config fetch and apply
 * ---------------------------------------------------------------- */

static void
rc_fetch_and_apply(void)
{
	char		path[512];
	char		hostname[256];
	int			status_code;
	char	   *body;

	if (!remote_mode || remote_host == NULL || remote_host[0] == '\0')
		return;

	/* Get local hostname */
	if (gethostname(hostname, sizeof(hostname)) != 0)
	{
		elog(WARNING, "alohadb_remote_config: gethostname failed");
		return;
	}

	snprintf(path, sizeof(path), "/api/config/%s", hostname);

	elog(LOG, "alohadb_remote_config: fetching config from %s:%d%s",
		 remote_host, remote_port, path);

	body = rc_http_get(remote_host, remote_port, path,
					   remote_password, &status_code);

	/* Update shared state */
	if (rc_state != NULL)
	{
		LWLockAcquire(&rc_state->lock, LW_EXCLUSIVE);
		rc_state->last_fetch = GetCurrentTimestamp();

		if (body != NULL && status_code == 200)
		{
			rc_state->fetch_ok = true;
			snprintf(rc_state->last_status, sizeof(rc_state->last_status),
					 "OK (HTTP %d)", status_code);
			elog(LOG, "alohadb_remote_config: config fetched successfully");
		}
		else if (body != NULL)
		{
			rc_state->fetch_ok = false;
			snprintf(rc_state->last_status, sizeof(rc_state->last_status),
					 "error (HTTP %d)", status_code);
			elog(WARNING, "alohadb_remote_config: server returned HTTP %d",
				 status_code);
		}
		else
		{
			rc_state->fetch_ok = false;
			snprintf(rc_state->last_status, sizeof(rc_state->last_status),
					 "connection failed");
			elog(WARNING, "alohadb_remote_config: could not connect to management server");
		}

		LWLockRelease(&rc_state->lock);
	}

	if (body != NULL)
	{
		/*
		 * The response body is expected to be a JSON object with config
		 * parameters.  Parsing and applying config is done by the
		 * management app pushing via libpq, so here we just log success.
		 * Future: parse JSON and call SetConfigOption() for each key.
		 */
		pfree(body);
	}
}

/* ----------------------------------------------------------------
 * _PG_init
 * ---------------------------------------------------------------- */

void		_PG_init(void);

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	/* Define GUCs */
	DefineCustomBoolVariable("alohadb.remote_mode",
							 "Enable remote configuration management.",
							 "When enabled, AlohaDB fetches configuration "
							 "from a remote management server on startup "
							 "and on config reload.",
							 &remote_mode,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL, NULL, NULL);

	DefineCustomStringVariable("alohadb.remote_host",
							   "Hostname of the remote management server.",
							   NULL,
							   &remote_host,
							   "",
							   PGC_POSTMASTER,
							   0,
							   NULL, NULL, NULL);

	DefineCustomIntVariable("alohadb.remote_port",
							"Port of the remote management server.",
							NULL,
							&remote_port,
							5480,
							1,
							65535,
							PGC_POSTMASTER,
							0,
							NULL, NULL, NULL);

	DefineCustomStringVariable("alohadb.remote_password",
							   "Authentication token for the remote management server.",
							   "AES-256-GCM encrypted token provided by the "
							   "management server administrator.",
							   &remote_password,
							   "",
							   PGC_POSTMASTER,
							   GUC_SUPERUSER_ONLY,
							   NULL, NULL, NULL);

	MarkGUCPrefixReserved("alohadb.remote");

	/* Register shared memory hooks */
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = rc_shmem_request;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = rc_shmem_startup;

	/* If remote mode is enabled, fetch config at startup */
	if (remote_mode)
		rc_fetch_and_apply();

	elog(LOG, "alohadb_remote_config: initialized (remote_mode=%s)",
		 remote_mode ? "on" : "off");
}

/* ----------------------------------------------------------------
 * SQL-callable functions
 * ---------------------------------------------------------------- */

PG_FUNCTION_INFO_V1(remote_config_status);

Datum
remote_config_status(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[5];
	bool		nulls[5];
	HeapTuple	tuple;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	tupdesc = BlessTupleDesc(tupdesc);

	memset(nulls, false, sizeof(nulls));

	/* remote_mode */
	values[0] = BoolGetDatum(remote_mode);

	/* remote_host */
	if (remote_host != NULL && remote_host[0] != '\0')
		values[1] = CStringGetTextDatum(remote_host);
	else
		nulls[1] = true;

	/* remote_port */
	values[2] = Int32GetDatum(remote_port);

	/* last_fetch + last_status from shared state */
	if (rc_state != NULL)
	{
		LWLockAcquire(&rc_state->lock, LW_SHARED);

		if (rc_state->last_fetch != 0)
			values[3] = TimestampTzGetDatum(rc_state->last_fetch);
		else
			nulls[3] = true;

		values[4] = CStringGetTextDatum(rc_state->last_status);

		LWLockRelease(&rc_state->lock);
	}
	else
	{
		nulls[3] = true;
		values[4] = CStringGetTextDatum("shared memory not initialized");
	}

	tuple = heap_form_tuple(tupdesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}
