/*-------------------------------------------------------------------------
 *
 * alohadb_restapi.h
 *	  Shared declarations for the alohadb_restapi extension.
 *
 *	  Provides an auto-generated REST API for PostgreSQL tables,
 *	  served by a background worker running a TCP HTTP server.
 *	  Supports GET/POST/PUT/DELETE mapped to SELECT/INSERT/UPDATE/DELETE.
 *
 *	  Patent note: PostgREST (MIT, 2014) is extensive prior art.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * contrib/alohadb_restapi/alohadb_restapi.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef ALOHADB_RESTAPI_H
#define ALOHADB_RESTAPI_H

#include "postgres.h"
#include "fmgr.h"

/* ----------------------------------------------------------------
 * Constants
 * ---------------------------------------------------------------- */

#define RESTAPI_DEFAULT_PORT		8432
#define RESTAPI_MAX_REQUEST_SIZE	(1024 * 1024)	/* 1 MB */
#define RESTAPI_MAX_CONNECTIONS		64
#define RESTAPI_PATH_PREFIX			"/api/"
#define RESTAPI_PATH_PREFIX_LEN		5
#define RESTAPI_MAX_BODY_SIZE		RESTAPI_MAX_REQUEST_SIZE
#define RESTAPI_READ_BUF_SIZE		8192
#define RESTAPI_WRITE_BUF_SIZE		(1024 * 1024)
#define RESTAPI_LISTEN_BACKLOG		16
#define RESTAPI_POLL_TIMEOUT_MS		1000
#define RESTAPI_DEFAULT_LIMIT		100

/* ----------------------------------------------------------------
 * Connection state machine
 * ---------------------------------------------------------------- */

typedef enum RestApiConnState
{
	RESTAPI_CONN_READING,		/* accumulating request data */
	RESTAPI_CONN_PROCESSING,	/* request complete, generating response */
	RESTAPI_CONN_WRITING,		/* sending response data */
	RESTAPI_CONN_DONE			/* finished, ready to close */
} RestApiConnState;

/* ----------------------------------------------------------------
 * RestApiRequest - parsed HTTP request
 * ---------------------------------------------------------------- */

typedef struct RestApiRequest
{
	char		method[8];			/* GET, POST, PUT, DELETE */
	char		path[256];			/* e.g., /api/users/42 */
	char	   *body;				/* request body (JSON), palloc'd */
	int			body_len;			/* length of body */
	char		role[NAMEDATALEN];	/* from X-PG-Role header */
	int			content_length;		/* from Content-Length header */
} RestApiRequest;

/* ----------------------------------------------------------------
 * RestApiResponse - HTTP response to send back
 * ---------------------------------------------------------------- */

typedef struct RestApiResponse
{
	int			status_code;		/* HTTP status: 200, 201, 404, etc. */
	char	   *body;				/* JSON response body, palloc'd */
	int			body_len;			/* length of body */
} RestApiResponse;

/* ----------------------------------------------------------------
 * RestApiConn - per-connection state
 * ---------------------------------------------------------------- */

typedef struct RestApiConn
{
	int				fd;				/* socket file descriptor */
	RestApiConnState state;			/* current connection state */
	char		   *read_buf;		/* incoming data buffer */
	int				read_len;		/* bytes in read_buf */
	int				read_cap;		/* allocated size of read_buf */
	char		   *write_buf;		/* outgoing data buffer */
	int				write_len;		/* bytes in write_buf */
	int				write_pos;		/* bytes already sent */
} RestApiConn;

/* ----------------------------------------------------------------
 * GUC variables (defined in alohadb_restapi.c)
 * ---------------------------------------------------------------- */

extern int	restapi_port;
extern char *restapi_database;
extern char *restapi_default_role;
extern char *restapi_schema;

/* ----------------------------------------------------------------
 * Background worker entry point (exported for postmaster)
 * ---------------------------------------------------------------- */

extern PGDLLEXPORT void alohadb_restapi_worker_main(Datum main_arg);

/* ----------------------------------------------------------------
 * SQL-callable functions
 * ---------------------------------------------------------------- */

extern Datum alohadb_restapi_handle(PG_FUNCTION_ARGS);
extern Datum alohadb_restapi_status(PG_FUNCTION_ARGS);
extern Datum alohadb_restapi_endpoints(PG_FUNCTION_ARGS);

/* ----------------------------------------------------------------
 * HTTP layer (restapi_http.c)
 * ---------------------------------------------------------------- */

extern bool http_parse_request(const char *raw, int raw_len,
							   RestApiRequest *req);
extern char *http_format_response(RestApiResponse *resp, int *out_len);

/* ----------------------------------------------------------------
 * Handler layer (restapi_handler.c)
 * ---------------------------------------------------------------- */

extern RestApiResponse *restapi_handle_request(RestApiRequest *req);

#endif							/* ALOHADB_RESTAPI_H */
