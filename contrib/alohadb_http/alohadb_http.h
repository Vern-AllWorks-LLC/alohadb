/*-------------------------------------------------------------------------
 *
 * alohadb_http.h
 *	  Shared declarations for the alohadb_http extension.
 *
 *	  Provides HTTP client capabilities from within PostgreSQL using
 *	  POSIX sockets for basic HTTP/1.1 support.  HTTPS is not supported
 *	  without an external TLS library; attempts to use https:// URLs
 *	  will raise an ERROR.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * contrib/alohadb_http/alohadb_http.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef ALOHADB_HTTP_H
#define ALOHADB_HTTP_H

#include "postgres.h"
#include "fmgr.h"

/* ----------------------------------------------------------------
 * Default GUC values
 * ---------------------------------------------------------------- */

/* HTTP functionality is disabled by default for safety */
#define HTTP_DEFAULT_ENABLED		false

/* Default timeout for HTTP requests in milliseconds */
#define HTTP_DEFAULT_TIMEOUT_MS		30000

/* Maximum size of an HTTP response we will buffer (16 MB) */
#define HTTP_MAX_RESPONSE_SIZE		(16 * 1024 * 1024)

/* Size of the read buffer for socket I/O */
#define HTTP_READ_BUFFER_SIZE		8192

/* Maximum number of response headers we track */
#define HTTP_MAX_HEADERS			128

/* ----------------------------------------------------------------
 * Parsed URL structure
 * ---------------------------------------------------------------- */

typedef struct ParsedURL
{
	char	   *host;			/* hostname (palloc'd) */
	int			port;			/* port number (default 80) */
	char	   *path;			/* path including query string (palloc'd) */
	bool		is_https;		/* true if scheme is https */
} ParsedURL;

/* ----------------------------------------------------------------
 * HTTP response structure
 * ---------------------------------------------------------------- */

typedef struct HttpResponse
{
	int			status_code;	/* HTTP status code (e.g. 200) */
	char	   *headers_json;	/* response headers as JSON string */
	char	   *body;			/* response body (palloc'd) */
	int			body_len;		/* length of body */
} HttpResponse;

/* ----------------------------------------------------------------
 * GUC variables (defined in alohadb_http.c)
 * ---------------------------------------------------------------- */

extern bool http_enabled;
extern int	http_max_timeout_ms;

/* ----------------------------------------------------------------
 * Utility functions (defined in http_client.c)
 * ---------------------------------------------------------------- */

extern ParsedURL http_parse_url(const char *url);
extern HttpResponse http_execute_request(const char *method,
										 const char *url,
										 const char *body,
										 const char *headers_json,
										 int timeout_ms);

/* ----------------------------------------------------------------
 * SQL-callable functions (defined in http_client.c)
 * ---------------------------------------------------------------- */

extern Datum http_get(PG_FUNCTION_ARGS);
extern Datum http_post(PG_FUNCTION_ARGS);
extern Datum http_put(PG_FUNCTION_ARGS);
extern Datum http_delete(PG_FUNCTION_ARGS);
extern Datum http_request(PG_FUNCTION_ARGS);

#endif							/* ALOHADB_HTTP_H */
