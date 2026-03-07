/*-------------------------------------------------------------------------
 *
 * alohadb_http.c
 *	  Main entry point for the alohadb_http extension.
 *
 *	  Provides HTTP client capabilities from within PostgreSQL.
 *	  This file contains the module magic, _PG_init for GUC
 *	  registration, and shared state.  The actual HTTP functions
 *	  are implemented in http_client.c.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_http/alohadb_http.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "utils/guc.h"

#include "alohadb_http.h"

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_http",
					.version = "1.0"
);

/* ----------------------------------------------------------------
 * GUC variables
 * ---------------------------------------------------------------- */

/* Master switch: HTTP functions are disabled unless this is true */
bool		http_enabled = HTTP_DEFAULT_ENABLED;

/* Maximum allowed timeout in milliseconds for any HTTP request */
int			http_max_timeout_ms = HTTP_DEFAULT_TIMEOUT_MS;

/* ----------------------------------------------------------------
 * _PG_init
 *
 * Module load callback.  Registers GUC variables that control
 * HTTP client behavior.
 * ---------------------------------------------------------------- */
void
_PG_init(void)
{
	DefineCustomBoolVariable("alohadb.http_enabled",
							 "Enable or disable HTTP client functions.",
							 "When false (the default), all HTTP functions "
							 "will raise an error.  Set to true to allow "
							 "outbound HTTP requests.",
							 &http_enabled,
							 HTTP_DEFAULT_ENABLED,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("alohadb.http_max_timeout_ms",
							"Maximum timeout for HTTP requests in milliseconds.",
							"Individual requests may specify a lower timeout, "
							"but never higher than this value.",
							&http_max_timeout_ms,
							HTTP_DEFAULT_TIMEOUT_MS,
							1000,		/* min: 1 second */
							300000,		/* max: 5 minutes */
							PGC_SUSET,
							GUC_UNIT_MS,
							NULL,
							NULL,
							NULL);

	MarkGUCPrefixReserved("alohadb.http");
}
