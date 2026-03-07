/*-------------------------------------------------------------------------
 *
 * alohadb_restapi.c
 *	  Main entry point for the alohadb_restapi extension.
 *
 *	  Implements an auto-generated REST API for PostgreSQL tables,
 *	  served by a background worker running a TCP HTTP server.
 *	  Maps HTTP methods to SQL operations:
 *	    GET    -> SELECT
 *	    POST   -> INSERT
 *	    PUT    -> UPDATE
 *	    DELETE -> DELETE
 *
 *	  Patent note: PostgREST (MIT, 2014) is extensive prior art.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_restapi/alohadb_restapi.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

/* Background worker essentials */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"

/* SPI and transaction management */
#include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"

#include "alohadb_restapi.h"

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_restapi",
					.version = "1.0"
);

/* SQL-callable function declarations */
PG_FUNCTION_INFO_V1(alohadb_restapi_handle);
PG_FUNCTION_INFO_V1(alohadb_restapi_status);
PG_FUNCTION_INFO_V1(alohadb_restapi_endpoints);

/* ----------------------------------------------------------------
 * GUC variables
 * ---------------------------------------------------------------- */

int			restapi_port = RESTAPI_DEFAULT_PORT;
char	   *restapi_database = NULL;
char	   *restapi_default_role = NULL;
char	   *restapi_schema = NULL;

/* ----------------------------------------------------------------
 * _PG_init
 *
 * Module load callback.  Registers GUC variables and, if loaded
 * via shared_preload_libraries, registers the background worker.
 * ---------------------------------------------------------------- */
void
_PG_init(void)
{
	BackgroundWorker worker;

	DefineCustomIntVariable("alohadb.restapi_port",
							"TCP port for the REST API HTTP server.",
							NULL,
							&restapi_port,
							RESTAPI_DEFAULT_PORT,
							1024,
							65535,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomStringVariable("alohadb.restapi_database",
							   "Database the REST API background worker connects to.",
							   NULL,
							   &restapi_database,
							   "postgres",
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable("alohadb.restapi_default_role",
							   "Default PostgreSQL role for unauthenticated REST API requests.",
							   NULL,
							   &restapi_default_role,
							   "",
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable("alohadb.restapi_schema",
							   "Schema to expose tables from via the REST API.",
							   NULL,
							   &restapi_schema,
							   "public",
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	if (!process_shared_preload_libraries_in_progress)
		return;

	MarkGUCPrefixReserved("alohadb.restapi");

	/* Register the background worker. */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
	snprintf(worker.bgw_library_name, MAXPGPATH, "alohadb_restapi");
	snprintf(worker.bgw_function_name, BGW_MAXLEN,
			 "alohadb_restapi_worker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "alohadb_restapi worker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "alohadb_restapi");
	worker.bgw_main_arg = (Datum) 0;
	worker.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&worker);
}

/* ----------------------------------------------------------------
 * alohadb_restapi_handle
 *
 * SQL-callable function to invoke the REST API handler directly.
 * Useful for testing and for calling the API without going through
 * the HTTP layer.
 *
 * alohadb_restapi_handle(method text, path text, body json DEFAULT NULL,
 *                        headers json DEFAULT NULL,
 *                        query_params json DEFAULT NULL) RETURNS json
 * ---------------------------------------------------------------- */
Datum
alohadb_restapi_handle(PG_FUNCTION_ARGS)
{
	text	   *method_text;
	text	   *path_text;
	RestApiRequest req;
	RestApiResponse *resp;
	StringInfoData result;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("method and path must not be NULL")));

	method_text = PG_GETARG_TEXT_PP(0);
	path_text = PG_GETARG_TEXT_PP(1);

	/* Build the request struct */
	memset(&req, 0, sizeof(req));
	strlcpy(req.method, text_to_cstring(method_text), sizeof(req.method));
	strlcpy(req.path, text_to_cstring(path_text), sizeof(req.path));

	/* Body (arg 2) */
	if (!PG_ARGISNULL(2))
	{
		text   *body_text = PG_GETARG_TEXT_PP(2);

		req.body = text_to_cstring(body_text);
		req.body_len = strlen(req.body);
	}

	/* Role from default_role GUC */
	if (restapi_default_role && restapi_default_role[0])
		strlcpy(req.role, restapi_default_role, NAMEDATALEN);

	/*
	 * The handler uses SPI internally.  Connect SPI, call the handler,
	 * then disconnect.  We are already inside a transaction since this
	 * is a SQL-callable function.
	 */
	{
		MemoryContext oldcxt;
		int			status_code;
		char	   *body_copy = NULL;
		int			body_len = 0;

		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());

		resp = restapi_handle_request(&req);

		/* Save response fields before leaving SPI context */
		status_code = resp->status_code;
		if (resp->body && resp->body_len > 0)
		{
			body_len = resp->body_len;
			/* Switch to caller's memory context to allocate the copy */
			oldcxt = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
			body_copy = palloc(body_len + 1);
			memcpy(body_copy, resp->body, body_len);
			body_copy[body_len] = '\0';
			MemoryContextSwitchTo(oldcxt);
		}

		PopActiveSnapshot();
		SPI_finish();

		/* Build result in caller's context */
		initStringInfo(&result);
		if (body_copy)
			appendBinaryStringInfo(&result, body_copy, body_len);
		else
			appendStringInfo(&result, "{\"status\": %d}", status_code);
	}

	PG_RETURN_TEXT_P(cstring_to_text(result.data));
}

/* ----------------------------------------------------------------
 * alohadb_restapi_status
 *
 * Returns current GUC configuration as (setting text, value text) rows.
 * ---------------------------------------------------------------- */
Datum
alohadb_restapi_status(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	char		port_str[16];

	InitMaterializedSRF(fcinfo, 0);

	/* port */
	snprintf(port_str, sizeof(port_str), "%d", restapi_port);
	{
		Datum	values[2];
		bool	nulls[2] = {false, false};

		values[0] = CStringGetTextDatum("port");
		values[1] = CStringGetTextDatum(port_str);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	/* database */
	{
		Datum	values[2];
		bool	nulls[2] = {false, false};

		values[0] = CStringGetTextDatum("database");
		values[1] = CStringGetTextDatum(restapi_database ? restapi_database : "postgres");
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	/* schema */
	{
		Datum	values[2];
		bool	nulls[2] = {false, false};

		values[0] = CStringGetTextDatum("schema");
		values[1] = CStringGetTextDatum(restapi_schema ? restapi_schema : "public");
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	/* default_role */
	{
		Datum	values[2];
		bool	nulls[2] = {false, false};

		values[0] = CStringGetTextDatum("default_role");
		values[1] = CStringGetTextDatum(restapi_default_role ? restapi_default_role : "");
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	return (Datum) 0;
}

/* ----------------------------------------------------------------
 * alohadb_restapi_endpoints
 *
 * Queries pg_catalog.pg_tables for tables in the configured schema
 * and returns the auto-generated endpoint list.
 * ---------------------------------------------------------------- */
Datum
alohadb_restapi_endpoints(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	StringInfoData query;
	int			ret;
	uint64		i;
	const char *schema;

	InitMaterializedSRF(fcinfo, 0);

	schema = (restapi_schema && restapi_schema[0]) ? restapi_schema : "public";

	/* Query pg_tables for tables in the configured schema */
	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT tablename FROM pg_catalog.pg_tables "
					 "WHERE schemaname = %s ORDER BY tablename",
					 quote_literal_cstr(schema));

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(query.data, true, 0);

	if (ret != SPI_OK_SELECT)
	{
		PopActiveSnapshot();
		SPI_finish();
		pfree(query.data);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_restapi: failed to query pg_tables")));
	}

	for (i = 0; i < SPI_processed; i++)
	{
		char	   *tablename;
		char		path_buf[512];
		static const char *methods[] = {"GET", "POST", "PUT", "DELETE"};
		static const char *descs[] = {
			"List rows or get by ID",
			"Insert a new row",
			"Update a row by ID",
			"Delete a row by ID"
		};
		int			m;

		tablename = SPI_getvalue(SPI_tuptable->vals[i],
								SPI_tuptable->tupdesc, 1);
		if (!tablename)
			continue;

		for (m = 0; m < 4; m++)
		{
			Datum	values[3];
			bool	nulls[3] = {false, false, false};

			snprintf(path_buf, sizeof(path_buf), "/api/%s", tablename);

			values[0] = CStringGetTextDatum(methods[m]);
			values[1] = CStringGetTextDatum(path_buf);
			values[2] = CStringGetTextDatum(descs[m]);

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
								 values, nulls);
		}
	}

	PopActiveSnapshot();
	SPI_finish();
	pfree(query.data);

	return (Datum) 0;
}
