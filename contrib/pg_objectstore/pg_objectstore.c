/*-------------------------------------------------------------------------
 *
 * pg_objectstore.c
 *	  SQL-callable functions for cloud object-store access.
 *
 *	  Dispatches to provider-specific implementations:
 *	    S3    -> objectstore_s3.c   (SigV4 / HMAC-SHA256)
 *	    GCS   -> objectstore_http.c (OAuth2 + JSON REST)
 *	    Azure -> objectstore_http.c (SAS / HMAC-SHA256)
 *
 *	  All signing uses open standards (RFC 2104, RFC 6749, publicly
 *	  documented AWS/GCS/Azure protocols); no proprietary SDK code.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_objectstore/pg_objectstore.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#include "pg_objectstore.h"

PG_MODULE_MAGIC_EXT(
					.name = "pg_objectstore",
					.version = "1.0"
);

PG_FUNCTION_INFO_V1(objectstore_get);
PG_FUNCTION_INFO_V1(objectstore_get_text);
PG_FUNCTION_INFO_V1(objectstore_put);
PG_FUNCTION_INFO_V1(objectstore_list);
PG_FUNCTION_INFO_V1(objectstore_delete);

/* ----------------------------------------------------------------
 * Internal: fetch credential config jsonb for a label.
 *
 * Must be called inside an active SPI session.
 * Returns a palloc'd cstring of the config JSON, or errors out.
 * ---------------------------------------------------------------- */
static char *
fetch_credential_config(const char *label, char *out_provider, int provider_len)
{
	StringInfoData sql;
	int			ret;
	char	   *config_json;
	char	   *provider;

	initStringInfo(&sql);
	appendStringInfo(&sql,
		"SELECT provider, config::text "
		"FROM   public.pg_objectstore_credentials "
		"WHERE  label = %s",
		quote_literal_cstr(label));

	ret = SPI_execute(sql.data, true, 1);
	pfree(sql.data);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
		ereport(ERROR,
				(errcode(ERRCODE_NO_DATA_FOUND),
				 errmsg("objectstore: credential label \"%s\" not found", label)));

	provider = SPI_getvalue(SPI_tuptable->vals[0],
							SPI_tuptable->tupdesc, 1);
	strlcpy(out_provider, provider ? provider : "", provider_len);

	config_json = SPI_getvalue(SPI_tuptable->vals[0],
							   SPI_tuptable->tupdesc, 2);
	if (!config_json)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("objectstore: credential \"%s\" has null config", label)));

	/* Copy out of SPI memory */
	return pstrdup(config_json);
}

/* ----------------------------------------------------------------
 * Internal: log an operation to pg_objectstore_log.
 * Called within an active SPI session.
 * ---------------------------------------------------------------- */
static void
log_operation(const char *operation, const char *uri,
			  int64 bytes_transferred, double duration_ms,
			  bool success, const char *error_message)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
		"INSERT INTO public.pg_objectstore_log "
		"    (operation, uri, bytes_transferred, duration_ms, "
		"     success, error_message) "
		"VALUES (%s, %s, " INT64_FORMAT ", %f, %s, %s)",
		quote_literal_cstr(operation),
		quote_literal_cstr(uri),
		bytes_transferred,
		duration_ms,
		success ? "true" : "false",
		error_message ? quote_literal_cstr(error_message) : "NULL");

	SPI_execute(sql.data, false, 0);
	pfree(sql.data);
}

/* ----------------------------------------------------------------
 * objectstore_get(uri text, credential_label text) RETURNS bytea
 * ---------------------------------------------------------------- */
Datum
objectstore_get(PG_FUNCTION_ARGS)
{
	char   *uri   = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char   *label = text_to_cstring(PG_GETARG_TEXT_PP(1));
	char	provider[32];
	char   *config_json;
	bytea  *result;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	config_json = fetch_credential_config(label, provider, sizeof(provider));
	PopActiveSnapshot();
	SPI_finish();

	/* Dispatch to provider implementation */
	if (strcmp(provider, OBJSTORE_PROVIDER_S3) == 0)
		result = s3_object_get(uri, config_json);
	else if (strcmp(provider, OBJSTORE_PROVIDER_GCS) == 0)
		result = http_object_get(uri, config_json, "gcs");
	else if (strcmp(provider, OBJSTORE_PROVIDER_AZURE) == 0)
		result = http_object_get(uri, config_json, "azure");
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("objectstore: unknown provider \"%s\"", provider)));

	/* Log the operation */
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	log_operation("GET", uri,
				  result ? (int64) VARSIZE_ANY_EXHDR(result) : 0,
				  0.0, result != NULL, NULL);
	PopActiveSnapshot();
	SPI_finish();

	if (!result)
		PG_RETURN_NULL();

	PG_RETURN_BYTEA_P(result);
}

/* ----------------------------------------------------------------
 * objectstore_get_text(uri text, credential_label text) RETURNS text
 * ---------------------------------------------------------------- */
Datum
objectstore_get_text(PG_FUNCTION_ARGS)
{
	char   *uri   = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char   *label = text_to_cstring(PG_GETARG_TEXT_PP(1));
	char	provider[32];
	char   *config_json;
	bytea  *raw;
	text   *result;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	config_json = fetch_credential_config(label, provider, sizeof(provider));
	PopActiveSnapshot();
	SPI_finish();

	if (strcmp(provider, OBJSTORE_PROVIDER_S3) == 0)
		raw = s3_object_get(uri, config_json);
	else if (strcmp(provider, OBJSTORE_PROVIDER_GCS) == 0)
		raw = http_object_get(uri, config_json, "gcs");
	else if (strcmp(provider, OBJSTORE_PROVIDER_AZURE) == 0)
		raw = http_object_get(uri, config_json, "azure");
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("objectstore: unknown provider \"%s\"", provider)));

	if (!raw)
		PG_RETURN_NULL();

	result = cstring_to_text_with_len(VARDATA_ANY(raw),
									  VARSIZE_ANY_EXHDR(raw));
	PG_RETURN_TEXT_P(result);
}

/* ----------------------------------------------------------------
 * objectstore_put(uri text, data bytea, credential_label text)
 * RETURNS void
 * ---------------------------------------------------------------- */
Datum
objectstore_put(PG_FUNCTION_ARGS)
{
	char   *uri   = text_to_cstring(PG_GETARG_TEXT_PP(0));
	bytea  *data  = PG_GETARG_BYTEA_PP(1);
	char   *label = text_to_cstring(PG_GETARG_TEXT_PP(2));
	char	provider[32];
	char   *config_json;
	bool	ok;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	config_json = fetch_credential_config(label, provider, sizeof(provider));
	PopActiveSnapshot();
	SPI_finish();

	if (strcmp(provider, OBJSTORE_PROVIDER_S3) == 0)
		ok = s3_object_put(uri, data, config_json);
	else if (strcmp(provider, OBJSTORE_PROVIDER_GCS) == 0)
		ok = http_object_put(uri, data, config_json, "gcs");
	else if (strcmp(provider, OBJSTORE_PROVIDER_AZURE) == 0)
		ok = http_object_put(uri, data, config_json, "azure");
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("objectstore: unknown provider \"%s\"", provider)));

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	log_operation("PUT", uri,
				  ok ? (int64) VARSIZE_ANY_EXHDR(data) : 0,
				  0.0, ok, ok ? NULL : "PUT failed");
	PopActiveSnapshot();
	SPI_finish();

	if (!ok)
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("objectstore: PUT to \"%s\" failed", uri)));

	PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * objectstore_list(prefix text, credential_label text,
 *                  max_keys int DEFAULT 1000)
 * RETURNS TABLE(key text, size_bytes bigint,
 *               last_modified timestamptz, etag text)
 * ---------------------------------------------------------------- */
Datum
objectstore_list(PG_FUNCTION_ARGS)
{
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext	per_query_ctx;
	MemoryContext	oldcxt;
	char		   *prefix;
	char		   *label;
	int				max_keys;
	char			provider[32];
	char		   *config_json;
	ObjstoreListResult *items;
	int				nresults;
	int				i;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required")));

	prefix   = text_to_cstring(PG_GETARG_TEXT_PP(0));
	label    = text_to_cstring(PG_GETARG_TEXT_PP(1));
	max_keys = PG_GETARG_INT32(2);

	tupdesc = CreateTemplateTupleDesc(4);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "key", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "size_bytes", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "last_modified", TIMESTAMPTZOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "etag", TEXTOID, -1, 0);

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcxt = MemoryContextSwitchTo(per_query_ctx);
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = BlessTupleDesc(tupdesc);
	MemoryContextSwitchTo(oldcxt);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	config_json = fetch_credential_config(label, provider, sizeof(provider));
	PopActiveSnapshot();
	SPI_finish();

	if (strcmp(provider, OBJSTORE_PROVIDER_S3) == 0)
		items = s3_object_list(prefix, config_json, max_keys, &nresults);
	else if (strcmp(provider, OBJSTORE_PROVIDER_GCS) == 0)
		items = http_object_list(prefix, config_json, "gcs", max_keys, &nresults);
	else if (strcmp(provider, OBJSTORE_PROVIDER_AZURE) == 0)
		items = http_object_list(prefix, config_json, "azure", max_keys, &nresults);
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("objectstore: unknown provider \"%s\"", provider)));

	for (i = 0; i < nresults; i++)
	{
		Datum	values[4];
		bool	nulls[4];

		memset(nulls, 0, sizeof(nulls));
		values[0] = CStringGetTextDatum(items[i].key);
		values[1] = Int64GetDatum(items[i].size_bytes);
		if (items[i].last_modified > 0)
			values[2] = TimestampTzGetDatum(
				(TimestampTz) items[i].last_modified * USECS_PER_SEC -
				(POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) *
				USECS_PER_DAY);
		else
			nulls[2] = true;
		if (items[i].etag[0])
			values[3] = CStringGetTextDatum(items[i].etag);
		else
			nulls[3] = true;

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	return (Datum) 0;
}

/* ----------------------------------------------------------------
 * objectstore_delete(uri text, credential_label text) RETURNS void
 * ---------------------------------------------------------------- */
Datum
objectstore_delete(PG_FUNCTION_ARGS)
{
	char   *uri   = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char   *label = text_to_cstring(PG_GETARG_TEXT_PP(1));
	char	provider[32];
	char   *config_json;
	bool	ok;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	config_json = fetch_credential_config(label, provider, sizeof(provider));
	PopActiveSnapshot();
	SPI_finish();

	if (strcmp(provider, OBJSTORE_PROVIDER_S3) == 0)
		ok = s3_object_delete(uri, config_json);
	else if (strcmp(provider, OBJSTORE_PROVIDER_GCS) == 0)
		ok = http_object_delete(uri, config_json, "gcs");
	else if (strcmp(provider, OBJSTORE_PROVIDER_AZURE) == 0)
		ok = http_object_delete(uri, config_json, "azure");
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("objectstore: unknown provider \"%s\"", provider)));

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	log_operation("DELETE", uri, 0, 0.0, ok,
				  ok ? NULL : "DELETE failed");
	PopActiveSnapshot();
	SPI_finish();

	if (!ok)
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("objectstore: DELETE \"%s\" failed", uri)));

	PG_RETURN_VOID();
}
