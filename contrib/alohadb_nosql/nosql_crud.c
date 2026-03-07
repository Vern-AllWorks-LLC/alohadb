/*-------------------------------------------------------------------------
 *
 * nosql_crud.c
 *	  CRUD functions for the alohadb_nosql extension.
 *
 *	  All operations use SPI to execute SQL against the underlying
 *	  collection tables.  Table names are sanitized with
 *	  quote_identifier() to prevent SQL injection.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_nosql/nosql_crud.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"

#include "alohadb_nosql.h"

PG_FUNCTION_INFO_V1(doc_insert);
PG_FUNCTION_INFO_V1(doc_insert_batch);
PG_FUNCTION_INFO_V1(doc_get);
PG_FUNCTION_INFO_V1(doc_put);
PG_FUNCTION_INFO_V1(doc_remove);
PG_FUNCTION_INFO_V1(doc_count);
PG_FUNCTION_INFO_V1(doc_patch);

/* ----------------------------------------------------------------
 * doc_insert
 *
 * Insert a single document into a collection.  If the document
 * contains an "_id" field, that value is used; otherwise one is
 * generated via gen_random_uuid().
 *
 * Args: collection text, doc jsonb
 * Returns: jsonb ({"_id": "..."})
 * ---------------------------------------------------------------- */
Datum
doc_insert(PG_FUNCTION_ARGS)
{
	text	   *coll_text = PG_GETARG_TEXT_PP(0);
	Datum		doc_datum = PG_GETARG_DATUM(1);
	char	   *collection = text_to_cstring(coll_text);
	const char *safe_coll = quote_identifier(collection);
	StringInfoData sql;
	int			ret;
	Datum		result;
	bool		isnull;

	/*
	 * Check whether the document has an _id field.
	 * If so, use it; otherwise, let the DEFAULT generate one.
	 *
	 * Strategy: INSERT with the document, extract _id from either
	 * the doc or the generated default via RETURNING.
	 *
	 * We use:
	 *   INSERT INTO coll (_id, data)
	 *   VALUES (coalesce($1->>'_id', gen_random_uuid()::text),
	 *           $1 - '_id')
	 *   RETURNING jsonb_build_object('_id', _id)
	 *
	 * We strip '_id' from data so it only lives in the _id column.
	 */
	initStringInfo(&sql);
	appendStringInfo(&sql,
		"INSERT INTO %s (_id, data) "
		"VALUES (coalesce($1->>'_id', gen_random_uuid()::text), "
		"$1 - '_id') "
		"RETURNING jsonb_build_object('_id', _id)",
		safe_coll);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	{
		Oid		argtypes[1] = {JSONBOID};
		Datum	args[1];

		args[0] = doc_datum;

		ret = SPI_execute_with_args(sql.data, 1, argtypes, args, NULL,
									false, 1);
	}

	if (ret != SPI_OK_INSERT_RETURNING)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_nosql: doc_insert failed on collection \"%s\": error code %d",
						collection, ret)));

	result = SPI_getbinval(SPI_tuptable->vals[0],
						   SPI_tuptable->tupdesc, 1, &isnull);
	if (!isnull)
		result = SPI_datumTransfer(result, false, -1);

	PopActiveSnapshot();
	SPI_finish();

	pfree(sql.data);

	if (isnull)
		PG_RETURN_NULL();

	PG_RETURN_DATUM(result);
}

/* ----------------------------------------------------------------
 * doc_insert_batch
 *
 * Insert multiple documents into a collection.
 *
 * Args: collection text, docs jsonb[]
 * Returns: jsonb ({"inserted": N})
 * ---------------------------------------------------------------- */
Datum
doc_insert_batch(PG_FUNCTION_ARGS)
{
	text	   *coll_text = PG_GETARG_TEXT_PP(0);
	ArrayType  *docs_array = PG_GETARG_ARRAYTYPE_P(1);
	char	   *collection = text_to_cstring(coll_text);
	const char *safe_coll = quote_identifier(collection);
	StringInfoData sql;
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;
	int			inserted = 0;
	int			i;
	int			ret;
	Datum		result;

	/* Deconstruct the array */
	deconstruct_array(docs_array, JSONBOID, -1, false, TYPALIGN_INT,
					  &elems, &nulls, &nelems);

	initStringInfo(&sql);
	appendStringInfo(&sql,
		"INSERT INTO %s (_id, data) "
		"VALUES (coalesce($1->>'_id', gen_random_uuid()::text), "
		"$1 - '_id')",
		safe_coll);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	for (i = 0; i < nelems; i++)
	{
		Oid		argtypes[1] = {JSONBOID};
		Datum	args[1];

		if (nulls[i])
			continue;

		args[0] = elems[i];

		ret = SPI_execute_with_args(sql.data, 1, argtypes, args, NULL,
									false, 0);

		if (ret != SPI_OK_INSERT)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("alohadb_nosql: doc_insert_batch failed on document %d in collection \"%s\": error code %d",
							i, collection, ret)));

		inserted++;
	}

	PopActiveSnapshot();
	SPI_finish();

	pfree(sql.data);

	/* Build result: {"inserted": N} */
	{
		StringInfoData res_json;

		initStringInfo(&res_json);
		appendStringInfo(&res_json, "{\"inserted\": %d}", inserted);

		result = DirectFunctionCall1(jsonb_in,
									CStringGetDatum(res_json.data));
		pfree(res_json.data);
	}

	PG_RETURN_DATUM(result);
}

/* ----------------------------------------------------------------
 * doc_get
 *
 * Fetch a single document by _id.
 *
 * Args: collection text, id text
 * Returns: jsonb (the data column) or NULL
 * ---------------------------------------------------------------- */
Datum
doc_get(PG_FUNCTION_ARGS)
{
	text	   *coll_text = PG_GETARG_TEXT_PP(0);
	text	   *id_text = PG_GETARG_TEXT_PP(1);
	char	   *collection = text_to_cstring(coll_text);
	const char *safe_coll = quote_identifier(collection);
	StringInfoData sql;
	int			ret;
	Datum		result;
	bool		isnull;

	initStringInfo(&sql);
	appendStringInfo(&sql,
		"SELECT data FROM %s WHERE _id = $1",
		safe_coll);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	{
		Oid		argtypes[1] = {TEXTOID};
		Datum	args[1];

		args[0] = PointerGetDatum(id_text);

		ret = SPI_execute_with_args(sql.data, 1, argtypes, args, NULL,
									true, 1);
	}

	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_nosql: doc_get failed on collection \"%s\": error code %d",
						collection, ret)));

	if (SPI_processed == 0)
	{
		PopActiveSnapshot();
		SPI_finish();
		pfree(sql.data);
		PG_RETURN_NULL();
	}

	result = SPI_getbinval(SPI_tuptable->vals[0],
						   SPI_tuptable->tupdesc, 1, &isnull);
	if (!isnull)
		result = SPI_datumTransfer(result, false, -1);

	PopActiveSnapshot();
	SPI_finish();

	pfree(sql.data);

	if (isnull)
		PG_RETURN_NULL();

	PG_RETURN_DATUM(result);
}

/* ----------------------------------------------------------------
 * doc_put
 *
 * Full document replace by _id.
 *
 * Args: collection text, id text, doc jsonb
 * Returns: jsonb ({"modified": 0|1})
 * ---------------------------------------------------------------- */
Datum
doc_put(PG_FUNCTION_ARGS)
{
	text	   *coll_text = PG_GETARG_TEXT_PP(0);
	text	   *id_text = PG_GETARG_TEXT_PP(1);
	Datum		doc_datum = PG_GETARG_DATUM(2);
	char	   *collection = text_to_cstring(coll_text);
	const char *safe_coll = quote_identifier(collection);
	StringInfoData sql;
	int			ret;
	uint64		modified;
	Datum		result;

	initStringInfo(&sql);
	appendStringInfo(&sql,
		"UPDATE %s SET data = $1 WHERE _id = $2",
		safe_coll);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	{
		Oid		argtypes[2] = {JSONBOID, TEXTOID};
		Datum	args[2];

		args[0] = doc_datum;
		args[1] = PointerGetDatum(id_text);

		ret = SPI_execute_with_args(sql.data, 2, argtypes, args, NULL,
									false, 0);
	}

	if (ret != SPI_OK_UPDATE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_nosql: doc_put failed on collection \"%s\": error code %d",
						collection, ret)));

	modified = SPI_processed;

	PopActiveSnapshot();
	SPI_finish();

	pfree(sql.data);

	/* Build result */
	{
		StringInfoData res_json;

		initStringInfo(&res_json);
		appendStringInfo(&res_json, "{\"modified\": %llu}",
						 (unsigned long long) modified);

		result = DirectFunctionCall1(jsonb_in,
									CStringGetDatum(res_json.data));
		pfree(res_json.data);
	}

	PG_RETURN_DATUM(result);
}

/* ----------------------------------------------------------------
 * doc_remove
 *
 * Delete documents matching a filter.
 *
 * Args: collection text, filter jsonb DEFAULT '{}'::jsonb
 * Returns: jsonb ({"deleted": N})
 * ---------------------------------------------------------------- */
Datum
doc_remove(PG_FUNCTION_ARGS)
{
	text	   *coll_text = PG_GETARG_TEXT_PP(0);
	char	   *collection = text_to_cstring(coll_text);
	const char *safe_coll = quote_identifier(collection);
	char	   *filter_str = NULL;
	StringInfoData sql;
	StringInfoData where_clause;
	int			ret;
	uint64		deleted;
	Datum		result;

	if (!PG_ARGISNULL(1))
		filter_str = DatumGetCString(DirectFunctionCall1(jsonb_out, PG_GETARG_DATUM(1)));

	/* Build WHERE clause from filter */
	initStringInfo(&where_clause);
	if (filter_str != NULL)
		nosql_filter_to_where(&where_clause, filter_str);

	/* Build DELETE statement */
	initStringInfo(&sql);
	appendStringInfo(&sql, "DELETE FROM %s", safe_coll);

	if (where_clause.len > 0)
		appendStringInfo(&sql, " WHERE %s", where_clause.data);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_DELETE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_nosql: doc_remove failed on collection \"%s\": error code %d",
						collection, ret)));

	deleted = SPI_processed;

	PopActiveSnapshot();
	SPI_finish();

	pfree(sql.data);
	pfree(where_clause.data);

	/* Build result */
	{
		StringInfoData res_json;

		initStringInfo(&res_json);
		appendStringInfo(&res_json, "{\"deleted\": %llu}",
						 (unsigned long long) deleted);

		result = DirectFunctionCall1(jsonb_in,
									CStringGetDatum(res_json.data));
		pfree(res_json.data);
	}

	PG_RETURN_DATUM(result);
}

/* ----------------------------------------------------------------
 * doc_count
 *
 * Count documents matching a filter.
 *
 * Args: collection text, filter jsonb DEFAULT '{}'::jsonb
 * Returns: int8
 * ---------------------------------------------------------------- */
Datum
doc_count(PG_FUNCTION_ARGS)
{
	text	   *coll_text = PG_GETARG_TEXT_PP(0);
	char	   *collection = text_to_cstring(coll_text);
	const char *safe_coll = quote_identifier(collection);
	char	   *filter_str = NULL;
	StringInfoData sql;
	StringInfoData where_clause;
	int			ret;
	int64		count_val = 0;

	if (!PG_ARGISNULL(1))
		filter_str = DatumGetCString(DirectFunctionCall1(jsonb_out, PG_GETARG_DATUM(1)));

	/* Build WHERE clause from filter */
	initStringInfo(&where_clause);
	if (filter_str != NULL)
		nosql_filter_to_where(&where_clause, filter_str);

	/* Build SELECT count(*) */
	initStringInfo(&sql);
	appendStringInfo(&sql, "SELECT count(*) FROM %s", safe_coll);

	if (where_clause.len > 0)
		appendStringInfo(&sql, " WHERE %s", where_clause.data);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(sql.data, true, 0);
	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_nosql: doc_count failed on collection \"%s\": error code %d",
						collection, ret)));

	if (SPI_processed > 0)
	{
		bool	isnull;
		Datum	cnt;

		cnt = SPI_getbinval(SPI_tuptable->vals[0],
							SPI_tuptable->tupdesc, 1, &isnull);
		if (!isnull)
			count_val = DatumGetInt64(cnt);
	}

	PopActiveSnapshot();
	SPI_finish();

	pfree(sql.data);
	pfree(where_clause.data);

	PG_RETURN_INT64(count_val);
}

/* ----------------------------------------------------------------
 * doc_patch
 *
 * Partial update: merge changes into matching documents.
 * Uses the jsonb concatenation operator (||) for merge.
 *
 * Args: collection text, filter jsonb, changes jsonb
 * Returns: jsonb ({"matched": N, "modified": N})
 * ---------------------------------------------------------------- */
Datum
doc_patch(PG_FUNCTION_ARGS)
{
	text	   *coll_text = PG_GETARG_TEXT_PP(0);
	Datum		filter_datum = PG_GETARG_DATUM(1);
	Datum		changes_datum = PG_GETARG_DATUM(2);
	char	   *collection = text_to_cstring(coll_text);
	const char *safe_coll = quote_identifier(collection);
	char	   *filter_str;
	StringInfoData sql;
	StringInfoData where_clause;
	StringInfoData count_sql;
	int			ret;
	uint64		modified;
	int64		matched = 0;
	Datum		result;

	/* Get filter as text for the where-clause builder */
	{
		Datum	filter_text_datum;

		filter_text_datum = DirectFunctionCall1(jsonb_out, filter_datum);
		filter_str = DatumGetCString(filter_text_datum);
	}

	/* Count matching rows first (for "matched" in the result) */
	initStringInfo(&where_clause);
	nosql_filter_to_where(&where_clause, filter_str);

	initStringInfo(&count_sql);
	appendStringInfo(&count_sql, "SELECT count(*) FROM %s", safe_coll);
	if (where_clause.len > 0)
		appendStringInfo(&count_sql, " WHERE %s", where_clause.data);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(count_sql.data, true, 0);
	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		bool	isnull;
		Datum	cnt;

		cnt = SPI_getbinval(SPI_tuptable->vals[0],
							SPI_tuptable->tupdesc, 1, &isnull);
		if (!isnull)
			matched = DatumGetInt64(cnt);
	}

	/* Now do the UPDATE with || merge */
	initStringInfo(&sql);
	appendStringInfo(&sql,
		"UPDATE %s SET data = data || $1",
		safe_coll);

	if (where_clause.len > 0)
		appendStringInfo(&sql, " WHERE %s", where_clause.data);

	{
		Oid		argtypes[1] = {JSONBOID};
		Datum	args[1];

		args[0] = changes_datum;

		ret = SPI_execute_with_args(sql.data, 1, argtypes, args, NULL,
									false, 0);
	}

	if (ret != SPI_OK_UPDATE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_nosql: doc_patch failed on collection \"%s\": error code %d",
						collection, ret)));

	modified = SPI_processed;

	PopActiveSnapshot();
	SPI_finish();

	/* sql.data was allocated inside SPI context; already freed by SPI_finish */
	pfree(count_sql.data);
	pfree(where_clause.data);

	/* Build result */
	{
		StringInfoData res_json;

		initStringInfo(&res_json);
		appendStringInfo(&res_json,
			"{\"matched\": %lld, \"modified\": %llu}",
			(long long) matched, (unsigned long long) modified);

		result = DirectFunctionCall1(jsonb_in,
									CStringGetDatum(res_json.data));
		pfree(res_json.data);
	}

	PG_RETURN_DATUM(result);
}
