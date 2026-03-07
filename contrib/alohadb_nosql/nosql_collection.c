/*-------------------------------------------------------------------------
 *
 * nosql_collection.c
 *	  Collection management functions for the alohadb_nosql extension.
 *
 *	  A "collection" is an ordinary PostgreSQL table with the schema:
 *	    _id  text PRIMARY KEY DEFAULT gen_random_uuid()::text
 *	    data jsonb NOT NULL
 *
 *	  A GIN index with jsonb_path_ops is automatically created on
 *	  the data column so that containment queries (@>) use the index.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_nosql/nosql_collection.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"

#include "alohadb_nosql.h"

PG_FUNCTION_INFO_V1(doc_create_collection);
PG_FUNCTION_INFO_V1(doc_drop_collection);
PG_FUNCTION_INFO_V1(doc_list_collections);

/* ----------------------------------------------------------------
 * doc_create_collection
 *
 * Create a new document collection (table + GIN index + metadata).
 *
 * Args: name text, schema jsonb DEFAULT NULL
 * ---------------------------------------------------------------- */
Datum
doc_create_collection(PG_FUNCTION_ARGS)
{
	text	   *name_text = PG_GETARG_TEXT_PP(0);
	char	   *name = text_to_cstring(name_text);
	const char *safe_name = quote_identifier(name);
	StringInfoData sql;
	int			ret;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/*
	 * Create the collection table.
	 */
	initStringInfo(&sql);
	appendStringInfo(&sql,
		"CREATE TABLE %s ("
		"_id text PRIMARY KEY DEFAULT gen_random_uuid()::text, "
		"data jsonb NOT NULL"
		")",
		safe_name);

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_UTILITY)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_nosql: failed to create collection table \"%s\": error code %d",
						name, ret)));

	/*
	 * Create the GIN index on the data column.
	 */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
		"CREATE INDEX %s ON %s USING GIN (data jsonb_path_ops)",
		quote_identifier(psprintf("%s_data_gin_idx", name)),
		safe_name);

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_UTILITY)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_nosql: failed to create GIN index for collection \"%s\": error code %d",
						name, ret)));

	/*
	 * If a JSON schema is provided and alohadb_jsonschema extension exists,
	 * add a CHECK constraint for schema validation.
	 */
	if (!PG_ARGISNULL(1))
	{
		text	   *schema_text = PG_GETARG_TEXT_PP(1);
		char	   *schema_str = text_to_cstring(schema_text);
		bool		has_jsonschema = false;

		/* Check whether alohadb_jsonschema extension is installed */
		ret = SPI_execute(
			"SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'alohadb_jsonschema'",
			true, 1);

		if (ret == SPI_OK_SELECT && SPI_processed > 0)
			has_jsonschema = true;

		if (has_jsonschema)
		{
			resetStringInfo(&sql);
			appendStringInfo(&sql,
				"ALTER TABLE %s ADD CONSTRAINT %s "
				"CHECK (jsonschema_is_valid(data, %s::jsonb))",
				safe_name,
				quote_identifier(psprintf("%s_schema_chk", name)),
				quote_literal_cstr(schema_str));

			ret = SPI_execute(sql.data, false, 0);
			if (ret != SPI_OK_UTILITY)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("alohadb_nosql: failed to add schema constraint for collection \"%s\": error code %d",
								name, ret)));
		}
		else
		{
			ereport(WARNING,
					(errmsg("alohadb_nosql: alohadb_jsonschema extension not found, "
							"schema validation constraint not added for collection \"%s\"",
							name)));
		}
	}

	/*
	 * Register the collection in the metadata table.
	 */
	{
		Oid		argtypes[2] = {TEXTOID, BOOLOID};
		Datum	values[2];

		values[0] = CStringGetTextDatum(name);
		values[1] = BoolGetDatum(!PG_ARGISNULL(1));

		ret = SPI_execute_with_args(
			"INSERT INTO " NOSQL_COLLECTIONS_TABLE " (name, has_schema) "
			"VALUES ($1, $2)",
			2, argtypes, values, NULL, false, 0);

		if (ret != SPI_OK_INSERT)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("alohadb_nosql: failed to register collection \"%s\" in metadata: error code %d",
							name, ret)));
	}

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * doc_drop_collection
 *
 * Drop a document collection (table + metadata).
 *
 * Args: name text
 * ---------------------------------------------------------------- */
Datum
doc_drop_collection(PG_FUNCTION_ARGS)
{
	text	   *name_text = PG_GETARG_TEXT_PP(0);
	char	   *name = text_to_cstring(name_text);
	const char *safe_name = quote_identifier(name);
	StringInfoData sql;
	int			ret;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/*
	 * Drop the table.
	 */
	initStringInfo(&sql);
	appendStringInfo(&sql, "DROP TABLE IF EXISTS %s", safe_name);

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_UTILITY)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_nosql: failed to drop collection table \"%s\": error code %d",
						name, ret)));

	/*
	 * Remove from metadata.
	 */
	{
		Oid		argtypes[1] = {TEXTOID};
		Datum	values[1];

		values[0] = CStringGetTextDatum(name);

		ret = SPI_execute_with_args(
			"DELETE FROM " NOSQL_COLLECTIONS_TABLE " WHERE name = $1",
			1, argtypes, values, NULL, false, 0);

		if (ret != SPI_OK_DELETE)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("alohadb_nosql: failed to remove collection \"%s\" from metadata: error code %d",
							name, ret)));
	}

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * doc_list_collections
 *
 * List all document collections with metadata.
 *
 * Returns: SETOF record (name text, doc_count int8, has_schema bool)
 * ---------------------------------------------------------------- */
Datum
doc_list_collections(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			ret;
	uint64		i;

	InitMaterializedSRF(fcinfo, 0);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/*
	 * For each registered collection, count the rows in its table.
	 * We use a PL/pgSQL-style approach via SPI: first fetch the list
	 * of collections, then count each.
	 */
	ret = SPI_execute(
		"SELECT name, has_schema FROM " NOSQL_COLLECTIONS_TABLE " ORDER BY name",
		true, 0);

	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_nosql: failed to list collections: error code %d", ret)));

	/*
	 * Copy results out of SPI context before running further queries.
	 */
	{
		uint64		ncols = SPI_processed;
		char	  **names;
		bool	   *schemas;
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
		names = palloc(sizeof(char *) * ncols);
		schemas = palloc(sizeof(bool) * ncols);
		MemoryContextSwitchTo(oldcxt);

		for (i = 0; i < ncols; i++)
		{
			bool	isnull;
			char   *val;

			val = SPI_getvalue(SPI_tuptable->vals[i],
							   SPI_tuptable->tupdesc, 1);
			oldcxt = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
			names[i] = pstrdup(val ? val : "");
			MemoryContextSwitchTo(oldcxt);

			schemas[i] = DatumGetBool(
				SPI_getbinval(SPI_tuptable->vals[i],
							  SPI_tuptable->tupdesc, 2, &isnull));
			if (isnull)
				schemas[i] = false;
		}

		/*
		 * Now count rows in each collection table.
		 */
		for (i = 0; i < ncols; i++)
		{
			Datum		values[3];
			bool		nulls[3];
			StringInfoData count_sql;
			int64		doc_count = 0;

			memset(nulls, 0, sizeof(nulls));

			initStringInfo(&count_sql);
			appendStringInfo(&count_sql,
				"SELECT count(*) FROM %s",
				quote_identifier(names[i]));

			ret = SPI_execute(count_sql.data, true, 0);
			if (ret == SPI_OK_SELECT && SPI_processed > 0)
			{
				bool	cnt_isnull;
				Datum	cnt_datum;

				cnt_datum = SPI_getbinval(SPI_tuptable->vals[0],
										  SPI_tuptable->tupdesc, 1,
										  &cnt_isnull);
				if (!cnt_isnull)
					doc_count = DatumGetInt64(cnt_datum);
			}

			pfree(count_sql.data);

			values[0] = CStringGetTextDatum(names[i]);
			values[1] = Int64GetDatum(doc_count);
			values[2] = BoolGetDatum(schemas[i]);

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
								values, nulls);
		}
	}

	PopActiveSnapshot();
	SPI_finish();

	return (Datum) 0;
}
