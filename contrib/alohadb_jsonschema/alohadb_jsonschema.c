/*-------------------------------------------------------------------------
 *
 * alohadb_jsonschema.c
 *	  Main entry point for the alohadb_jsonschema extension.
 *
 *	  Provides SQL-callable functions for JSON Schema Draft-07
 *	  validation of JSONB documents, including a boolean validator
 *	  for use in CHECK constraints, a set-returning function for
 *	  detailed error reporting, a schema registry, and named
 *	  schema validation.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_jsonschema/alohadb_jsonschema.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

#include "alohadb_jsonschema.h"

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_jsonschema",
					.version = "1.0"
);

/* SQL-callable function declarations */
PG_FUNCTION_INFO_V1(jsonschema_is_valid);
PG_FUNCTION_INFO_V1(jsonschema_validate);
PG_FUNCTION_INFO_V1(jsonschema_register);
PG_FUNCTION_INFO_V1(jsonschema_validate_named);

/* ----------------------------------------------------------------
 * jsonschema_is_valid
 *
 * Validate a JSONB document against a JSON Schema.  Returns true
 * if the document is valid.  Marked IMMUTABLE STRICT PARALLEL SAFE
 * so it can be used in CHECK constraints.
 *
 * SQL signature:
 *   jsonschema_is_valid(doc jsonb, schema jsonb) RETURNS boolean
 * ---------------------------------------------------------------- */
Datum
jsonschema_is_valid(PG_FUNCTION_ARGS)
{
	Jsonb	   *doc = PG_GETARG_JSONB_P(0);
	Jsonb	   *schema = PG_GETARG_JSONB_P(1);
	JsonSchemaResult *result;
	MemoryContext result_mcxt;
	bool		valid;

	result_mcxt = AllocSetContextCreate(CurrentMemoryContext,
										"jsonschema_is_valid result",
										ALLOCSET_DEFAULT_SIZES);

	result = jsonschema_validate_internal(doc, schema, result_mcxt);
	valid = result->valid;

	MemoryContextDelete(result_mcxt);

	PG_RETURN_BOOL(valid);
}

/* ----------------------------------------------------------------
 * jsonschema_validate
 *
 * Validate a JSONB document against a JSON Schema, returning
 * detailed results as a set of rows.
 *
 * If the document is valid, a single row is returned with
 * valid=true and null error fields.  If invalid, one row is
 * returned per error.
 *
 * SQL signature:
 *   jsonschema_validate(doc jsonb, schema jsonb)
 *     RETURNS TABLE(valid boolean, error_path text, error_message text)
 * ---------------------------------------------------------------- */
Datum
jsonschema_validate(PG_FUNCTION_ARGS)
{
	Jsonb	   *doc = PG_GETARG_JSONB_P(0);
	Jsonb	   *schema = PG_GETARG_JSONB_P(1);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	JsonSchemaResult *result;
	MemoryContext result_mcxt;

	InitMaterializedSRF(fcinfo, 0);

	result_mcxt = AllocSetContextCreate(CurrentMemoryContext,
										"jsonschema_validate result",
										ALLOCSET_DEFAULT_SIZES);

	result = jsonschema_validate_internal(doc, schema, result_mcxt);

	if (result->valid)
	{
		/* Return one row: valid=true, null path and message */
		Datum		values[3];
		bool		nulls[3];

		memset(nulls, 0, sizeof(nulls));
		values[0] = BoolGetDatum(true);
		nulls[1] = true;
		nulls[2] = true;

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}
	else
	{
		ListCell   *lc;

		foreach(lc, result->errors)
		{
			JsonSchemaError *err = (JsonSchemaError *) lfirst(lc);
			Datum		values[3];
			bool		nulls[3];

			memset(nulls, 0, sizeof(nulls));
			values[0] = BoolGetDatum(false);
			values[1] = CStringGetTextDatum(err->error_path);
			values[2] = CStringGetTextDatum(err->error_message);

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
								 values, nulls);
		}
	}

	MemoryContextDelete(result_mcxt);

	return (Datum) 0;
}

/* ----------------------------------------------------------------
 * jsonschema_register
 *
 * Store a named JSON Schema in the alohadb_jsonschema_registry
 * table for later use with jsonschema_validate_named.
 *
 * SQL signature:
 *   jsonschema_register(schema_name text, schema_doc jsonb) RETURNS void
 * ---------------------------------------------------------------- */
Datum
jsonschema_register(PG_FUNCTION_ARGS)
{
	text	   *schema_name = PG_GETARG_TEXT_PP(0);
	Jsonb	   *schema_doc = PG_GETARG_JSONB_P(1);
	char	   *name_cstr;
	char	   *doc_cstr;
	StringInfoData query;
	int			ret;

	name_cstr = text_to_cstring(schema_name);
	doc_cstr = JsonbToCString(NULL, &schema_doc->root, VARSIZE(schema_doc));

	initStringInfo(&query);
	appendStringInfo(&query,
		"INSERT INTO alohadb_jsonschema_registry (schema_name, schema_doc) "
		"VALUES (%s, %s::jsonb) "
		"ON CONFLICT (schema_name) DO UPDATE SET schema_doc = EXCLUDED.schema_doc, "
		"created_at = now()",
		quote_literal_cstr(name_cstr),
		quote_literal_cstr(doc_cstr));

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(query.data, false, 0);
	if (ret != SPI_OK_INSERT)
		elog(ERROR, "alohadb_jsonschema: failed to register schema '%s': SPI error %d",
			 name_cstr, ret);

	PopActiveSnapshot();
	SPI_finish();

	pfree(query.data);
	pfree(name_cstr);
	pfree(doc_cstr);

	PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * jsonschema_validate_named
 *
 * Look up a schema by name from the registry and validate the
 * document against it.  Returns true if valid.
 *
 * SQL signature:
 *   jsonschema_validate_named(doc jsonb, schema_name text) RETURNS boolean
 * ---------------------------------------------------------------- */
Datum
jsonschema_validate_named(PG_FUNCTION_ARGS)
{
	Jsonb	   *doc = PG_GETARG_JSONB_P(0);
	text	   *schema_name = PG_GETARG_TEXT_PP(1);
	char	   *name_cstr;
	StringInfoData query;
	int			ret;
	bool		valid;
	Jsonb	   *schema_doc;
	Datum		schema_datum;
	bool		isnull;
	JsonSchemaResult *result;
	MemoryContext result_mcxt;

	name_cstr = text_to_cstring(schema_name);

	initStringInfo(&query);
	appendStringInfo(&query,
		"SELECT schema_doc FROM alohadb_jsonschema_registry "
		"WHERE schema_name = %s",
		quote_literal_cstr(name_cstr));

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(query.data, true, 1);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "alohadb_jsonschema: failed to look up schema '%s': SPI error %d",
			 name_cstr, ret);

	if (SPI_processed == 0)
	{
		PopActiveSnapshot();
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("JSON Schema '%s' not found in registry", name_cstr)));
	}

	schema_datum = SPI_getbinval(SPI_tuptable->vals[0],
								 SPI_tuptable->tupdesc, 1, &isnull);
	if (isnull)
	{
		PopActiveSnapshot();
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("schema_doc is NULL for schema '%s'", name_cstr)));
	}

	/*
	 * Copy the schema datum out of SPI memory context so it survives
	 * SPI_finish.
	 */
	schema_doc = DatumGetJsonbPCopy(schema_datum);

	PopActiveSnapshot();
	SPI_finish();

	/* Now validate */
	result_mcxt = AllocSetContextCreate(CurrentMemoryContext,
										"jsonschema_validate_named result",
										ALLOCSET_DEFAULT_SIZES);

	result = jsonschema_validate_internal(doc, schema_doc, result_mcxt);
	valid = result->valid;

	MemoryContextDelete(result_mcxt);

	pfree(query.data);
	pfree(name_cstr);

	PG_RETURN_BOOL(valid);
}
