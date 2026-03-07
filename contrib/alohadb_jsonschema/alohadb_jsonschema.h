/*-------------------------------------------------------------------------
 *
 * alohadb_jsonschema.h
 *	  Shared declarations for the alohadb_jsonschema extension.
 *
 *	  Provides JSON Schema Draft-07 validation for JSONB documents.
 *	  Supports type checking, property validation, numeric/string
 *	  constraints, pattern matching, combinators (allOf/anyOf/oneOf/not),
 *	  enum/const, and $ref resolution.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * contrib/alohadb_jsonschema/alohadb_jsonschema.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef ALOHADB_JSONSCHEMA_H
#define ALOHADB_JSONSCHEMA_H

#include "postgres.h"
#include "fmgr.h"
#include "nodes/pg_list.h"
#include "utils/jsonb.h"

/*
 * A single validation error, with a JSON Pointer-style path and a
 * human-readable message.
 */
typedef struct JsonSchemaError
{
	bool		valid;
	char	   *error_path;
	char	   *error_message;
} JsonSchemaError;

/*
 * Aggregate result of a validation run.  If valid is true, errors is NIL.
 * Otherwise errors is a List of JsonSchemaError pointers.
 */
typedef struct JsonSchemaResult
{
	List	   *errors;
	bool		valid;
} JsonSchemaResult;

/*
 * Core validation entry point.
 *
 * Validates 'doc' against 'schema'.  The root_schema is the top-level
 * schema document, used for $ref resolution.  Error strings are allocated
 * in result_mcxt so they survive after the working context is reset.
 */
extern JsonSchemaResult *jsonschema_validate_internal(Jsonb *doc,
													  Jsonb *schema,
													  MemoryContext result_mcxt);

/* SQL-callable functions */
extern Datum jsonschema_is_valid(PG_FUNCTION_ARGS);
extern Datum jsonschema_validate(PG_FUNCTION_ARGS);
extern Datum jsonschema_register(PG_FUNCTION_ARGS);
extern Datum jsonschema_validate_named(PG_FUNCTION_ARGS);

#endif							/* ALOHADB_JSONSCHEMA_H */
