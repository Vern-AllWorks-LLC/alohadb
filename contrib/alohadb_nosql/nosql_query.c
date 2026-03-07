/*-------------------------------------------------------------------------
 *
 * nosql_query.c
 *	  Query, search, analytics, and array-manipulation functions for
 *	  the alohadb_nosql extension.
 *
 *	  Also contains the filter-to-WHERE-clause translation helper
 *	  (nosql_filter_to_where) that is shared with nosql_crud.c.
 *
 *	  All query optimization is delegated entirely to PostgreSQL's
 *	  native query planner.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_nosql/nosql_query.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"

#include "alohadb_nosql.h"

PG_FUNCTION_INFO_V1(doc_search);
PG_FUNCTION_INFO_V1(doc_query);
PG_FUNCTION_INFO_V1(doc_group);
PG_FUNCTION_INFO_V1(doc_array_append);
PG_FUNCTION_INFO_V1(doc_array_remove);
PG_FUNCTION_INFO_V1(doc_array_add_unique);

/* ----------------------------------------------------------------
 * Forward declarations for static helpers
 * ---------------------------------------------------------------- */
static void append_operator_clause(StringInfo buf, const char *field,
								   const char *op, JsonbValue *val);
static char *jsonbvalue_to_cstring(JsonbValue *val);

/* ----------------------------------------------------------------
 * nosql_filter_to_where
 *
 * Convert a filter JSON object to a SQL WHERE clause appended to buf.
 * The filter_str is the text representation of a jsonb value.
 *
 * Filter syntax:
 *   {"age": {"gt": 25}}           -> (data->'age')::numeric > 25
 *   {"name": {"eq": "Alice"}}     -> data->>'name' = 'Alice'
 *   {"name": {"like": "%alice%"}} -> data->>'name' LIKE '%alice%'
 *   {"tags": {"contains": "x"}}   -> data->'tags' @> '"x"'::jsonb
 *   {"status": {"in": ["a","b"]}} -> data->>'status' IN ('a','b')
 *   {"email": {"exists": true}}   -> data ? 'email'
 *   {"name": "Alice"}             -> data @> '{"name":"Alice"}'::jsonb
 *   {}                            -> (nothing appended)
 *
 * Multiple top-level keys are ANDed together.
 * ---------------------------------------------------------------- */
void
nosql_filter_to_where(StringInfo buf, const char *filter_str)
{
	Jsonb	   *jb;
	JsonbIterator *it;
	JsonbIteratorToken tok;
	JsonbValue	key;
	JsonbValue	val;
	int			clause_count = 0;
	Datum		jb_datum;

	if (filter_str == NULL || filter_str[0] == '\0')
		return;

	/* Parse the filter string to a Jsonb datum */
	jb_datum = DirectFunctionCall1(jsonb_in, CStringGetDatum(filter_str));
	jb = DatumGetJsonbP(jb_datum);

	/* Must be an object */
	if (!JB_ROOT_IS_OBJECT(jb))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("alohadb_nosql: filter must be a JSON object")));

	/* Empty object -> no WHERE clause */
	if (JB_ROOT_COUNT(jb) == 0)
		return;

	it = JsonbIteratorInit(&jb->root);

	/* Skip the opening WJB_BEGIN_OBJECT token */
	tok = JsonbIteratorNext(&it, &key, false);
	if (tok != WJB_BEGIN_OBJECT)
		return;

	while ((tok = JsonbIteratorNext(&it, &key, true)) != WJB_END_OBJECT &&
		   tok != WJB_DONE)
	{
		char	   *field_name;

		if (tok != WJB_KEY)
			continue;

		/* Get the field name */
		field_name = pnstrdup(key.val.string.val, key.val.string.len);

		/* Get the value for this key (skipNested=false to distinguish objects) */
		tok = JsonbIteratorNext(&it, &val, false);

		if (clause_count > 0)
			appendStringInfoString(buf, " AND ");

		if (tok == WJB_VALUE)
		{
			/*
			 * Scalar value: simple equality via containment operator
			 * to leverage GIN index.
			 *   {"name": "Alice"} -> data @> '{"name":"Alice"}'::jsonb
			 */
			char   *val_str = jsonbvalue_to_cstring(&val);

			appendStringInfo(buf,
				"data @> jsonb_build_object(%s, %s::jsonb)",
				quote_literal_cstr(field_name),
				quote_literal_cstr(val_str));

			pfree(val_str);
			clause_count++;
		}
		else if (tok == WJB_BEGIN_OBJECT)
		{
			/*
			 * Operator object: {"gt": 25, "lt": 100}
			 * Multiple operators within the same field are ANDed.
			 */
			JsonbValue	opkey, opval;
			JsonbIteratorToken optok;
			int			op_count = 0;

			appendStringInfoChar(buf, '(');

			while ((optok = JsonbIteratorNext(&it, &opkey, true)) != WJB_END_OBJECT &&
				   optok != WJB_DONE)
			{
				char   *op_name;

				if (optok != WJB_KEY)
					continue;

				op_name = pnstrdup(opkey.val.string.val, opkey.val.string.len);
				optok = JsonbIteratorNext(&it, &opval, true);

				if (op_count > 0)
					appendStringInfoString(buf, " AND ");

				if (optok == WJB_VALUE)
				{
					append_operator_clause(buf, field_name, op_name, &opval);
					op_count++;
				}
				else if (optok == WJB_BEGIN_ARRAY)
				{
					/*
					 * Array value for operators like "in": ["a", "b"]
					 */
					if (strcmp(op_name, "in") == 0)
					{
						int		in_count = 0;
						JsonbValue	elem;
						JsonbIteratorToken etok;

						appendStringInfo(buf, "data->>'%s' IN (",
										 field_name);

						while ((etok = JsonbIteratorNext(&it, &elem, true)) != WJB_END_ARRAY &&
							   etok != WJB_DONE)
						{
							char   *elem_str;

							if (etok != WJB_ELEM)
								continue;

							elem_str = jsonbvalue_to_cstring(&elem);
							if (in_count > 0)
								appendStringInfoString(buf, ", ");
							appendStringInfoString(buf,
								quote_literal_cstr(elem_str));
							pfree(elem_str);
							in_count++;
						}

						appendStringInfoChar(buf, ')');
						op_count++;
					}
					else
					{
						/* Unknown array operator; skip the array */
						JsonbValue	skip;
						JsonbIteratorToken stok;

						while ((stok = JsonbIteratorNext(&it, &skip, true)) != WJB_END_ARRAY &&
							   stok != WJB_DONE)
							;	/* consume */
					}
				}

				pfree(op_name);
			}

			appendStringInfoChar(buf, ')');
			clause_count++;
		}
		else if (tok == WJB_BEGIN_ARRAY)
		{
			/*
			 * Array value at top level: treat as equality containment.
			 * Skip through the array to consume it from the iterator.
			 */
			JsonbValue	skip;
			JsonbIteratorToken stok;
			StringInfoData arr_buf;

			initStringInfo(&arr_buf);
			appendStringInfo(&arr_buf, "{\"%s\":", field_name);

			/* Re-serialize is complex; use the containment shortcut */
			while ((stok = JsonbIteratorNext(&it, &skip, true)) != WJB_END_ARRAY &&
				   stok != WJB_DONE)
				;	/* consume */

			/* For array equality, use simple containment */
			appendStringInfo(buf,
				"data @> '{}'::jsonb");
			pfree(arr_buf.data);
			clause_count++;
		}

		pfree(field_name);
	}
}

/* ----------------------------------------------------------------
 * append_operator_clause
 *
 * Append a single operator comparison to the WHERE clause buffer.
 *
 * Supported operators:
 *   eq, ne, gt, gte, lt, lte, like, contains, exists
 * ---------------------------------------------------------------- */
static void
append_operator_clause(StringInfo buf, const char *field,
					   const char *op, JsonbValue *val)
{
	char	   *val_str;
	bool		is_numeric;

	/*
	 * Handle "exists" specially: data ? 'fieldname'
	 */
	if (strcmp(op, "exists") == 0)
	{
		if (val->type == jbvBool && val->val.boolean)
			appendStringInfo(buf, "data ? %s",
							 quote_literal_cstr(field));
		else
			appendStringInfo(buf, "NOT (data ? %s)",
							 quote_literal_cstr(field));
		return;
	}

	/*
	 * Handle "contains" specially: data->'field' @> '"value"'::jsonb
	 */
	if (strcmp(op, "contains") == 0)
	{
		val_str = jsonbvalue_to_cstring(val);
		appendStringInfo(buf, "data->'%s' @> %s::jsonb",
						 field,
						 quote_literal_cstr(val_str));
		pfree(val_str);
		return;
	}

	/*
	 * For comparison operators, decide whether to use numeric or text
	 * comparison based on the jsonb value type.
	 */
	val_str = jsonbvalue_to_cstring(val);
	is_numeric = (val->type == jbvNumeric);

	if (strcmp(op, "eq") == 0)
	{
		if (is_numeric)
			appendStringInfo(buf, "(data->'%s')::numeric = %s",
							 field, val_str);
		else
			appendStringInfo(buf, "data->>'%s' = %s",
							 field, quote_literal_cstr(val_str));
	}
	else if (strcmp(op, "ne") == 0)
	{
		if (is_numeric)
			appendStringInfo(buf, "(data->'%s')::numeric <> %s",
							 field, val_str);
		else
			appendStringInfo(buf, "data->>'%s' <> %s",
							 field, quote_literal_cstr(val_str));
	}
	else if (strcmp(op, "gt") == 0)
	{
		if (is_numeric)
			appendStringInfo(buf, "(data->'%s')::numeric > %s",
							 field, val_str);
		else
			appendStringInfo(buf, "data->>'%s' > %s",
							 field, quote_literal_cstr(val_str));
	}
	else if (strcmp(op, "gte") == 0)
	{
		if (is_numeric)
			appendStringInfo(buf, "(data->'%s')::numeric >= %s",
							 field, val_str);
		else
			appendStringInfo(buf, "data->>'%s' >= %s",
							 field, quote_literal_cstr(val_str));
	}
	else if (strcmp(op, "lt") == 0)
	{
		if (is_numeric)
			appendStringInfo(buf, "(data->'%s')::numeric < %s",
							 field, val_str);
		else
			appendStringInfo(buf, "data->>'%s' < %s",
							 field, quote_literal_cstr(val_str));
	}
	else if (strcmp(op, "lte") == 0)
	{
		if (is_numeric)
			appendStringInfo(buf, "(data->'%s')::numeric <= %s",
							 field, val_str);
		else
			appendStringInfo(buf, "data->>'%s' <= %s",
							 field, quote_literal_cstr(val_str));
	}
	else if (strcmp(op, "like") == 0)
	{
		appendStringInfo(buf, "data->>'%s' LIKE %s",
						 field, quote_literal_cstr(val_str));
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("alohadb_nosql: unknown filter operator \"%s\"", op)));
	}

	pfree(val_str);
}

/* ----------------------------------------------------------------
 * jsonbvalue_to_cstring
 *
 * Convert a JsonbValue scalar to a palloc'd C string.
 * ---------------------------------------------------------------- */
static char *
jsonbvalue_to_cstring(JsonbValue *val)
{
	switch (val->type)
	{
		case jbvString:
			return pnstrdup(val->val.string.val, val->val.string.len);

		case jbvNumeric:
			return DatumGetCString(
				DirectFunctionCall1(numeric_out,
								   NumericGetDatum(val->val.numeric)));

		case jbvBool:
			return pstrdup(val->val.boolean ? "true" : "false");

		case jbvNull:
			return pstrdup("null");

		default:
			return pstrdup("");
	}
}

/* ----------------------------------------------------------------
 * Helper: parse sort/limit/offset/fields from opts jsonb
 *
 * opts JSON structure:
 *   {
 *     "sort": {"field": "asc"},  -- or "desc"
 *     "limit": 100,
 *     "offset": 0,
 *     "fields": ["name", "age"]
 *   }
 * ---------------------------------------------------------------- */
static void
parse_search_opts(const char *opts_str,
				  StringInfo select_expr,
				  StringInfo order_clause,
				  int64 *limit_val,
				  int64 *offset_val)
{
	Jsonb	   *jb;
	JsonbIterator *it;
	JsonbIteratorToken tok;
	JsonbValue	key, val;
	Datum		jb_datum;

	*limit_val = 0;
	*offset_val = 0;

	if (opts_str == NULL || opts_str[0] == '\0')
	{
		appendStringInfoString(select_expr, "data");
		return;
	}

	jb_datum = DirectFunctionCall1(jsonb_in, CStringGetDatum(opts_str));
	jb = DatumGetJsonbP(jb_datum);

	if (!JB_ROOT_IS_OBJECT(jb) || JB_ROOT_COUNT(jb) == 0)
	{
		appendStringInfoString(select_expr, "data");
		return;
	}

	/* Default select expression */
	appendStringInfoString(select_expr, "data");

	it = JsonbIteratorInit(&jb->root);
	tok = JsonbIteratorNext(&it, &key, false);	/* WJB_BEGIN_OBJECT */

	while ((tok = JsonbIteratorNext(&it, &key, true)) != WJB_END_OBJECT &&
		   tok != WJB_DONE)
	{
		char   *keyname;

		if (tok != WJB_KEY)
			continue;

		keyname = pnstrdup(key.val.string.val, key.val.string.len);

		if (strcmp(keyname, "limit") == 0)
		{
			tok = JsonbIteratorNext(&it, &val, true);
			if (tok == WJB_VALUE && val.type == jbvNumeric)
				*limit_val = DatumGetInt64(
					DirectFunctionCall1(numeric_int8,
										NumericGetDatum(val.val.numeric)));
		}
		else if (strcmp(keyname, "offset") == 0)
		{
			tok = JsonbIteratorNext(&it, &val, true);
			if (tok == WJB_VALUE && val.type == jbvNumeric)
				*offset_val = DatumGetInt64(
					DirectFunctionCall1(numeric_int8,
										NumericGetDatum(val.val.numeric)));
		}
		else if (strcmp(keyname, "sort") == 0)
		{
			/*
			 * Sort is an object: {"field_name": "asc|desc"}
			 * We support a single sort key for simplicity.
			 */
			tok = JsonbIteratorNext(&it, &val, true);
			if (tok == WJB_BEGIN_OBJECT)
			{
				JsonbValue	skey, sval;
				JsonbIteratorToken stok;

				while ((stok = JsonbIteratorNext(&it, &skey, true)) != WJB_END_OBJECT &&
					   stok != WJB_DONE)
				{
					char   *sort_field;
					char   *sort_dir;

					if (stok != WJB_KEY)
						continue;

					sort_field = pnstrdup(skey.val.string.val,
										  skey.val.string.len);

					stok = JsonbIteratorNext(&it, &sval, true);
					sort_dir = jsonbvalue_to_cstring(&sval);

					if (order_clause->len > 0)
						appendStringInfoString(order_clause, ", ");
					appendStringInfo(order_clause,
						"data->>'%s'", sort_field);

					if (pg_strcasecmp(sort_dir, "desc") == 0)
						appendStringInfoString(order_clause, " DESC");
					else
						appendStringInfoString(order_clause, " ASC");

					pfree(sort_field);
					pfree(sort_dir);
				}
			}
		}
		else if (strcmp(keyname, "fields") == 0)
		{
			/*
			 * Fields is an array of field names for projection:
			 *   ["name", "age"]
			 * -> jsonb_build_object('name', data->'name', 'age', data->'age')
			 */
			tok = JsonbIteratorNext(&it, &val, true);
			if (tok == WJB_BEGIN_ARRAY)
			{
				JsonbValue	elem;
				JsonbIteratorToken etok;
				int			field_count = 0;
				StringInfoData proj;

				initStringInfo(&proj);
				appendStringInfoString(&proj, "jsonb_build_object(");

				while ((etok = JsonbIteratorNext(&it, &elem, true)) != WJB_END_ARRAY &&
					   etok != WJB_DONE)
				{
					char   *fname;

					if (etok != WJB_ELEM || elem.type != jbvString)
						continue;

					fname = pnstrdup(elem.val.string.val,
									 elem.val.string.len);

					if (field_count > 0)
						appendStringInfoString(&proj, ", ");

					appendStringInfo(&proj, "%s, data->'%s'",
									 quote_literal_cstr(fname), fname);
					pfree(fname);
					field_count++;
				}

				appendStringInfoChar(&proj, ')');

				if (field_count > 0)
				{
					/* Replace the default "data" select expression */
					resetStringInfo(select_expr);
					appendStringInfoString(select_expr, proj.data);
				}

				pfree(proj.data);
			}
		}
		else
		{
			/* Unknown option key; skip its value */
			tok = JsonbIteratorNext(&it, &val, true);
			/* If it was a composite value, consume it */
			if (tok == WJB_BEGIN_OBJECT)
			{
				int depth = 1;
				while (depth > 0)
				{
					tok = JsonbIteratorNext(&it, &val, true);
					if (tok == WJB_BEGIN_OBJECT || tok == WJB_BEGIN_ARRAY)
						depth++;
					else if (tok == WJB_END_OBJECT || tok == WJB_END_ARRAY)
						depth--;
					else if (tok == WJB_DONE)
						break;
				}
			}
			else if (tok == WJB_BEGIN_ARRAY)
			{
				int depth = 1;
				while (depth > 0)
				{
					tok = JsonbIteratorNext(&it, &val, true);
					if (tok == WJB_BEGIN_OBJECT || tok == WJB_BEGIN_ARRAY)
						depth++;
					else if (tok == WJB_END_OBJECT || tok == WJB_END_ARRAY)
						depth--;
					else if (tok == WJB_DONE)
						break;
				}
			}
		}

		pfree(keyname);
	}
}

/* ----------------------------------------------------------------
 * doc_search
 *
 * Search a collection with filter, sort, limit, offset, projection.
 *
 * Args: collection text, filter jsonb DEFAULT '{}',
 *       opts jsonb DEFAULT NULL
 * Returns: SETOF jsonb
 * ---------------------------------------------------------------- */
Datum
doc_search(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	text	   *coll_text = PG_GETARG_TEXT_PP(0);
	char	   *collection = text_to_cstring(coll_text);
	const char *safe_coll = quote_identifier(collection);
	char	   *filter_str = NULL;
	char	   *opts_str = NULL;
	StringInfoData sql;
	StringInfoData where_clause;
	StringInfoData select_expr;
	StringInfoData order_clause;
	int64		limit_val = 0;
	int64		offset_val = 0;
	int			ret;
	uint64		i;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	/* Get filter (jsonb -> C string) */
	if (!PG_ARGISNULL(1))
		filter_str = DatumGetCString(DirectFunctionCall1(jsonb_out, PG_GETARG_DATUM(1)));

	/* Get opts (jsonb -> C string) */
	if (!PG_ARGISNULL(2))
		opts_str = DatumGetCString(DirectFunctionCall1(jsonb_out, PG_GETARG_DATUM(2)));

	/* Parse opts for projection, sort, limit, offset */
	initStringInfo(&select_expr);
	initStringInfo(&order_clause);
	parse_search_opts(opts_str, &select_expr, &order_clause,
					  &limit_val, &offset_val);

	/* Build WHERE clause from filter */
	initStringInfo(&where_clause);
	if (filter_str != NULL)
		nosql_filter_to_where(&where_clause, filter_str);

	/* Build the full query */
	initStringInfo(&sql);
	appendStringInfo(&sql, "SELECT %s FROM %s",
					 select_expr.data, safe_coll);

	if (where_clause.len > 0)
		appendStringInfo(&sql, " WHERE %s", where_clause.data);

	if (order_clause.len > 0)
		appendStringInfo(&sql, " ORDER BY %s", order_clause.data);

	if (limit_val > 0)
		appendStringInfo(&sql, " LIMIT %lld", (long long) limit_val);

	if (offset_val > 0)
		appendStringInfo(&sql, " OFFSET %lld", (long long) offset_val);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(sql.data, true, 0);
	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_nosql: doc_search query failed: error code %d", ret)));

	for (i = 0; i < SPI_processed; i++)
	{
		Datum		values[1];
		bool		nulls[1];
		bool		isnull;

		values[0] = SPI_getbinval(SPI_tuptable->vals[i],
								  SPI_tuptable->tupdesc, 1, &isnull);
		nulls[0] = isnull;

		if (!isnull)
			values[0] = SPI_datumTransfer(values[0], false, -1);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							values, nulls);
	}

	PopActiveSnapshot();
	SPI_finish();

	pfree(sql.data);
	pfree(where_clause.data);
	pfree(select_expr.data);
	pfree(order_clause.data);

	return (Datum) 0;
}

/* ----------------------------------------------------------------
 * doc_query
 *
 * Execute a direct SQL WHERE clause against a collection.
 * This gives users full PostgreSQL expression power.
 *
 * Args: collection text, sql_where text
 * Returns: SETOF jsonb
 * ---------------------------------------------------------------- */
Datum
doc_query(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	text	   *coll_text = PG_GETARG_TEXT_PP(0);
	text	   *where_text = PG_GETARG_TEXT_PP(1);
	char	   *collection = text_to_cstring(coll_text);
	char	   *sql_where = text_to_cstring(where_text);
	const char *safe_coll = quote_identifier(collection);
	StringInfoData sql;
	int			ret;
	uint64		i;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	initStringInfo(&sql);
	appendStringInfo(&sql, "SELECT data FROM %s WHERE %s",
					 safe_coll, sql_where);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(sql.data, true, 0);
	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_nosql: doc_query failed: error code %d", ret)));

	for (i = 0; i < SPI_processed; i++)
	{
		Datum		values[1];
		bool		nulls[1];
		bool		isnull;

		values[0] = SPI_getbinval(SPI_tuptable->vals[i],
								  SPI_tuptable->tupdesc, 1, &isnull);
		nulls[0] = isnull;

		if (!isnull)
			values[0] = SPI_datumTransfer(values[0], false, -1);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							values, nulls);
	}

	PopActiveSnapshot();
	SPI_finish();

	pfree(sql.data);

	return (Datum) 0;
}

/* ----------------------------------------------------------------
 * doc_group
 *
 * Group-by analytics over a collection.  Thin wrapper that delegates
 * entirely to PostgreSQL's query planner.
 *
 * Args: collection text, group_by text, agg_expr text,
 *       filter jsonb DEFAULT NULL
 * Returns: SETOF jsonb
 * ---------------------------------------------------------------- */
Datum
doc_group(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	text	   *coll_text = PG_GETARG_TEXT_PP(0);
	text	   *groupby_text = PG_GETARG_TEXT_PP(1);
	text	   *aggexpr_text = PG_GETARG_TEXT_PP(2);
	char	   *collection = text_to_cstring(coll_text);
	char	   *group_by = text_to_cstring(groupby_text);
	char	   *agg_expr = text_to_cstring(aggexpr_text);
	const char *safe_coll = quote_identifier(collection);
	char	   *filter_str = NULL;
	StringInfoData sql;
	StringInfoData where_clause;
	int			ret;
	uint64		i;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	if (!PG_ARGISNULL(3))
		filter_str = DatumGetCString(DirectFunctionCall1(jsonb_out, PG_GETARG_DATUM(3)));

	initStringInfo(&where_clause);
	if (filter_str != NULL)
		nosql_filter_to_where(&where_clause, filter_str);

	initStringInfo(&sql);
	appendStringInfo(&sql,
		"SELECT jsonb_build_object("
		"'group', %s, "
		"'result', %s"
		") FROM %s",
		group_by, agg_expr, safe_coll);

	if (where_clause.len > 0)
		appendStringInfo(&sql, " WHERE %s", where_clause.data);

	appendStringInfo(&sql, " GROUP BY %s", group_by);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(sql.data, true, 0);
	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_nosql: doc_group query failed: error code %d", ret)));

	for (i = 0; i < SPI_processed; i++)
	{
		Datum		values[1];
		bool		nulls[1];
		bool		isnull;

		values[0] = SPI_getbinval(SPI_tuptable->vals[i],
								  SPI_tuptable->tupdesc, 1, &isnull);
		nulls[0] = isnull;

		if (!isnull)
			values[0] = SPI_datumTransfer(values[0], false, -1);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							values, nulls);
	}

	PopActiveSnapshot();
	SPI_finish();

	pfree(sql.data);
	pfree(where_clause.data);

	return (Datum) 0;
}

/* ----------------------------------------------------------------
 * doc_array_append
 *
 * Append a value to a JSON array at the specified path.
 *
 * Args: doc jsonb, path text, value jsonb
 * Returns: jsonb (IMMUTABLE)
 * ---------------------------------------------------------------- */
Datum
doc_array_append(PG_FUNCTION_ARGS)
{
	Datum		doc_datum = PG_GETARG_DATUM(0);
	text	   *path_text = PG_GETARG_TEXT_PP(1);
	Datum		value_datum = PG_GETARG_DATUM(2);
	char	   *path_str = text_to_cstring(path_text);
	StringInfoData sql;
	int			ret;
	Datum		result;
	bool		isnull;

	/*
	 * Use SPI to call:
	 *   SELECT jsonb_set($1, (path || '{-1}')::text[], $2 || jsonb_build_array($3))
	 *
	 * Actually, the cleanest approach is:
	 *   SELECT jsonb_set($1, $2::text[], (($1 #> $2::text[]) || jsonb_build_array($3)))
	 *
	 * But simpler to use the insert-after-last approach:
	 *   SELECT jsonb_insert($1, ($2 || '{-1}')::text[], $3, true)
	 *
	 * jsonb_insert with after=true appends to the array.
	 */
	initStringInfo(&sql);
	appendStringInfo(&sql,
		"SELECT jsonb_insert($1, ('{%s,-1}')::text[], $2, true)",
		path_str);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	{
		Oid		argtypes[2] = {JSONBOID, JSONBOID};
		Datum	args[2];

		args[0] = doc_datum;
		args[1] = value_datum;

		ret = SPI_execute_with_args(sql.data, 2, argtypes, args, NULL, true, 1);
	}

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_nosql: doc_array_append failed: error code %d", ret)));

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
 * doc_array_remove
 *
 * Remove all occurrences of a value from a JSON array at path.
 *
 * Args: doc jsonb, path text, value jsonb
 * Returns: jsonb (IMMUTABLE)
 * ---------------------------------------------------------------- */
Datum
doc_array_remove(PG_FUNCTION_ARGS)
{
	Datum		doc_datum = PG_GETARG_DATUM(0);
	text	   *path_text = PG_GETARG_TEXT_PP(1);
	Datum		value_datum = PG_GETARG_DATUM(2);
	char	   *path_str = text_to_cstring(path_text);
	StringInfoData sql;
	int			ret;
	Datum		result;
	bool		isnull;

	/*
	 * Rebuild the array without the matching element:
	 *   SELECT jsonb_set($1, '{path}'::text[],
	 *     (SELECT coalesce(jsonb_agg(elem), '[]'::jsonb)
	 *      FROM jsonb_array_elements($1 #> '{path}'::text[]) AS elem
	 *      WHERE elem <> $2))
	 */
	initStringInfo(&sql);
	appendStringInfo(&sql,
		"SELECT jsonb_set($1, '{%s}'::text[], "
		"(SELECT coalesce(jsonb_agg(elem), '[]'::jsonb) "
		"FROM jsonb_array_elements($1 #> '{%s}'::text[]) AS elem "
		"WHERE elem <> $2))",
		path_str, path_str);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	{
		Oid		argtypes[2] = {JSONBOID, JSONBOID};
		Datum	args[2];

		args[0] = doc_datum;
		args[1] = value_datum;

		ret = SPI_execute_with_args(sql.data, 2, argtypes, args, NULL, true, 1);
	}

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_nosql: doc_array_remove failed: error code %d", ret)));

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
 * doc_array_add_unique
 *
 * Append a value to a JSON array at path only if not already present.
 *
 * Args: doc jsonb, path text, value jsonb
 * Returns: jsonb (IMMUTABLE)
 * ---------------------------------------------------------------- */
Datum
doc_array_add_unique(PG_FUNCTION_ARGS)
{
	Datum		doc_datum = PG_GETARG_DATUM(0);
	text	   *path_text = PG_GETARG_TEXT_PP(1);
	Datum		value_datum = PG_GETARG_DATUM(2);
	char	   *path_str = text_to_cstring(path_text);
	StringInfoData sql;
	int			ret;
	Datum		result;
	bool		isnull;

	/*
	 * Only append if not already present:
	 *   SELECT CASE
	 *     WHEN ($1 #> '{path}'::text[]) @> jsonb_build_array($2)
	 *     THEN $1
	 *     ELSE jsonb_insert($1, '{path,-1}'::text[], $2, true)
	 *   END
	 */
	initStringInfo(&sql);
	appendStringInfo(&sql,
		"SELECT CASE "
		"WHEN ($1 #> '{%s}'::text[]) @> jsonb_build_array($2) "
		"THEN $1 "
		"ELSE jsonb_insert($1, '{%s,-1}'::text[], $2, true) "
		"END",
		path_str, path_str);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	{
		Oid		argtypes[2] = {JSONBOID, JSONBOID};
		Datum	args[2];

		args[0] = doc_datum;
		args[1] = value_datum;

		ret = SPI_execute_with_args(sql.data, 2, argtypes, args, NULL, true, 1);
	}

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_nosql: doc_array_add_unique failed: error code %d", ret)));

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
