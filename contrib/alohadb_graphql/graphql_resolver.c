/*-------------------------------------------------------------------------
 *
 * graphql_resolver.c
 *	  Resolves parsed GraphQL AST into SQL queries executed via SPI.
 *
 *	  Provides three SQL-callable functions:
 *	    - graphql_execute(query text, variables jsonb, operation_name text)
 *	      Parses the query, translates to SQL, executes, returns jsonb.
 *	    - graphql_schema() -> text
 *	      Auto-generates a GraphQL SDL schema from information_schema.
 *	    - graphql_schema_json() -> jsonb
 *	      Same but as a jsonb document.
 *
 *	  Schema introspection queries information_schema.tables and
 *	  information_schema.columns to discover the database schema.
 *	  Foreign key relationships are discovered from
 *	  information_schema.key_column_usage and
 *	  information_schema.referential_constraints.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_graphql/graphql_resolver.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"

#include "alohadb_graphql.h"

/* PG_FUNCTION_INFO_V1 must be in same file as function implementation */
PG_FUNCTION_INFO_V1(graphql_execute);
PG_FUNCTION_INFO_V1(graphql_schema);
PG_FUNCTION_INFO_V1(graphql_schema_json);

/* ----------------------------------------------------------------
 * Internal function prototypes
 * ---------------------------------------------------------------- */

static char *resolve_query(GqlNode *root);
static char *resolve_query_field(GqlNode *field, const char *parent_table,
								 const char *parent_alias);
static char *resolve_mutation(GqlNode *root);
static char *resolve_insert(GqlNode *field);
static char *resolve_delete(GqlNode *field);

static void append_where_clause(StringInfo sql, const char *table_alias,
								const char *where_json);
static void append_order_by_clause(StringInfo sql, const char *table_alias,
									const char *order_json);

static char *get_fk_column(const char *from_table, const char *to_table);

static char *build_sdl_schema(void);
static char *build_json_schema(void);
static char *gql_type_for_pg_type(const char *pg_type);

static GqlNode *find_argument(GqlNode *args, const char *name);

/* ----------------------------------------------------------------
 * Helper: find an argument node by name
 * ---------------------------------------------------------------- */
static GqlNode *
find_argument(GqlNode *args, const char *name)
{
	GqlNode *arg;

	for (arg = args; arg != NULL; arg = arg->next)
	{
		if (arg->name && strcmp(arg->name, name) == 0)
			return arg;
	}
	return NULL;
}

/* ----------------------------------------------------------------
 * Resolve a query operation into a JSON result string.
 *
 * This function must be called within an active SPI connection.
 * Returns a JSON string like {"data": {"table": [...]}}
 * ---------------------------------------------------------------- */
static char *
resolve_query(GqlNode *root)
{
	StringInfoData result;
	GqlNode *field;
	bool		first = true;

	initStringInfo(&result);
	appendStringInfoString(&result, "{\"data\": {");

	for (field = root->children; field != NULL; field = field->next)
	{
		char   *field_json;

		if (!first)
			appendStringInfoString(&result, ", ");
		first = false;

		field_json = resolve_query_field(field, NULL, NULL);
		appendStringInfo(&result, "\"%s\": %s", field->name, field_json);
		pfree(field_json);
	}

	appendStringInfoString(&result, "}}");
	return result.data;
}

/* ----------------------------------------------------------------
 * resolve_query_field
 *
 * Resolve a single field (table query) into a JSON array string.
 * Handles WHERE, LIMIT, OFFSET, ORDER BY from arguments.
 * Handles nested object fields via FK lookup.
 *
 * If parent_table/parent_alias are non-NULL, this is a nested
 * sub-query that should be correlated via FK.
 * ---------------------------------------------------------------- */
static char *
resolve_query_field(GqlNode *field, const char *parent_table,
					const char *parent_alias)
{
	StringInfoData sql;
	StringInfoData result;
	GqlNode    *child;
	GqlNode    *where_arg;
	GqlNode    *limit_arg;
	GqlNode    *offset_arg;
	GqlNode    *order_arg;
	const char *table_name;
	char		alias[GQL_MAX_NAME_LEN + 4];
	int			ret;
	uint64		nrows;
	uint64		i;
	bool		first_row;

	/*
	 * Separate child fields into scalar columns and nested objects.
	 * Nested objects are fields that themselves have children (a selection
	 * set), meaning they reference a related table.
	 */
	table_name = field->name;
	snprintf(alias, sizeof(alias), "t_%s", table_name);

	/* Collect arguments */
	where_arg = find_argument(field->args, "where");
	limit_arg = find_argument(field->args, "limit");
	offset_arg = find_argument(field->args, "offset");
	order_arg = find_argument(field->args, "order_by");

	/*
	 * Build SELECT query.  First pass: scalar columns.
	 */
	initStringInfo(&sql);
	appendStringInfo(&sql, "SELECT row_to_json(%s) FROM (SELECT ",
					 alias);

	{
		bool	first_col = true;

		for (child = field->children; child != NULL; child = child->next)
		{
			/* Only include scalar fields (no children = leaf column) */
			if (child->children == NULL)
			{
				if (!first_col)
					appendStringInfoString(&sql, ", ");
				first_col = false;

				appendStringInfo(&sql, "%s.%s",
								 quote_identifier(alias),
								 quote_identifier(child->name));
			}
		}

		/*
		 * If no scalar columns were selected, default to selecting all
		 * scalar columns with '*'.  This can happen if only nested fields
		 * were requested.
		 */
		if (first_col)
			appendStringInfo(&sql, "%s.*", quote_identifier(alias));
	}

	appendStringInfo(&sql, " FROM %s AS %s",
					 quote_identifier(table_name),
					 quote_identifier(alias));

	/*
	 * If this is a nested field, add a JOIN condition on the FK.
	 */
	if (parent_table != NULL && parent_alias != NULL)
	{
		char   *fk_col;

		fk_col = get_fk_column(table_name, parent_table);
		if (fk_col != NULL)
		{
			/*
			 * The child table references the parent table.
			 * e.g. orders.user_id = users.id
			 */
			appendStringInfo(&sql, " WHERE %s.%s = %s.id",
							 quote_identifier(alias),
							 quote_identifier(fk_col),
							 quote_identifier(parent_alias));
			pfree(fk_col);
		}
		else
		{
			/*
			 * Try the reverse: parent references child.
			 * e.g. users.department_id = departments.id
			 */
			fk_col = get_fk_column(parent_table, table_name);
			if (fk_col != NULL)
			{
				appendStringInfo(&sql, " WHERE %s.id = %s.%s",
								 quote_identifier(alias),
								 quote_identifier(parent_alias),
								 quote_identifier(fk_col));
				pfree(fk_col);
			}
		}
	}

	/* WHERE clause from argument */
	if (where_arg && where_arg->value)
	{
		if (parent_table != NULL)
			appendStringInfoString(&sql, " AND ");
		else
			appendStringInfoString(&sql, " WHERE ");

		/*
		 * Strip the outer AND/WHERE prefix decision from the helper;
		 * append_where_clause appends conditions only.
		 */
		append_where_clause(&sql, alias, where_arg->value);
	}

	/* ORDER BY */
	if (order_arg && order_arg->value)
		append_order_by_clause(&sql, alias, order_arg->value);

	/* LIMIT */
	if (limit_arg && limit_arg->value)
		appendStringInfo(&sql, " LIMIT %s", limit_arg->value);

	/* OFFSET */
	if (offset_arg && offset_arg->value)
		appendStringInfo(&sql, " OFFSET %s", offset_arg->value);

	appendStringInfo(&sql, ") AS %s", quote_identifier(alias));

	/* Execute the query */
	ret = SPI_execute(sql.data, true, 0);

	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("GraphQL: failed to execute query for field \"%s\": "
						"SPI error %d",
						field->name, ret)));

	/* Save row count before any nested queries overwrite SPI state */
	nrows = SPI_processed;

	/*
	 * Build result JSON array.  For each row, we get the row_to_json
	 * text value and optionally append nested object fields.
	 */
	initStringInfo(&result);
	appendStringInfoChar(&result, '[');

	if (nrows > 0)
	{
		/*
		 * Copy all row JSON strings out of SPI context before we run
		 * any nested sub-queries.
		 */
		char	  **row_jsons;
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(CurTransactionContext);
		row_jsons = palloc(sizeof(char *) * nrows);
		for (i = 0; i < nrows; i++)
		{
			char   *val;

			val = SPI_getvalue(SPI_tuptable->vals[i],
							   SPI_tuptable->tupdesc, 1);
			row_jsons[i] = pstrdup(val ? val : "{}");
		}
		MemoryContextSwitchTo(oldcxt);

		/*
		 * Check whether we have any nested object fields that need
		 * sub-queries.
		 */
		first_row = true;
		for (i = 0; i < nrows; i++)
		{
			bool	has_nested = false;

			for (child = field->children; child != NULL; child = child->next)
			{
				if (child->children != NULL)
				{
					has_nested = true;
					break;
				}
			}

			if (!first_row)
				appendStringInfoString(&result, ", ");
			first_row = false;

			if (!has_nested)
			{
				/* Simple case: just output the row JSON */
				appendStringInfoString(&result, row_jsons[i]);
			}
			else
			{
				/*
				 * For nested fields, we strip the closing '}' from the
				 * row JSON and append the nested arrays.
				 *
				 * Note: for nested sub-queries we would need to
				 * correlate on FK.  For simplicity we execute a
				 * separate sub-query per nested field.  In a production
				 * system this should use JOINs or lateral subqueries.
				 */
				int		rlen = strlen(row_jsons[i]);

				if (rlen > 0 && row_jsons[i][rlen - 1] == '}')
					row_jsons[i][rlen - 1] = '\0';

				appendStringInfoString(&result, row_jsons[i]);

				for (child = field->children; child != NULL; child = child->next)
				{
					if (child->children != NULL)
					{
						char   *nested_json;

						nested_json = resolve_query_field(child,
														  table_name,
														  alias);
						appendStringInfo(&result, ", \"%s\": %s",
										 child->name, nested_json);
						pfree(nested_json);
					}
				}

				appendStringInfoChar(&result, '}');
			}
		}
	}

	appendStringInfoChar(&result, ']');
	pfree(sql.data);

	return result.data;
}

/* ----------------------------------------------------------------
 * append_where_clause
 *
 * Parse a JSON-ish where argument like {"col": "val", "col2": "val2"}
 * and append SQL conditions.  Each key becomes col = 'val' joined
 * by AND.  This does NOT append "WHERE" -- the caller handles that.
 * ---------------------------------------------------------------- */
static void
append_where_clause(StringInfo sql, const char *table_alias,
					const char *where_json)
{
	const char *p;
	bool		first = true;
	char		key[GQL_MAX_NAME_LEN];
	char		val[GQL_MAX_VALUE_LEN];
	int			ki, vi;

	if (where_json == NULL)
		return;

	p = where_json;

	/* Skip leading whitespace and '{' */
	while (*p && (*p == ' ' || *p == '{'))
		p++;

	while (*p && *p != '}')
	{
		/* Skip whitespace and commas */
		while (*p && (*p == ' ' || *p == ',' || *p == '\t' || *p == '\n'))
			p++;

		if (*p == '}' || *p == '\0')
			break;

		/* Read key (may or may not be quoted) */
		ki = 0;
		if (*p == '"')
		{
			p++;	/* skip quote */
			while (*p && *p != '"' && ki < GQL_MAX_NAME_LEN - 1)
				key[ki++] = *p++;
			if (*p == '"')
				p++;
		}
		else
		{
			while (*p && *p != ':' && *p != ' ' && ki < GQL_MAX_NAME_LEN - 1)
				key[ki++] = *p++;
		}
		key[ki] = '\0';

		/* Skip whitespace and colon */
		while (*p && (*p == ' ' || *p == ':'))
			p++;

		/* Read value */
		vi = 0;
		if (*p == '"')
		{
			p++;	/* skip quote */
			while (*p && *p != '"' && vi < GQL_MAX_VALUE_LEN - 1)
				val[vi++] = *p++;
			if (*p == '"')
				p++;

			val[vi] = '\0';

			/* String comparison with proper escaping */
			if (!first)
				appendStringInfoString(sql, " AND ");
			first = false;

			appendStringInfo(sql, "%s.%s = %s",
							 quote_identifier(table_alias),
							 quote_identifier(key),
							 quote_literal_cstr(val));
		}
		else if (*p == 'n' && strncmp(p, "null", 4) == 0)
		{
			p += 4;

			if (!first)
				appendStringInfoString(sql, " AND ");
			first = false;

			appendStringInfo(sql, "%s.%s IS NULL",
							 quote_identifier(table_alias),
							 quote_identifier(key));
		}
		else
		{
			/* Numeric or boolean value */
			while (*p && *p != ',' && *p != '}' && *p != ' ' &&
				   vi < GQL_MAX_VALUE_LEN - 1)
				val[vi++] = *p++;
			val[vi] = '\0';

			if (!first)
				appendStringInfoString(sql, " AND ");
			first = false;

			appendStringInfo(sql, "%s.%s = %s",
							 quote_identifier(table_alias),
							 quote_identifier(key),
							 quote_literal_cstr(val));
		}

		/* Skip trailing whitespace and commas */
		while (*p && (*p == ' ' || *p == ','))
			p++;
	}
}

/* ----------------------------------------------------------------
 * append_order_by_clause
 *
 * Parse a JSON-ish order_by argument like {"col": "asc"} or
 * {"col": "desc"} and append ORDER BY clause.
 * ---------------------------------------------------------------- */
static void
append_order_by_clause(StringInfo sql, const char *table_alias,
					   const char *order_json)
{
	const char *p;
	bool		first = true;
	char		key[GQL_MAX_NAME_LEN];
	char		dir[16];
	int			ki, di;

	if (order_json == NULL)
		return;

	p = order_json;

	/* Skip leading whitespace and '{' */
	while (*p && (*p == ' ' || *p == '{'))
		p++;

	appendStringInfoString(sql, " ORDER BY ");

	while (*p && *p != '}')
	{
		/* Skip whitespace and commas */
		while (*p && (*p == ' ' || *p == ',' || *p == '\t' || *p == '\n'))
			p++;

		if (*p == '}' || *p == '\0')
			break;

		/* Read key */
		ki = 0;
		if (*p == '"')
		{
			p++;
			while (*p && *p != '"' && ki < GQL_MAX_NAME_LEN - 1)
				key[ki++] = *p++;
			if (*p == '"')
				p++;
		}
		else
		{
			while (*p && *p != ':' && *p != ' ' && ki < GQL_MAX_NAME_LEN - 1)
				key[ki++] = *p++;
		}
		key[ki] = '\0';

		/* Skip colon */
		while (*p && (*p == ' ' || *p == ':'))
			p++;

		/* Read direction */
		di = 0;
		if (*p == '"')
		{
			p++;
			while (*p && *p != '"' && di < 15)
				dir[di++] = *p++;
			if (*p == '"')
				p++;
		}
		else
		{
			while (*p && *p != ',' && *p != '}' && *p != ' ' && di < 15)
				dir[di++] = *p++;
		}
		dir[di] = '\0';

		if (!first)
			appendStringInfoString(sql, ", ");
		first = false;

		appendStringInfo(sql, "%s.%s",
						 quote_identifier(table_alias),
						 quote_identifier(key));

		/* Validate direction */
		if (pg_strcasecmp(dir, "desc") == 0)
			appendStringInfoString(sql, " DESC");
		else
			appendStringInfoString(sql, " ASC");

		/* Skip trailing whitespace and commas */
		while (*p && (*p == ' ' || *p == ','))
			p++;
	}
}

/* ----------------------------------------------------------------
 * get_fk_column
 *
 * Look up a foreign key from from_table that references to_table.
 * Returns the column name in from_table, or NULL if none found.
 *
 * Uses SPI to query information_schema.  Must be called within
 * an active SPI connection.
 * ---------------------------------------------------------------- */
static char *
get_fk_column(const char *from_table, const char *to_table)
{
	StringInfoData sql;
	int			ret;
	char	   *result = NULL;

	initStringInfo(&sql);
	appendStringInfo(&sql,
		"SELECT kcu.column_name "
		"FROM information_schema.key_column_usage kcu "
		"JOIN information_schema.referential_constraints rc "
		"  ON rc.constraint_name = kcu.constraint_name "
		"  AND rc.constraint_schema = kcu.constraint_schema "
		"JOIN information_schema.table_constraints tc "
		"  ON tc.constraint_name = rc.unique_constraint_name "
		"  AND tc.constraint_schema = rc.unique_constraint_schema "
		"WHERE kcu.table_name = %s "
		"  AND tc.table_name = %s "
		"LIMIT 1",
		quote_literal_cstr(from_table),
		quote_literal_cstr(to_table));

	ret = SPI_execute(sql.data, true, 1);

	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		char   *val;

		val = SPI_getvalue(SPI_tuptable->vals[0],
						   SPI_tuptable->tupdesc, 1);
		if (val)
			result = pstrdup(val);
	}

	pfree(sql.data);
	return result;
}

/* ----------------------------------------------------------------
 * resolve_mutation
 *
 * Resolve a mutation operation.  Supports:
 *   insert_TABLENAME(objects: [{...}, ...]) { returning_fields }
 *   delete_TABLENAME(where: {...}) { returning_fields }
 * ---------------------------------------------------------------- */
static char *
resolve_mutation(GqlNode *root)
{
	StringInfoData result;
	GqlNode *field;
	bool		first = true;

	initStringInfo(&result);
	appendStringInfoString(&result, "{\"data\": {");

	for (field = root->children; field != NULL; field = field->next)
	{
		char   *field_json;

		if (!first)
			appendStringInfoString(&result, ", ");
		first = false;

		if (strncmp(field->name, "insert_", 7) == 0)
			field_json = resolve_insert(field);
		else if (strncmp(field->name, "delete_", 7) == 0)
			field_json = resolve_delete(field);
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("GraphQL: unsupported mutation \"%s\"; "
							"expected insert_TABLE or delete_TABLE",
							field->name)));

		appendStringInfo(&result, "\"%s\": %s", field->name, field_json);
		pfree(field_json);
	}

	appendStringInfoString(&result, "}}");
	return result.data;
}

/* ----------------------------------------------------------------
 * resolve_insert
 *
 * Handle insert_TABLENAME(objects: [{col: "val", ...}, ...]) { fields }
 * ---------------------------------------------------------------- */
static char *
resolve_insert(GqlNode *field)
{
	const char *table_name;
	GqlNode    *objects_arg;
	GqlNode    *child;
	StringInfoData sql;
	StringInfoData result;
	int			ret;
	uint64		nrows;
	uint64		i;
	bool		first_col;

	table_name = field->name + 7;	/* skip "insert_" */

	objects_arg = find_argument(field->args, "objects");
	if (!objects_arg || !objects_arg->value)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("GraphQL: insert_%s requires an 'objects' argument",
						table_name)));

	/*
	 * For simplicity, we convert the objects array to a jsonb value and
	 * use jsonb_to_recordset to INSERT.  This approach handles arbitrary
	 * columns without needing to parse the JSON ourselves.
	 *
	 * Strategy: INSERT INTO table SELECT * FROM jsonb_populate_recordset(...)
	 *
	 * Actually, for maximum simplicity and reliability, we use:
	 *   INSERT INTO table
	 *   SELECT * FROM jsonb_populate_recordset(null::table, objects_jsonb)
	 *   RETURNING selected_fields
	 */
	initStringInfo(&sql);
	appendStringInfo(&sql,
		"INSERT INTO %s SELECT * FROM jsonb_populate_recordset(null::%s, %s::jsonb) RETURNING ",
		quote_identifier(table_name),
		quote_identifier(table_name),
		quote_literal_cstr(objects_arg->value));

	/* Build RETURNING clause from child fields */
	first_col = true;
	for (child = field->children; child != NULL; child = child->next)
	{
		if (!first_col)
			appendStringInfoString(&sql, ", ");
		first_col = false;
		appendStringInfoString(&sql, quote_identifier(child->name));
	}

	if (first_col)
		appendStringInfoChar(&sql, '*');

	ret = SPI_execute(sql.data, false, 0);

	if (ret != SPI_OK_INSERT_RETURNING)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("GraphQL: insert_%s failed: SPI error %d",
						table_name, ret)));

	nrows = SPI_processed;

	/* Build result JSON */
	initStringInfo(&result);
	appendStringInfoString(&result, "{\"returning\": [");

	for (i = 0; i < nrows; i++)
	{
		bool	first_field = true;

		if (i > 0)
			appendStringInfoString(&result, ", ");

		appendStringInfoChar(&result, '{');

		for (child = field->children; child != NULL; child = child->next)
		{
			int		colno;
			char   *val;

			colno = SPI_fnumber(SPI_tuptable->tupdesc, child->name);
			if (colno == SPI_ERROR_NOATTRIBUTE)
				continue;

			val = SPI_getvalue(SPI_tuptable->vals[i],
							   SPI_tuptable->tupdesc, colno);

			if (!first_field)
				appendStringInfoString(&result, ", ");
			first_field = false;

			if (val == NULL)
				appendStringInfo(&result, "\"%s\": null", child->name);
			else
			{
				/*
				 * Try to detect numeric values to output them unquoted.
				 * Otherwise output as a JSON string.
				 */
				Oid		typoid = SPI_gettypeid(SPI_tuptable->tupdesc, colno);

				if (typoid == INT2OID || typoid == INT4OID ||
					typoid == INT8OID || typoid == FLOAT4OID ||
					typoid == FLOAT8OID || typoid == NUMERICOID ||
					typoid == BOOLOID)
				{
					appendStringInfo(&result, "\"%s\": %s",
									 child->name, val);
				}
				else
				{
					appendStringInfo(&result, "\"%s\": \"%s\"",
									 child->name, val);
				}
			}
		}

		appendStringInfoChar(&result, '}');
	}

	appendStringInfoString(&result, "]}");
	pfree(sql.data);

	return result.data;
}

/* ----------------------------------------------------------------
 * resolve_delete
 *
 * Handle delete_TABLENAME(where: {col: "val"}) { fields }
 * ---------------------------------------------------------------- */
static char *
resolve_delete(GqlNode *field)
{
	const char *table_name;
	GqlNode    *where_arg;
	GqlNode    *child;
	StringInfoData sql;
	StringInfoData result;
	int			ret;
	uint64		nrows;
	uint64		i;
	bool		first_col;

	table_name = field->name + 7;	/* skip "delete_" */

	where_arg = find_argument(field->args, "where");
	if (!where_arg || !where_arg->value)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("GraphQL: delete_%s requires a 'where' argument",
						table_name)));

	initStringInfo(&sql);
	appendStringInfo(&sql, "DELETE FROM %s AS t_del WHERE ",
					 quote_identifier(table_name));

	append_where_clause(&sql, "t_del", where_arg->value);

	/* RETURNING clause */
	appendStringInfoString(&sql, " RETURNING ");
	first_col = true;
	for (child = field->children; child != NULL; child = child->next)
	{
		if (!first_col)
			appendStringInfoString(&sql, ", ");
		first_col = false;
		appendStringInfoString(&sql, quote_identifier(child->name));
	}

	if (first_col)
		appendStringInfoChar(&sql, '*');

	ret = SPI_execute(sql.data, false, 0);

	if (ret != SPI_OK_DELETE_RETURNING)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("GraphQL: delete_%s failed: SPI error %d",
						table_name, ret)));

	nrows = SPI_processed;

	/* Build result JSON */
	initStringInfo(&result);
	appendStringInfoString(&result, "{\"returning\": [");

	for (i = 0; i < nrows; i++)
	{
		bool	first_field = true;

		if (i > 0)
			appendStringInfoString(&result, ", ");

		appendStringInfoChar(&result, '{');

		for (child = field->children; child != NULL; child = child->next)
		{
			int		colno;
			char   *val;

			colno = SPI_fnumber(SPI_tuptable->tupdesc, child->name);
			if (colno == SPI_ERROR_NOATTRIBUTE)
				continue;

			val = SPI_getvalue(SPI_tuptable->vals[i],
							   SPI_tuptable->tupdesc, colno);

			if (!first_field)
				appendStringInfoString(&result, ", ");
			first_field = false;

			if (val == NULL)
				appendStringInfo(&result, "\"%s\": null", child->name);
			else
			{
				Oid		typoid = SPI_gettypeid(SPI_tuptable->tupdesc, colno);

				if (typoid == INT2OID || typoid == INT4OID ||
					typoid == INT8OID || typoid == FLOAT4OID ||
					typoid == FLOAT8OID || typoid == NUMERICOID ||
					typoid == BOOLOID)
				{
					appendStringInfo(&result, "\"%s\": %s",
									 child->name, val);
				}
				else
				{
					appendStringInfo(&result, "\"%s\": \"%s\"",
									 child->name, val);
				}
			}
		}

		appendStringInfoChar(&result, '}');
	}

	appendStringInfoString(&result, "]}");
	pfree(sql.data);

	return result.data;
}

/* ----------------------------------------------------------------
 * gql_type_for_pg_type
 *
 * Map a PostgreSQL data type name to a GraphQL type name.
 * ---------------------------------------------------------------- */
static char *
gql_type_for_pg_type(const char *pg_type)
{
	if (strcmp(pg_type, "integer") == 0 || strcmp(pg_type, "int4") == 0 ||
		strcmp(pg_type, "smallint") == 0 || strcmp(pg_type, "int2") == 0 ||
		strcmp(pg_type, "bigint") == 0 || strcmp(pg_type, "int8") == 0 ||
		strcmp(pg_type, "serial") == 0 || strcmp(pg_type, "bigserial") == 0)
		return "Int";

	if (strcmp(pg_type, "real") == 0 || strcmp(pg_type, "float4") == 0 ||
		strcmp(pg_type, "double precision") == 0 || strcmp(pg_type, "float8") == 0 ||
		strcmp(pg_type, "numeric") == 0 || strcmp(pg_type, "decimal") == 0)
		return "Float";

	if (strcmp(pg_type, "boolean") == 0 || strcmp(pg_type, "bool") == 0)
		return "Boolean";

	if (strcmp(pg_type, "json") == 0 || strcmp(pg_type, "jsonb") == 0)
		return "JSON";

	/* Everything else maps to String */
	return "String";
}

/* ----------------------------------------------------------------
 * build_sdl_schema
 *
 * Query information_schema and auto-generate a GraphQL SDL schema.
 * Returns a palloc'd string.
 * ---------------------------------------------------------------- */
static char *
build_sdl_schema(void)
{
	StringInfoData schema;
	int			ret;
	uint64		ntables;
	uint64		t;

	initStringInfo(&schema);

	/* Query all user tables in public schema */
	ret = SPI_execute(
		"SELECT table_name FROM information_schema.tables "
		"WHERE table_schema = 'public' AND table_type = 'BASE TABLE' "
		"ORDER BY table_name",
		true, 0);

	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("GraphQL: failed to query information_schema.tables")));

	ntables = SPI_processed;

	if (ntables == 0)
	{
		appendStringInfoString(&schema, "# No tables found in public schema\n");
		return schema.data;
	}

	/*
	 * Copy table names out of SPI before running per-table column queries.
	 */
	{
		char	  **table_names;
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(CurTransactionContext);
		table_names = palloc(sizeof(char *) * ntables);
		for (t = 0; t < ntables; t++)
		{
			char   *val = SPI_getvalue(SPI_tuptable->vals[t],
									   SPI_tuptable->tupdesc, 1);
			table_names[t] = pstrdup(val ? val : "");
		}
		MemoryContextSwitchTo(oldcxt);

		/* Generate type definitions */
		for (t = 0; t < ntables; t++)
		{
			StringInfoData colsql;
			int			colret;
			uint64		ncols;
			uint64		c;

			appendStringInfo(&schema, "type %s {\n", table_names[t]);

			initStringInfo(&colsql);
			appendStringInfo(&colsql,
				"SELECT column_name, data_type, is_nullable "
				"FROM information_schema.columns "
				"WHERE table_schema = 'public' AND table_name = %s "
				"ORDER BY ordinal_position",
				quote_literal_cstr(table_names[t]));

			colret = SPI_execute(colsql.data, true, 0);
			pfree(colsql.data);

			if (colret != SPI_OK_SELECT)
			{
				appendStringInfoString(&schema, "  # error reading columns\n");
				appendStringInfoString(&schema, "}\n\n");
				continue;
			}

			ncols = SPI_processed;
			for (c = 0; c < ncols; c++)
			{
				char   *col_name;
				char   *data_type;
				char   *nullable;
				char   *gql_type;

				col_name = SPI_getvalue(SPI_tuptable->vals[c],
										SPI_tuptable->tupdesc, 1);
				data_type = SPI_getvalue(SPI_tuptable->vals[c],
										 SPI_tuptable->tupdesc, 2);
				nullable = SPI_getvalue(SPI_tuptable->vals[c],
										SPI_tuptable->tupdesc, 3);

				gql_type = gql_type_for_pg_type(data_type ? data_type : "text");

				appendStringInfo(&schema, "  %s: %s", col_name, gql_type);

				if (nullable && strcmp(nullable, "NO") == 0)
					appendStringInfoChar(&schema, '!');

				appendStringInfoChar(&schema, '\n');
			}

			appendStringInfoString(&schema, "}\n\n");
		}

		/* Generate Query type */
		appendStringInfoString(&schema, "type Query {\n");
		for (t = 0; t < ntables; t++)
		{
			appendStringInfo(&schema,
				"  %s(where: %sWhereInput, limit: Int, offset: Int, "
				"order_by: %sOrderBy): [%s!]!\n",
				table_names[t], table_names[t],
				table_names[t], table_names[t]);
		}
		appendStringInfoString(&schema, "}\n\n");

		/* Generate Mutation type */
		appendStringInfoString(&schema, "type Mutation {\n");
		for (t = 0; t < ntables; t++)
		{
			appendStringInfo(&schema,
				"  insert_%s(objects: [%sInput!]!): %sMutationResponse!\n",
				table_names[t], table_names[t], table_names[t]);
			appendStringInfo(&schema,
				"  delete_%s(where: %sWhereInput!): %sMutationResponse!\n",
				table_names[t], table_names[t], table_names[t]);
		}
		appendStringInfoString(&schema, "}\n");
	}

	return schema.data;
}

/* ----------------------------------------------------------------
 * build_json_schema
 *
 * Same as build_sdl_schema but returns a JSON representation.
 * ---------------------------------------------------------------- */
static char *
build_json_schema(void)
{
	StringInfoData schema;
	int			ret;
	uint64		ntables;
	uint64		t;
	bool		first_table;

	initStringInfo(&schema);
	appendStringInfoString(&schema, "{\"types\": [");

	ret = SPI_execute(
		"SELECT table_name FROM information_schema.tables "
		"WHERE table_schema = 'public' AND table_type = 'BASE TABLE' "
		"ORDER BY table_name",
		true, 0);

	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("GraphQL: failed to query information_schema.tables")));

	ntables = SPI_processed;

	{
		char	  **table_names;
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(CurTransactionContext);
		table_names = palloc(sizeof(char *) * ntables);
		for (t = 0; t < ntables; t++)
		{
			char   *val = SPI_getvalue(SPI_tuptable->vals[t],
									   SPI_tuptable->tupdesc, 1);
			table_names[t] = pstrdup(val ? val : "");
		}
		MemoryContextSwitchTo(oldcxt);

		first_table = true;
		for (t = 0; t < ntables; t++)
		{
			StringInfoData colsql;
			int			colret;
			uint64		ncols;
			uint64		c;
			bool		first_col;

			if (!first_table)
				appendStringInfoString(&schema, ", ");
			first_table = false;

			appendStringInfo(&schema, "{\"name\": \"%s\", \"fields\": [",
							 table_names[t]);

			initStringInfo(&colsql);
			appendStringInfo(&colsql,
				"SELECT column_name, data_type, is_nullable "
				"FROM information_schema.columns "
				"WHERE table_schema = 'public' AND table_name = %s "
				"ORDER BY ordinal_position",
				quote_literal_cstr(table_names[t]));

			colret = SPI_execute(colsql.data, true, 0);
			pfree(colsql.data);

			if (colret == SPI_OK_SELECT)
			{
				ncols = SPI_processed;
				first_col = true;
				for (c = 0; c < ncols; c++)
				{
					char   *col_name;
					char   *data_type;
					char   *nullable;
					char   *gql_type;

					col_name = SPI_getvalue(SPI_tuptable->vals[c],
											SPI_tuptable->tupdesc, 1);
					data_type = SPI_getvalue(SPI_tuptable->vals[c],
											 SPI_tuptable->tupdesc, 2);
					nullable = SPI_getvalue(SPI_tuptable->vals[c],
											SPI_tuptable->tupdesc, 3);

					gql_type = gql_type_for_pg_type(data_type ? data_type : "text");

					if (!first_col)
						appendStringInfoString(&schema, ", ");
					first_col = false;

					appendStringInfo(&schema,
						"{\"name\": \"%s\", \"type\": \"%s\", \"nullable\": %s}",
						col_name ? col_name : "",
						gql_type,
						(nullable && strcmp(nullable, "NO") == 0) ? "false" : "true");
				}
			}

			appendStringInfoString(&schema, "]}");
		}
	}

	appendStringInfoString(&schema, "]}");
	return schema.data;
}

/* ----------------------------------------------------------------
 * graphql_execute
 *
 * SQL function: graphql_execute(query text,
 *                               variables jsonb DEFAULT NULL,
 *                               operation_name text DEFAULT NULL)
 * RETURNS jsonb
 *
 * Parses the GraphQL query, resolves it to SQL, executes via SPI,
 * and returns the result as a jsonb document.
 * ---------------------------------------------------------------- */
Datum
graphql_execute(PG_FUNCTION_ARGS)
{
	text	   *query_text;
	char	   *query_str;
	GqlNode    *ast;
	char	   *json_result;
	Datum		jsonb_datum;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("GraphQL query must not be NULL")));

	query_text = PG_GETARG_TEXT_PP(0);
	query_str = text_to_cstring(query_text);

	/* Parse the GraphQL query */
	ast = gql_parse(query_str);

	/* Execute via SPI */
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	if (ast->type == GQL_QUERY)
		json_result = resolve_query(ast);
	else if (ast->type == GQL_MUTATION)
		json_result = resolve_mutation(ast);
	else
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("GraphQL: unsupported operation type")));

	/*
	 * Copy json_result out of SPI memory context before SPI_finish,
	 * then convert to jsonb afterwards.
	 */
	{
		MemoryContext oldcxt;
		char	   *json_copy;

		oldcxt = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
		json_copy = pstrdup(json_result);
		MemoryContextSwitchTo(oldcxt);

		PopActiveSnapshot();
		SPI_finish();

		gql_free(ast);
		pfree(query_str);

		jsonb_datum = DirectFunctionCall1(jsonb_in,
										  CStringGetDatum(json_copy));

		PG_RETURN_DATUM(jsonb_datum);
	}
}

/* ----------------------------------------------------------------
 * graphql_schema
 *
 * SQL function: graphql_schema() RETURNS text
 *
 * Returns an auto-generated GraphQL SDL schema from the current
 * database's information_schema.
 * ---------------------------------------------------------------- */
Datum
graphql_schema(PG_FUNCTION_ARGS)
{
	char	   *sdl;
	text	   *result;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	sdl = build_sdl_schema();

	{
		MemoryContext oldcxt;
		char	   *sdl_copy;

		oldcxt = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
		sdl_copy = pstrdup(sdl);
		MemoryContextSwitchTo(oldcxt);

		PopActiveSnapshot();
		SPI_finish();

		result = cstring_to_text(sdl_copy);
		pfree(sdl_copy);

		PG_RETURN_TEXT_P(result);
	}
}

/* ----------------------------------------------------------------
 * graphql_schema_json
 *
 * SQL function: graphql_schema_json() RETURNS jsonb
 *
 * Returns the schema as a jsonb document.
 * ---------------------------------------------------------------- */
Datum
graphql_schema_json(PG_FUNCTION_ARGS)
{
	char	   *json_str;
	Datum		jsonb_datum;
	Datum		result_copy;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	json_str = build_json_schema();

	{
		MemoryContext oldcxt;
		char	   *json_copy;

		oldcxt = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
		json_copy = pstrdup(json_str);
		MemoryContextSwitchTo(oldcxt);

		PopActiveSnapshot();
		SPI_finish();

		jsonb_datum = DirectFunctionCall1(jsonb_in,
										  CStringGetDatum(json_copy));

		PG_RETURN_DATUM(jsonb_datum);
	}
}
