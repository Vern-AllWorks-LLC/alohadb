/*-------------------------------------------------------------------------
 *
 * restapi_handler.c
 *	  REST-to-SQL request handler for the alohadb_restapi extension.
 *
 *	  Parses REST API paths and maps HTTP methods to SQL operations:
 *	    GET /api/<table>        -> SELECT (list with LIMIT)
 *	    GET /api/<table>/<id>   -> SELECT ... WHERE pk = id
 *	    POST /api/<table>       -> INSERT ... RETURNING row_to_json(...)
 *	    PUT /api/<table>/<id>   -> UPDATE ... WHERE pk = id RETURNING ...
 *	    PATCH /api/<table>/<id> -> UPDATE ... WHERE pk = id RETURNING ...
 *	    DELETE /api/<table>/<id> -> DELETE ... WHERE pk = id
 *
 *	  All SQL is executed via SPI within the caller's transaction.
 *	  Table and column names are protected with quote_identifier().
 *	  Literal values are protected with quote_literal_cstr().
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_restapi/restapi_handler.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "alohadb_restapi.h"
#include <string.h>

/* ----------------------------------------------------------------
 * Forward declarations
 * ---------------------------------------------------------------- */

static bool validate_table_name(const char *name);
static char *find_primary_key(const char *schema, const char *table);
static bool table_exists_in_schema(const char *schema, const char *table);
static RestApiResponse *make_response(int status_code, const char *body);
static RestApiResponse *make_error(int status_code, const char *message);
static RestApiResponse *restapi_table_select(const char *schema,
											 const char *table,
											 const char *id);
static RestApiResponse *restapi_table_insert(const char *schema,
											 const char *table,
											 const char *body);
static RestApiResponse *restapi_table_update(const char *schema,
											 const char *table,
											 const char *id,
											 const char *body);
static RestApiResponse *restapi_table_delete(const char *schema,
											 const char *table,
											 const char *id);

/* ----------------------------------------------------------------
 * make_response / make_error
 *
 * Convenience helpers to allocate a RestApiResponse.
 * ---------------------------------------------------------------- */

static RestApiResponse *
make_response(int status_code, const char *body)
{
	RestApiResponse *resp = palloc0(sizeof(RestApiResponse));

	resp->status_code = status_code;
	if (body)
	{
		resp->body_len = strlen(body);
		resp->body = pstrdup(body);
	}
	else
	{
		resp->body = pstrdup("null");
		resp->body_len = 4;
	}
	return resp;
}

static RestApiResponse *
make_error(int status_code, const char *message)
{
	StringInfoData buf;
	RestApiResponse *resp;

	initStringInfo(&buf);
	appendStringInfo(&buf, "{\"error\": ");

	/*
	 * Manually escape the message for JSON.  Minimal escaping:
	 * backslash, double quote, and control characters.
	 */
	appendStringInfoChar(&buf, '"');
	{
		const char *p;

		for (p = message; *p; p++)
		{
			switch (*p)
			{
				case '"':
					appendStringInfoString(&buf, "\\\"");
					break;
				case '\\':
					appendStringInfoString(&buf, "\\\\");
					break;
				case '\n':
					appendStringInfoString(&buf, "\\n");
					break;
				case '\r':
					appendStringInfoString(&buf, "\\r");
					break;
				case '\t':
					appendStringInfoString(&buf, "\\t");
					break;
				default:
					if ((unsigned char) *p < 0x20)
						appendStringInfo(&buf, "\\u%04x", (unsigned char) *p);
					else
						appendStringInfoChar(&buf, *p);
					break;
			}
		}
	}
	appendStringInfoChar(&buf, '"');
	appendStringInfoChar(&buf, '}');

	resp = palloc0(sizeof(RestApiResponse));
	resp->status_code = status_code;
	resp->body = buf.data;
	resp->body_len = buf.len;
	return resp;
}

/* ----------------------------------------------------------------
 * validate_table_name
 *
 * Return true if the name consists only of alphanumeric characters
 * and underscores (SQL injection prevention).
 * ---------------------------------------------------------------- */
static bool
validate_table_name(const char *name)
{
	const char *p;

	if (!name || name[0] == '\0')
		return false;

	for (p = name; *p; p++)
	{
		char	c = *p;

		if (!((c >= 'a' && c <= 'z') ||
			  (c >= 'A' && c <= 'Z') ||
			  (c >= '0' && c <= '9') ||
			  c == '_'))
			return false;
	}
	return true;
}

/* ----------------------------------------------------------------
 * table_exists_in_schema
 *
 * Check whether a table exists in the given schema by querying
 * pg_catalog.pg_tables.  Assumes SPI is already connected.
 * ---------------------------------------------------------------- */
static bool
table_exists_in_schema(const char *schema, const char *table)
{
	StringInfoData query;
	int			ret;
	bool		exists;

	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT 1 FROM pg_catalog.pg_tables "
					 "WHERE schemaname = %s AND tablename = %s",
					 quote_literal_cstr(schema),
					 quote_literal_cstr(table));

	ret = SPI_execute(query.data, true, 1);
	exists = (ret == SPI_OK_SELECT && SPI_processed > 0);

	pfree(query.data);
	return exists;
}

/* ----------------------------------------------------------------
 * find_primary_key
 *
 * Find the name of the primary key column for a table.
 * Uses pg_index + pg_attribute to find the first column of the
 * primary key index.
 *
 * Returns a palloc'd string with the column name, or NULL if no
 * primary key is found.
 * ---------------------------------------------------------------- */
static char *
find_primary_key(const char *schema, const char *table)
{
	StringInfoData query;
	int			ret;
	char	   *result = NULL;

	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT a.attname "
					 "FROM pg_catalog.pg_index i "
					 "JOIN pg_catalog.pg_attribute a "
					 "  ON a.attrelid = i.indrelid "
					 "  AND a.attnum = ANY(i.indkey) "
					 "WHERE i.indrelid = %s::regclass "
					 "  AND i.indisprimary "
					 "LIMIT 1",
					 quote_literal_cstr(psprintf("%s.%s",
												 quote_identifier(schema),
												 quote_identifier(table))));

	ret = SPI_execute(query.data, true, 1);

	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		char   *val;

		val = SPI_getvalue(SPI_tuptable->vals[0],
						   SPI_tuptable->tupdesc, 1);
		if (val)
			result = pstrdup(val);
	}

	pfree(query.data);
	return result;
}

/* ----------------------------------------------------------------
 * restapi_table_select
 *
 * Handle GET requests.
 *   GET /api/<table>      -> list rows (with LIMIT)
 *   GET /api/<table>/<id> -> get a single row by primary key
 * ---------------------------------------------------------------- */
static RestApiResponse *
restapi_table_select(const char *schema, const char *table, const char *id)
{
	StringInfoData query;
	int			ret;
	char	   *json_result;

	initStringInfo(&query);

	if (id)
	{
		/* Single row by primary key */
		char   *pk_col;

		pk_col = find_primary_key(schema, table);
		if (!pk_col)
			return make_error(400, "table has no primary key");

		appendStringInfo(&query,
						 "SELECT row_to_json(t) FROM %s.%s t "
						 "WHERE %s = %s LIMIT 1",
						 quote_identifier(schema),
						 quote_identifier(table),
						 quote_identifier(pk_col),
						 quote_literal_cstr(id));

		ret = SPI_execute(query.data, true, 1);
		pfree(query.data);

		if (ret != SPI_OK_SELECT)
			return make_error(500, "SELECT query failed");

		if (SPI_processed == 0)
			return make_error(404, "row not found");

		json_result = SPI_getvalue(SPI_tuptable->vals[0],
								   SPI_tuptable->tupdesc, 1);
		return make_response(200, json_result ? json_result : "null");
	}
	else
	{
		/* List all rows with limit */
		appendStringInfo(&query,
						 "SELECT json_agg(row_to_json(t)) "
						 "FROM (SELECT * FROM %s.%s LIMIT %d) t",
						 quote_identifier(schema),
						 quote_identifier(table),
						 RESTAPI_DEFAULT_LIMIT);

		ret = SPI_execute(query.data, true, 1);
		pfree(query.data);

		if (ret != SPI_OK_SELECT)
			return make_error(500, "SELECT query failed");

		if (SPI_processed == 0)
			return make_response(200, "[]");

		json_result = SPI_getvalue(SPI_tuptable->vals[0],
								   SPI_tuptable->tupdesc, 1);
		return make_response(200, json_result ? json_result : "[]");
	}
}

/* ----------------------------------------------------------------
 * restapi_table_insert
 *
 * Handle POST requests.
 *   POST /api/<table> with JSON body
 *   -> INSERT INTO <table> ... RETURNING row_to_json(...)
 *
 * Uses json_populate_record to map JSON keys to columns.
 * ---------------------------------------------------------------- */
static RestApiResponse *
restapi_table_insert(const char *schema, const char *table, const char *body)
{
	StringInfoData keys_query;
	StringInfoData query;
	int			ret;
	uint64		nkeys;
	uint64		k;
	char	   *json_result;
	bool		first;

	if (!body || body[0] == '\0')
		return make_error(400, "request body is required for POST");

	/*
	 * Extract column names from JSON body.  We insert only the
	 * columns present in the JSON, letting serial/default columns
	 * use their defaults.
	 */
	initStringInfo(&keys_query);
	appendStringInfo(&keys_query,
					 "SELECT json_object_keys(%s::json)",
					 quote_literal_cstr(body));

	ret = SPI_execute(keys_query.data, true, 0);
	pfree(keys_query.data);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
		return make_error(400, "could not parse JSON body keys");

	nkeys = SPI_processed;

	/*
	 * Build: INSERT INTO schema.table (col1, col2, ...)
	 *        SELECT col1, col2, ... FROM json_populate_record(...)
	 *        RETURNING row_to_json(schema.table.*)
	 */
	initStringInfo(&query);
	appendStringInfo(&query, "WITH ins AS (INSERT INTO %s.%s (",
					 quote_identifier(schema),
					 quote_identifier(table));

	first = true;
	for (k = 0; k < nkeys; k++)
	{
		char   *col = SPI_getvalue(SPI_tuptable->vals[k],
								   SPI_tuptable->tupdesc, 1);
		if (!col)
			continue;
		if (!first)
			appendStringInfoString(&query, ", ");
		first = false;
		appendStringInfoString(&query, quote_identifier(col));
	}

	appendStringInfo(&query, ") SELECT ");

	first = true;
	for (k = 0; k < nkeys; k++)
	{
		char   *col = SPI_getvalue(SPI_tuptable->vals[k],
								   SPI_tuptable->tupdesc, 1);
		if (!col)
			continue;
		if (!first)
			appendStringInfoString(&query, ", ");
		first = false;
		appendStringInfoString(&query, quote_identifier(col));
	}

	appendStringInfo(&query,
					 " FROM json_populate_record(null::%s.%s, %s::json)"
					 " RETURNING *) SELECT row_to_json(ins) FROM ins",
					 quote_identifier(schema),
					 quote_identifier(table),
					 quote_literal_cstr(body));

	ret = SPI_execute(query.data, false, 1);
	pfree(query.data);

	if (ret != SPI_OK_SELECT)
		return make_error(500, "INSERT query failed");

	if (SPI_processed == 0)
		return make_error(500, "INSERT returned no rows");

	json_result = SPI_getvalue(SPI_tuptable->vals[0],
							   SPI_tuptable->tupdesc, 1);
	return make_response(201, json_result ? json_result : "null");
}

/* ----------------------------------------------------------------
 * restapi_table_update
 *
 * Handle PUT/PATCH requests.
 *   PUT /api/<table>/<id> with JSON body
 *   -> UPDATE <table> SET ... WHERE pk = id RETURNING row_to_json(...)
 *
 * Extracts JSON body keys via json_object_keys() to build individual
 * SET assignments using json_populate_record for proper type casting.
 * ---------------------------------------------------------------- */
static RestApiResponse *
restapi_table_update(const char *schema, const char *table,
					 const char *id, const char *body)
{
	StringInfoData query;
	int			ret;
	char	   *pk_col;
	char	   *json_result;
	StringInfoData keys_query;
	int			keys_ret;
	uint64		nkeys;
	uint64		k;
	bool		first;

	if (!body || body[0] == '\0')
		return make_error(400, "request body is required for PUT/PATCH");

	if (!id)
		return make_error(400, "row ID is required for PUT/PATCH");

	pk_col = find_primary_key(schema, table);
	if (!pk_col)
		return make_error(400, "table has no primary key");

	/*
	 * Extract the keys from the JSON body using json_object_keys().
	 * We need the column names to build the SET clause.
	 */
	initStringInfo(&keys_query);
	appendStringInfo(&keys_query,
					 "SELECT json_object_keys(%s::json)",
					 quote_literal_cstr(body));

	keys_ret = SPI_execute(keys_query.data, true, 0);
	pfree(keys_query.data);

	if (keys_ret != SPI_OK_SELECT || SPI_processed == 0)
		return make_error(400, "could not parse JSON body keys");

	nkeys = SPI_processed;

	/*
	 * Build the UPDATE statement with individual SET assignments.
	 * Each column value is extracted via a subquery using
	 * json_populate_record for proper type casting.
	 */
	initStringInfo(&query);
	appendStringInfo(&query, "UPDATE %s.%s SET ",
					 quote_identifier(schema),
					 quote_identifier(table));

	first = true;
	for (k = 0; k < nkeys; k++)
	{
		char   *col_name;

		col_name = SPI_getvalue(SPI_tuptable->vals[k],
								SPI_tuptable->tupdesc, 1);
		if (!col_name)
			continue;

		/* Skip primary key column in SET clause */
		if (strcmp(col_name, pk_col) == 0)
			continue;

		if (!first)
			appendStringInfoString(&query, ", ");
		first = false;

		appendStringInfo(&query,
						 "%s = (SELECT %s FROM json_populate_record("
						 "null::%s.%s, %s::json))",
						 quote_identifier(col_name),
						 quote_identifier(col_name),
						 quote_identifier(schema),
						 quote_identifier(table),
						 quote_literal_cstr(body));
	}

	if (first)
	{
		pfree(query.data);
		return make_error(400, "no updatable columns in request body");
	}

	appendStringInfo(&query, " WHERE %s = %s RETURNING row_to_json(%s.*)",
					 quote_identifier(pk_col),
					 quote_literal_cstr(id),
					 quote_identifier(table));

	ret = SPI_execute(query.data, false, 1);
	pfree(query.data);

	if (ret != SPI_OK_UPDATE_RETURNING)
		return make_error(500, "UPDATE query failed");

	if (SPI_processed == 0)
		return make_error(404, "row not found");

	json_result = SPI_getvalue(SPI_tuptable->vals[0],
							   SPI_tuptable->tupdesc, 1);
	return make_response(200, json_result ? json_result : "null");
}

/* ----------------------------------------------------------------
 * restapi_table_delete
 *
 * Handle DELETE requests.
 *   DELETE /api/<table>/<id>
 *   -> DELETE FROM <table> WHERE pk = id
 * ---------------------------------------------------------------- */
static RestApiResponse *
restapi_table_delete(const char *schema, const char *table, const char *id)
{
	StringInfoData query;
	int			ret;
	char	   *pk_col;

	if (!id)
		return make_error(400, "row ID is required for DELETE");

	pk_col = find_primary_key(schema, table);
	if (!pk_col)
		return make_error(400, "table has no primary key");

	initStringInfo(&query);
	appendStringInfo(&query,
					 "DELETE FROM %s.%s WHERE %s = %s",
					 quote_identifier(schema),
					 quote_identifier(table),
					 quote_identifier(pk_col),
					 quote_literal_cstr(id));

	ret = SPI_execute(query.data, false, 0);
	pfree(query.data);

	if (ret != SPI_OK_DELETE)
		return make_error(500, "DELETE query failed");

	if (SPI_processed == 0)
		return make_error(404, "row not found");

	return make_response(200, "{\"deleted\": true}");
}

/* ----------------------------------------------------------------
 * restapi_handle_request
 *
 * Main dispatch function.  Parses the path to extract the table
 * name and optional row ID, validates the table name and existence,
 * optionally switches role, and dispatches to the appropriate
 * CRUD handler.
 *
 * Path format: /api/<table>[/<id>]
 *
 * Assumes SPI is already connected and a transaction is active.
 * ---------------------------------------------------------------- */
RestApiResponse *
restapi_handle_request(RestApiRequest *req)
{
	const char *path;
	const char *table_start;
	const char *table_end;
	char		table_name[NAMEDATALEN];
	char	   *id = NULL;
	int			table_len;
	const char *schema;

	schema = (restapi_schema && restapi_schema[0]) ? restapi_schema : "public";

	path = req->path;

	/*
	 * Check that path starts with /api/
	 */
	if (strncmp(path, RESTAPI_PATH_PREFIX, RESTAPI_PATH_PREFIX_LEN) != 0)
		return make_error(404, "path must start with /api/");

	/*
	 * Extract table name.
	 * Path after /api/ is: <table>[/<id>]
	 */
	table_start = path + RESTAPI_PATH_PREFIX_LEN;

	if (*table_start == '\0')
		return make_error(400, "table name is required in path");

	/* Find end of table name (next '/' or end of string) */
	table_end = strchr(table_start, '/');
	if (table_end)
	{
		table_len = table_end - table_start;

		/* Extract ID (everything after the slash) */
		if (*(table_end + 1) != '\0')
			id = pstrdup(table_end + 1);
	}
	else
	{
		table_len = strlen(table_start);
	}

	if (table_len <= 0 || table_len >= NAMEDATALEN)
		return make_error(400, "invalid table name");

	memcpy(table_name, table_start, table_len);
	table_name[table_len] = '\0';

	/*
	 * Remove trailing slash from ID if present.
	 */
	if (id)
	{
		int		id_len = strlen(id);

		if (id_len > 0 && id[id_len - 1] == '/')
			id[id_len - 1] = '\0';
		if (id[0] == '\0')
		{
			pfree(id);
			id = NULL;
		}
	}

	/*
	 * Validate table name: alphanumeric + underscore only to prevent
	 * SQL injection.
	 */
	if (!validate_table_name(table_name))
		return make_error(400, "invalid table name: only alphanumeric and underscore allowed");

	/*
	 * Validate that the table exists in the configured schema.
	 */
	if (!table_exists_in_schema(schema, table_name))
		return make_error(404, "table not found");

	/*
	 * Role switching: if the request specifies an X-PG-Role, execute
	 * SET LOCAL ROLE to switch before running the main query.
	 */
	if (req->role[0] != '\0')
	{
		StringInfoData role_cmd;
		int			role_ret;

		initStringInfo(&role_cmd);
		appendStringInfo(&role_cmd, "SET LOCAL ROLE %s",
						 quote_identifier(req->role));
		role_ret = SPI_execute(role_cmd.data, false, 0);
		pfree(role_cmd.data);

		if (role_ret != SPI_OK_UTILITY)
			return make_error(500, "failed to set role");
	}

	/*
	 * Dispatch by HTTP method.
	 */
	if (strcmp(req->method, "GET") == 0)
		return restapi_table_select(schema, table_name, id);
	else if (strcmp(req->method, "POST") == 0)
		return restapi_table_insert(schema, table_name, req->body);
	else if (strcmp(req->method, "PUT") == 0 ||
			 strcmp(req->method, "PATCH") == 0)
		return restapi_table_update(schema, table_name, id, req->body);
	else if (strcmp(req->method, "DELETE") == 0)
		return restapi_table_delete(schema, table_name, id);
	else
		return make_error(405, "method not allowed");
}
