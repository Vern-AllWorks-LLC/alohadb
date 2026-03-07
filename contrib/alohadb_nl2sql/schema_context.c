/*-------------------------------------------------------------------------
 *
 * schema_context.c
 *	  Build a concise schema summary for use in LLM prompts.
 *
 *	  Queries pg_catalog tables via SPI to enumerate tables, columns,
 *	  types, and constraints in the current database, then formats them
 *	  as a compact text string suitable for inclusion in an LLM prompt.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_nl2sql/schema_context.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"

#include "alohadb_nl2sql.h"

/*
 * SQL query to retrieve table and column information from the current
 * database.  We only look at user-created tables (not system catalogs)
 * and order by schema, table, column position so the output is stable
 * and readable.
 */
static const char *schema_query =
	"SELECT n.nspname AS schema_name, "
	"       c.relname AS table_name, "
	"       a.attname AS column_name, "
	"       pg_catalog.format_type(a.atttypid, a.atttypmod) AS column_type, "
	"       a.attnotnull AS not_null, "
	"       CASE WHEN con.contype = 'p' THEN true ELSE false END AS is_pk "
	"FROM pg_catalog.pg_class c "
	"JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
	"JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid "
	"LEFT JOIN pg_catalog.pg_constraint con "
	"  ON con.conrelid = c.oid "
	"  AND con.contype = 'p' "
	"  AND a.attnum = ANY(con.conkey) "
	"WHERE c.relkind IN ('r', 'p') "
	"  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') "
	"  AND a.attnum > 0 "
	"  AND NOT a.attisdropped "
	"ORDER BY n.nspname, c.relname, a.attnum";

/*
 * SQL query to retrieve foreign key relationships.
 */
static const char *fk_query =
	"SELECT "
	"  n1.nspname AS src_schema, "
	"  c1.relname AS src_table, "
	"  a1.attname AS src_column, "
	"  n2.nspname AS ref_schema, "
	"  c2.relname AS ref_table, "
	"  a2.attname AS ref_column "
	"FROM pg_catalog.pg_constraint con "
	"JOIN pg_catalog.pg_class c1 ON c1.oid = con.conrelid "
	"JOIN pg_catalog.pg_namespace n1 ON n1.oid = c1.relnamespace "
	"JOIN pg_catalog.pg_class c2 ON c2.oid = con.confrelid "
	"JOIN pg_catalog.pg_namespace n2 ON n2.oid = c2.relnamespace "
	"JOIN pg_catalog.pg_attribute a1 "
	"  ON a1.attrelid = con.conrelid AND a1.attnum = con.conkey[1] "
	"JOIN pg_catalog.pg_attribute a2 "
	"  ON a2.attrelid = con.confrelid AND a2.attnum = con.confkey[1] "
	"WHERE con.contype = 'f' "
	"  AND n1.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') "
	"ORDER BY n1.nspname, c1.relname, a1.attname";

/*
 * nl2sql_build_schema_context
 *
 * Connect to SPI, run the schema and FK queries, and build a human-readable
 * text summary of the database schema.  Returns a palloc'd C string.
 *
 * The caller must NOT already be connected to SPI.
 */
char *
nl2sql_build_schema_context(void)
{
	StringInfoData buf;
	int			ret;
	uint64		proc;
	uint64		i;
	char	   *prev_schema = NULL;
	char	   *prev_table = NULL;
	bool		truncated = false;

	initStringInfo(&buf);
	appendStringInfoString(&buf, "Database schema:\n\n");

	SPI_connect();

	/* ---- Table and column information ---- */
	ret = SPI_execute(schema_query, true, 0);
	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_nl2sql: schema query failed: %s",
						SPI_result_code_string(ret))));
	}

	proc = SPI_processed;

	for (i = 0; i < proc; i++)
	{
		char	   *schema_name;
		char	   *table_name;
		char	   *column_name;
		char	   *column_type;
		bool		not_null;
		bool		is_pk;
		bool		isnull;

		schema_name = SPI_getvalue(SPI_tuptable->vals[i],
								  SPI_tuptable->tupdesc, 1);
		table_name = SPI_getvalue(SPI_tuptable->vals[i],
								 SPI_tuptable->tupdesc, 2);
		column_name = SPI_getvalue(SPI_tuptable->vals[i],
								  SPI_tuptable->tupdesc, 3);
		column_type = SPI_getvalue(SPI_tuptable->vals[i],
								  SPI_tuptable->tupdesc, 4);

		not_null = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[i],
											  SPI_tuptable->tupdesc, 5,
											  &isnull));
		is_pk = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[i],
										   SPI_tuptable->tupdesc, 6,
										   &isnull));

		/* Check whether we are approaching the size limit */
		if (buf.len > NL2SQL_MAX_SCHEMA_CONTEXT_LEN - 256)
		{
			appendStringInfoString(&buf,
								   "\n-- (schema output truncated due to size) --\n");
			truncated = true;
			break;
		}

		/* Emit table header when we reach a new table */
		if (prev_schema == NULL ||
			prev_table == NULL ||
			strcmp(schema_name, prev_schema) != 0 ||
			strcmp(table_name, prev_table) != 0)
		{
			if (prev_table != NULL)
				appendStringInfoString(&buf, ");\n\n");

			appendStringInfo(&buf, "CREATE TABLE %s.%s (\n",
							 schema_name, table_name);

			if (prev_schema)
				pfree(prev_schema);
			if (prev_table)
				pfree(prev_table);

			prev_schema = pstrdup(schema_name);
			prev_table = pstrdup(table_name);
		}
		else
		{
			/* Separate columns with a comma */
			appendStringInfoString(&buf, ",\n");
		}

		/* Column definition */
		appendStringInfo(&buf, "  %s %s", column_name, column_type);
		if (is_pk)
			appendStringInfoString(&buf, " PRIMARY KEY");
		else if (not_null)
			appendStringInfoString(&buf, " NOT NULL");

		if (schema_name)
			pfree(schema_name);
		if (table_name)
			pfree(table_name);
		if (column_name)
			pfree(column_name);
		if (column_type)
			pfree(column_type);
	}

	/* Close the last table definition */
	if (prev_table != NULL)
		appendStringInfoString(&buf, "\n);\n\n");

	if (prev_schema)
		pfree(prev_schema);
	if (prev_table)
		pfree(prev_table);

	/* ---- Foreign key relationships ---- */
	if (!truncated)
	{
		ret = SPI_execute(fk_query, true, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0)
		{
			appendStringInfoString(&buf, "-- Foreign key relationships:\n");

			proc = SPI_processed;
			for (i = 0; i < proc; i++)
			{
				char	   *src_schema;
				char	   *src_table;
				char	   *src_column;
				char	   *ref_schema;
				char	   *ref_table;
				char	   *ref_column;

				if (buf.len > NL2SQL_MAX_SCHEMA_CONTEXT_LEN - 128)
				{
					appendStringInfoString(&buf,
										   "-- (FK output truncated) --\n");
					break;
				}

				src_schema = SPI_getvalue(SPI_tuptable->vals[i],
										  SPI_tuptable->tupdesc, 1);
				src_table = SPI_getvalue(SPI_tuptable->vals[i],
										 SPI_tuptable->tupdesc, 2);
				src_column = SPI_getvalue(SPI_tuptable->vals[i],
										  SPI_tuptable->tupdesc, 3);
				ref_schema = SPI_getvalue(SPI_tuptable->vals[i],
										  SPI_tuptable->tupdesc, 4);
				ref_table = SPI_getvalue(SPI_tuptable->vals[i],
										 SPI_tuptable->tupdesc, 5);
				ref_column = SPI_getvalue(SPI_tuptable->vals[i],
										  SPI_tuptable->tupdesc, 6);

				appendStringInfo(&buf,
								 "-- %s.%s(%s) -> %s.%s(%s)\n",
								 src_schema, src_table, src_column,
								 ref_schema, ref_table, ref_column);

				if (src_schema) pfree(src_schema);
				if (src_table) pfree(src_table);
				if (src_column) pfree(src_column);
				if (ref_schema) pfree(ref_schema);
				if (ref_table) pfree(ref_table);
				if (ref_column) pfree(ref_column);
			}
		}
	}

	SPI_finish();

	return buf.data;
}
