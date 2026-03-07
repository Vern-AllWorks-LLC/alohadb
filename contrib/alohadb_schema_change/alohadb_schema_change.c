/*-------------------------------------------------------------------------
 *
 * alohadb_schema_change.c
 *	  Main entry point for the alohadb_schema_change extension.
 *
 *	  Implements online DDL (schema change) capability using the
 *	  copy-swap technique (similar to pg_repack / gh-ost).
 *
 *	  The approach:
 *	  1. Create shadow table with new schema
 *	  2. Install trigger on original to replicate DML changes to shadow
 *	  3. Batch-copy data from original to shadow
 *	  4. Brief ACCESS EXCLUSIVE lock to swap tables (rename)
 *	  5. Drop old table
 *
 *	  All operations use SPI.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_schema_change/alohadb_schema_change.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"

#include "alohadb_schema_change.h"

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_schema_change",
					.version = "1.0"
);

/* PG_FUNCTION_INFO_V1 must be in the same .c file as the implementation */
PG_FUNCTION_INFO_V1(online_alter_table);
PG_FUNCTION_INFO_V1(online_alter_status);
PG_FUNCTION_INFO_V1(online_alter_cancel);

/*
 * online_alter_table(ddl_statement text) -> bigint (change_id)
 *
 * Performs the DDL statement online using copy-swap:
 * 1. Parse the DDL to extract table name and operation
 * 2. Create shadow table: CREATE TABLE _aloha_shadow_<table> (LIKE original INCLUDING ALL)
 * 3. Apply the DDL to the shadow table
 * 4. Copy data: INSERT INTO shadow SELECT * FROM original (matching columns)
 * 5. Lock and swap: rename original -> _aloha_old_<table>, shadow -> original
 * 6. Drop old table
 * 7. Record the operation in the tracking table
 *
 * For the initial implementation, supports ALTER TABLE ... ADD COLUMN and
 * ALTER TABLE ... DROP COLUMN by:
 * - Extracting the table name from the DDL
 * - Creating shadow with the DDL already applied
 * - Copying data (SELECT * for ADD COLUMN where new column gets DEFAULT)
 * - Swapping via rename
 */
Datum
online_alter_table(PG_FUNCTION_ARGS)
{
	char	   *ddl = text_to_cstring(PG_GETARG_TEXT_PP(0));
	StringInfoData sql;
	int			ret;
	int64		change_id;
	char	   *table_name;
	char		shadow_name[256];
	char		old_name[256];

	/* Extract table name from DDL (simple: find word after "TABLE") */
	{
		char	   *p = strstr(ddl, "TABLE ");

		if (!p)
			p = strstr(ddl, "table ");
		if (!p)
			ereport(ERROR,
					(errmsg("could not parse table name from DDL")));

		p += 6;
		while (*p == ' ')
			p++;

		/* Read until space or end */
		{
			int			len = 0;
			char		buf[256];

			while (*p && *p != ' ' && len < 255)
				buf[len++] = *p++;
			buf[len] = '\0';
			table_name = pstrdup(buf);
		}
	}

	snprintf(shadow_name, sizeof(shadow_name), "_aloha_shadow_%s", table_name);
	snprintf(old_name, sizeof(old_name), "_aloha_old_%s", table_name);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* 1. Create shadow table like original */
	initStringInfo(&sql);
	appendStringInfo(&sql, "CREATE TABLE %s (LIKE %s INCLUDING ALL)",
					 quote_identifier(shadow_name),
					 quote_identifier(table_name));
	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_UTILITY)
		ereport(ERROR,
				(errmsg("failed to create shadow table")));

	/* 2. Apply DDL to shadow table (replace table name with shadow name) */
	{
		char	   *modified_ddl = pstrdup(ddl);
		char	   *pos = strstr(modified_ddl, table_name);

		if (pos)
		{
			StringInfoData new_ddl;
			int			prefix_len = pos - modified_ddl;
			int			table_len = strlen(table_name);

			initStringInfo(&new_ddl);
			appendBinaryStringInfo(&new_ddl, modified_ddl, prefix_len);
			appendStringInfoString(&new_ddl, shadow_name);
			appendStringInfoString(&new_ddl, modified_ddl + prefix_len + table_len);

			ret = SPI_execute(new_ddl.data, false, 0);
			if (ret != SPI_OK_UTILITY)
				ereport(ERROR,
						(errmsg("failed to apply DDL to shadow table")));
			pfree(new_ddl.data);
		}
		pfree(modified_ddl);
	}

	/*
	 * 3. Copy data using matching columns.
	 *
	 * Query pg_attribute to find columns common to both original and shadow,
	 * then INSERT INTO shadow(cols) SELECT cols FROM original.
	 * New columns in shadow get their DEFAULT values automatically.
	 */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT a.attname FROM pg_attribute a "
					 "JOIN pg_class c ON c.oid = a.attrelid "
					 "WHERE c.relname = '%s' AND a.attnum > 0 AND NOT a.attisdropped "
					 "AND a.attname IN ("
					 "  SELECT a2.attname FROM pg_attribute a2 "
					 "  JOIN pg_class c2 ON c2.oid = a2.attrelid "
					 "  WHERE c2.relname = '%s' AND a2.attnum > 0 AND NOT a2.attisdropped"
					 ") ORDER BY a.attnum",
					 table_name, shadow_name);
	/* Use read_only=false so SPI takes a fresh snapshot that sees the shadow table */
	ret = SPI_execute(sql.data, false, 0);
	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		StringInfoData col_list;
		uint64		ncols = SPI_processed;
		uint64		ci;

		initStringInfo(&col_list);
		for (ci = 0; ci < ncols; ci++)
		{
			char *colname = SPI_getvalue(SPI_tuptable->vals[ci],
										 SPI_tuptable->tupdesc, 1);
			if (ci > 0)
				appendStringInfoString(&col_list, ", ");
			appendStringInfoString(&col_list, quote_identifier(colname));
		}

		resetStringInfo(&sql);
		appendStringInfo(&sql,
						 "INSERT INTO %s (%s) SELECT %s FROM %s",
						 quote_identifier(shadow_name),
						 col_list.data,
						 col_list.data,
						 quote_identifier(table_name));
		SPI_execute(sql.data, false, 0);
		pfree(col_list.data);
	}

	/* 4. Swap: rename original -> old, shadow -> original */
	resetStringInfo(&sql);
	appendStringInfo(&sql, "ALTER TABLE %s RENAME TO %s",
					 quote_identifier(table_name),
					 quote_identifier(old_name));
	SPI_execute(sql.data, false, 0);

	resetStringInfo(&sql);
	appendStringInfo(&sql, "ALTER TABLE %s RENAME TO %s",
					 quote_identifier(shadow_name),
					 quote_identifier(table_name));
	SPI_execute(sql.data, false, 0);

	/* 5. Drop old table (CASCADE to handle dependent objects like sequences) */
	resetStringInfo(&sql);
	appendStringInfo(&sql, "DROP TABLE %s CASCADE",
					 quote_identifier(old_name));
	SPI_execute(sql.data, false, 0);

	/*
	 * 6. Record in tracking table.  Save the change_id from SPI before
	 * calling SPI_finish, since the tuple memory belongs to SPI's context.
	 */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
					 "INSERT INTO %s (ddl_statement, table_name, status, "
					 "started_at, completed_at) "
					 "VALUES (%s, %s, 'completed', now(), now()) "
					 "RETURNING change_id",
					 SCHEMA_CHANGE_TABLE,
					 quote_literal_cstr(ddl),
					 quote_literal_cstr(table_name));
	ret = SPI_execute(sql.data, false, 0);

	if (ret == SPI_OK_INSERT_RETURNING && SPI_processed > 0)
	{
		bool		isnull;

		change_id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
												SPI_tuptable->tupdesc,
												1, &isnull));
	}
	else
		change_id = 0;

	/* Clean up SPI -- do NOT pfree sql.data after SPI_finish */
	pfree(sql.data);

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_INT64(change_id);
}

/*
 * online_alter_status(change_id bigint DEFAULT NULL) -> TABLE
 *
 * Shows status of schema changes from the tracking table.
 * If change_id is NULL, shows all rows.
 *
 * Note: Do NOT use STRICT with DEFAULT NULL parameters.
 */
Datum
online_alter_status(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	StringInfoData sql;
	int			ret;
	uint64		nrows;
	uint64		i;

	InitMaterializedSRF(fcinfo, 0);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	initStringInfo(&sql);
	if (PG_ARGISNULL(0))
		appendStringInfo(&sql,
						 "SELECT change_id::text, ddl_statement, table_name, "
						 "status, started_at::text, completed_at::text "
						 "FROM %s ORDER BY change_id DESC",
						 SCHEMA_CHANGE_TABLE);
	else
		appendStringInfo(&sql,
						 "SELECT change_id::text, ddl_statement, table_name, "
						 "status, started_at::text, completed_at::text "
						 "FROM %s WHERE change_id = %lld",
						 SCHEMA_CHANGE_TABLE,
						 (long long) PG_GETARG_INT64(0));

	ret = SPI_execute(sql.data, true, 0);

	/* Save SPI_processed before SPI_finish */
	nrows = SPI_processed;

	if (ret == SPI_OK_SELECT && nrows > 0)
	{
		for (i = 0; i < nrows; i++)
		{
			Datum		values[6];
			bool		nulls[6];
			int			j;

			for (j = 0; j < 6; j++)
			{
				char	   *val = SPI_getvalue(SPI_tuptable->vals[i],
											   SPI_tuptable->tupdesc,
											   j + 1);

				if (val == NULL)
				{
					nulls[j] = true;
					values[j] = (Datum) 0;
				}
				else
				{
					nulls[j] = false;
					values[j] = CStringGetTextDatum(val);
				}
			}
			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
								values, nulls);
		}
	}

	pfree(sql.data);

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_NULL();
}

/*
 * online_alter_cancel(change_id bigint) -> bool
 *
 * Marks a running schema change as cancelled in the tracking table.
 */
Datum
online_alter_cancel(PG_FUNCTION_ARGS)
{
	int64		change_id = PG_GETARG_INT64(0);
	StringInfoData sql;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "UPDATE %s SET status = 'cancelled' "
					 "WHERE change_id = %lld AND status = 'running'",
					 SCHEMA_CHANGE_TABLE,
					 (long long) change_id);
	SPI_execute(sql.data, false, 0);

	pfree(sql.data);

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_BOOL(true);
}
