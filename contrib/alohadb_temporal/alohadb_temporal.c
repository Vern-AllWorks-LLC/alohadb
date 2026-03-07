/*-------------------------------------------------------------------------
 *
 * alohadb_temporal.c
 *	  SQL:2011-style system-versioned temporal tables for AlohaDB.
 *
 *	  Provides automatic history tracking via a BEFORE trigger that
 *	  copies old row versions to a companion history table on UPDATE
 *	  and DELETE.  Time-travel query functions allow querying the
 *	  state of a table at any past point in time.
 *
 *	  Uses the trigger+history table approach from MariaDB (GPL prior
 *	  art).  Oracle Flashback patent expired March 2024.  SQL:2011
 *	  temporal tables are an ISO standard.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_temporal/alohadb_temporal.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#include "alohadb_temporal.h"

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_temporal",
					.version = "1.0"
);

/* SQL-callable function declarations */
PG_FUNCTION_INFO_V1(alohadb_temporal_versioning_trigger);
PG_FUNCTION_INFO_V1(alohadb_enable_system_versioning);
PG_FUNCTION_INFO_V1(alohadb_disable_system_versioning);
PG_FUNCTION_INFO_V1(alohadb_as_of);
PG_FUNCTION_INFO_V1(alohadb_versions_between);
PG_FUNCTION_INFO_V1(alohadb_temporal_status);

/* ----------------------------------------------------------------
 * GUC variables
 * ---------------------------------------------------------------- */

/*
 * When true, the history table is preserved on disable_system_versioning.
 * When false, the history table is dropped.
 */
static bool temporal_keep_history = true;

/* ----------------------------------------------------------------
 * _PG_init
 *
 * Module load callback.  Registers GUC variables.
 * ---------------------------------------------------------------- */
void
_PG_init(void)
{
	DefineCustomBoolVariable("alohadb.temporal_keep_history",
							 "Keep history table when disabling system versioning.",
							 "When false, the history table is dropped on "
							 "alohadb_disable_system_versioning().",
							 &temporal_keep_history,
							 true,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	MarkGUCPrefixReserved("alohadb.temporal");
}

/* ----------------------------------------------------------------
 * Helper: build a schema-qualified, properly quoted table name
 * from a relation OID.
 * ---------------------------------------------------------------- */
static char *
temporal_get_qualified_name(Oid relid)
{
	char	   *nspname;
	char	   *relname;

	relname = get_rel_name(relid);
	if (relname == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("relation with OID %u does not exist", relid)));

	nspname = get_namespace_name(get_rel_namespace(relid));
	if (nspname == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("schema for relation \"%s\" does not exist", relname)));

	return quote_qualified_identifier(nspname, relname);
}

/* ----------------------------------------------------------------
 * Helper: build a schema-qualified, properly quoted history table
 * name from a relation OID (appends TEMPORAL_HISTORY_SUFFIX).
 * ---------------------------------------------------------------- */
static char *
temporal_get_history_name(Oid relid)
{
	char	   *nspname;
	char	   *relname;
	char	   *histname;

	relname = get_rel_name(relid);
	if (relname == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("relation with OID %u does not exist", relid)));

	nspname = get_namespace_name(get_rel_namespace(relid));
	if (nspname == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("schema for relation \"%s\" does not exist", relname)));

	histname = psprintf("%s%s", relname, TEMPORAL_HISTORY_SUFFIX);

	return quote_qualified_identifier(nspname, histname);
}

/* ----------------------------------------------------------------
 * Helper: get the unqualified history table name (for storing in
 * the registry table).
 * ---------------------------------------------------------------- */
static char *
temporal_get_history_relname(Oid relid)
{
	char	   *relname;

	relname = get_rel_name(relid);
	if (relname == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("relation with OID %u does not exist", relid)));

	return psprintf("%s%s", relname, TEMPORAL_HISTORY_SUFFIX);
}

/* ----------------------------------------------------------------
 * alohadb_enable_system_versioning(table_name regclass)
 *
 * Enable system versioning on the specified table:
 *   1. Add row_start and row_end columns
 *   2. Create history table (LIKE ... INCLUDING ALL)
 *   3. Create index on history table (row_start, row_end)
 *   4. Install the versioning trigger
 *   5. Register in alohadb_temporal_tables
 * ---------------------------------------------------------------- */
Datum
alohadb_enable_system_versioning(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	char	   *qualified_name;
	char	   *history_name;
	char	   *history_relname;
	StringInfoData buf;
	int			ret;

	qualified_name = temporal_get_qualified_name(relid);
	history_name = temporal_get_history_name(relid);
	history_relname = temporal_get_history_relname(relid);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/*
	 * Step 1: Add row_start and row_end columns to the base table.
	 */
	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "ALTER TABLE %s "
					 "ADD COLUMN %s timestamptz NOT NULL DEFAULT clock_timestamp(), "
					 "ADD COLUMN %s timestamptz NOT NULL DEFAULT '%s'",
					 qualified_name,
					 quote_identifier(TEMPORAL_ROW_START),
					 quote_identifier(TEMPORAL_ROW_END),
					 TEMPORAL_INFINITY);

	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_UTILITY)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to add temporal columns to %s", qualified_name)));

	/*
	 * Step 2: Create the history table using LIKE ... INCLUDING ALL.
	 */
	resetStringInfo(&buf);
	appendStringInfo(&buf,
					 "CREATE TABLE %s (LIKE %s INCLUDING ALL)",
					 history_name,
					 qualified_name);

	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_UTILITY)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to create history table %s", history_name)));

	/*
	 * Step 3: Create an index on the history table for efficient
	 * time-travel queries.
	 */
	resetStringInfo(&buf);
	appendStringInfo(&buf,
					 "CREATE INDEX ON %s (%s, %s)",
					 history_name,
					 quote_identifier(TEMPORAL_ROW_START),
					 quote_identifier(TEMPORAL_ROW_END));

	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_UTILITY)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to create index on history table %s",
						history_name)));

	/*
	 * Step 4: Install the BEFORE UPDATE OR DELETE trigger.
	 */
	resetStringInfo(&buf);
	appendStringInfo(&buf,
					 "CREATE TRIGGER alohadb_temporal_trig "
					 "BEFORE UPDATE OR DELETE ON %s "
					 "FOR EACH ROW EXECUTE FUNCTION "
					 "alohadb_temporal_versioning_trigger()",
					 qualified_name);

	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_UTILITY)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to create versioning trigger on %s",
						qualified_name)));

	/*
	 * Step 5: Register in alohadb_temporal_tables.
	 */
	resetStringInfo(&buf);
	appendStringInfo(&buf,
					 "INSERT INTO alohadb_temporal_tables "
					 "(table_name, history_table, enabled_at) "
					 "VALUES (%u, %s, now())",
					 relid,
					 quote_literal_cstr(history_relname));

	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_INSERT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to register table in alohadb_temporal_tables")));

	pfree(buf.data);

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * alohadb_disable_system_versioning(table_name regclass)
 *
 * Disable system versioning:
 *   1. Drop the trigger
 *   2. Remove from registry
 *   3. Optionally drop the history table
 *   4. Drop row_start and row_end columns
 * ---------------------------------------------------------------- */
Datum
alohadb_disable_system_versioning(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	char	   *qualified_name;
	char	   *history_name;
	StringInfoData buf;
	int			ret;

	qualified_name = temporal_get_qualified_name(relid);
	history_name = temporal_get_history_name(relid);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/*
	 * Step 1: Drop the versioning trigger.
	 */
	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "DROP TRIGGER IF EXISTS alohadb_temporal_trig ON %s",
					 qualified_name);

	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_UTILITY)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to drop versioning trigger on %s",
						qualified_name)));

	/*
	 * Step 2: Remove from the registry.
	 */
	resetStringInfo(&buf);
	appendStringInfo(&buf,
					 "DELETE FROM alohadb_temporal_tables "
					 "WHERE table_name = %u",
					 relid);

	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_DELETE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to remove table from alohadb_temporal_tables")));

	/*
	 * Step 3: If alohadb.temporal_keep_history is false, drop the
	 * history table.
	 */
	if (!temporal_keep_history)
	{
		resetStringInfo(&buf);
		appendStringInfo(&buf,
						 "DROP TABLE IF EXISTS %s",
						 history_name);

		ret = SPI_execute(buf.data, false, 0);
		if (ret != SPI_OK_UTILITY)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("failed to drop history table %s",
							history_name)));
	}

	/*
	 * Step 4: Drop the row_start and row_end columns.
	 */
	resetStringInfo(&buf);
	appendStringInfo(&buf,
					 "ALTER TABLE %s "
					 "DROP COLUMN %s, "
					 "DROP COLUMN %s",
					 qualified_name,
					 quote_identifier(TEMPORAL_ROW_START),
					 quote_identifier(TEMPORAL_ROW_END));

	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_UTILITY)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to drop temporal columns from %s",
						qualified_name)));

	pfree(buf.data);

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * alohadb_temporal_versioning_trigger()
 *
 * BEFORE UPDATE: Copy OLD row to history table with row_end set
 * to clock_timestamp().  Set NEW.row_start = clock_timestamp().
 * Return NEW.
 *
 * BEFORE DELETE: Copy OLD row to history table with row_end set
 * to clock_timestamp().  Return OLD.
 *
 * The trigger builds an INSERT statement using individual column
 * values from the OLD tuple, overriding row_end with
 * clock_timestamp().  For UPDATE, it also modifies row_start on
 * the NEW tuple using SPI_modifytuple().
 * ---------------------------------------------------------------- */
Datum
alohadb_temporal_versioning_trigger(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	Relation	rel;
	TupleDesc	tupdesc;
	HeapTuple	old_tuple;
	HeapTuple	new_tuple;
	HeapTuple	ret_tuple;
	int			natts;
	int			i;
	int			row_end_attno;
	int			row_start_attno;
	char	   *history_name;
	Oid			relid;
	StringInfoData buf;
	int			spi_ret;

	/* Sanity checks */
	if (!CALLED_AS_TRIGGER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("alohadb_temporal_versioning_trigger: not called as trigger")));

	if (!TRIGGER_FIRED_BEFORE(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("alohadb_temporal_versioning_trigger: must be fired BEFORE")));

	if (!TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event) &&
		!TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("alohadb_temporal_versioning_trigger: must be fired for UPDATE or DELETE")));

	rel = trigdata->tg_relation;
	tupdesc = rel->rd_att;
	natts = tupdesc->natts;
	relid = RelationGetRelid(rel);
	old_tuple = trigdata->tg_trigtuple;

	if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		new_tuple = trigdata->tg_newtuple;
	else
		new_tuple = NULL;

	/* Find the attribute numbers for row_start and row_end */
	row_start_attno = SPI_fnumber(tupdesc, TEMPORAL_ROW_START);
	row_end_attno = SPI_fnumber(tupdesc, TEMPORAL_ROW_END);

	if (row_start_attno == SPI_ERROR_NOATTRIBUTE ||
		row_end_attno == SPI_ERROR_NOATTRIBUTE)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("table \"%s\" is missing temporal columns \"%s\" and/or \"%s\"",
						RelationGetRelationName(rel),
						TEMPORAL_ROW_START, TEMPORAL_ROW_END)));

	/* Get the history table name */
	history_name = temporal_get_history_name(relid);

	/*
	 * Build an INSERT statement for the history table with individual
	 * column values from the OLD tuple.  We override row_end with
	 * clock_timestamp().
	 */
	SPI_connect();

	initStringInfo(&buf);
	appendStringInfo(&buf, "INSERT INTO %s VALUES (", history_name);

	for (i = 1; i <= natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i - 1);
		char	   *val;

		/* Skip dropped columns */
		if (att->attisdropped)
		{
			if (i > 1)
				appendStringInfoString(&buf, ", ");
			appendStringInfoString(&buf, "NULL");
			continue;
		}

		if (i > 1)
			appendStringInfoString(&buf, ", ");

		/*
		 * For the row_end column, use clock_timestamp() instead of
		 * the value from the OLD tuple.
		 */
		if (i == row_end_attno)
		{
			appendStringInfoString(&buf, "clock_timestamp()");
			continue;
		}

		val = SPI_getvalue(old_tuple, tupdesc, i);
		if (val == NULL)
		{
			appendStringInfoString(&buf, "NULL");
		}
		else
		{
			/*
			 * Quote the value appropriately.  We cast through the
			 * type's OID to ensure correct interpretation.
			 */
			appendStringInfo(&buf, "%s::%s",
							 quote_literal_cstr(val),
							 format_type_be(att->atttypid));
		}
	}

	appendStringInfoChar(&buf, ')');

	spi_ret = SPI_execute(buf.data, false, 0);
	if (spi_ret != SPI_OK_INSERT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_temporal_versioning_trigger: "
						"failed to insert into history table %s",
						history_name)));

	pfree(buf.data);

	/*
	 * For UPDATE: modify the NEW tuple to set row_start = clock_timestamp().
	 * Use SPI_modifytuple while we're still inside the SPI connection.
	 */
	if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
	{
		Datum		new_row_start;
		char		new_null;
		int			changedcols[1];

		/* Get current timestamp via GetCurrentTimestamp() */
		new_row_start = TimestampTzGetDatum(GetCurrentTimestamp());

		changedcols[0] = row_start_attno;
		new_null = ' ';		/* not null */

		ret_tuple = SPI_modifytuple(rel, new_tuple,
									1, changedcols,
									&new_row_start, &new_null);
		if (ret_tuple == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("alohadb_temporal_versioning_trigger: "
							"SPI_modifytuple failed")));

		SPI_finish();
		return PointerGetDatum(ret_tuple);
	}
	else
	{
		/* DELETE: return the original OLD tuple unchanged */
		SPI_finish();
		return PointerGetDatum(old_tuple);
	}
}

/* ----------------------------------------------------------------
 * alohadb_as_of(table_name regclass, ts timestamptz)
 *
 * Time-travel query: return all rows as they existed at the given
 * point in time.  Combines current rows and historical rows.
 *
 * SELECT * FROM <table> WHERE row_start <= ts AND row_end > ts
 * UNION ALL
 * SELECT * FROM <history> WHERE row_start <= ts AND row_end > ts
 * ---------------------------------------------------------------- */
Datum
alohadb_as_of(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	TimestampTz ts = PG_GETARG_TIMESTAMPTZ(1);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Tuplestorestate *tupstore;
	TupleDesc	tupdesc;
	char	   *qualified_name;
	char	   *history_name;
	StringInfoData buf;
	char	   *ts_str;
	int			ret;
	uint64		i;

	/* Set up the tuplestore using InitMaterializedSRF */
	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC | MAT_SRF_BLESS);
	tupstore = rsinfo->setResult;
	tupdesc = rsinfo->setDesc;

	qualified_name = temporal_get_qualified_name(relid);
	history_name = temporal_get_history_name(relid);

	/* Convert timestamp to string for use in query */
	ts_str = DatumGetCString(DirectFunctionCall1(timestamptz_out,
												 TimestampTzGetDatum(ts)));

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/*
	 * Build the UNION ALL query.
	 */
	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT * FROM %s "
					 "WHERE %s <= %s AND %s > %s "
					 "UNION ALL "
					 "SELECT * FROM %s "
					 "WHERE %s <= %s AND %s > %s",
					 qualified_name,
					 quote_identifier(TEMPORAL_ROW_START),
					 quote_literal_cstr(ts_str),
					 quote_identifier(TEMPORAL_ROW_END),
					 quote_literal_cstr(ts_str),
					 history_name,
					 quote_identifier(TEMPORAL_ROW_START),
					 quote_literal_cstr(ts_str),
					 quote_identifier(TEMPORAL_ROW_END),
					 quote_literal_cstr(ts_str));

	ret = SPI_execute(buf.data, true, 0);
	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_as_of: failed to execute time-travel query")));

	/* Copy results into the tuplestore */
	for (i = 0; i < SPI_processed; i++)
	{
		HeapTuple	tuple;

		tuple = SPI_tuptable->vals[i];
		tuplestore_puttuple(tupstore, tuple);
	}

	pfree(buf.data);

	PopActiveSnapshot();
	SPI_finish();

	return (Datum) 0;
}

/* ----------------------------------------------------------------
 * alohadb_versions_between(table_name regclass,
 *                          ts_start timestamptz,
 *                          ts_end timestamptz)
 *
 * Return all row versions that were valid at any point within the
 * specified time range.  A row version is included if its validity
 * period [row_start, row_end) overlaps with [ts_start, ts_end).
 *
 * SELECT * FROM <table>
 *   WHERE row_start < ts_end AND row_end > ts_start
 * UNION ALL
 * SELECT * FROM <history>
 *   WHERE row_start < ts_end AND row_end > ts_start
 * ---------------------------------------------------------------- */
Datum
alohadb_versions_between(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	TimestampTz ts_start = PG_GETARG_TIMESTAMPTZ(1);
	TimestampTz ts_end = PG_GETARG_TIMESTAMPTZ(2);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Tuplestorestate *tupstore;
	TupleDesc	tupdesc;
	char	   *qualified_name;
	char	   *history_name;
	StringInfoData buf;
	char	   *ts_start_str;
	char	   *ts_end_str;
	int			ret;
	uint64		i;

	/* Set up the tuplestore using InitMaterializedSRF */
	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC | MAT_SRF_BLESS);
	tupstore = rsinfo->setResult;
	tupdesc = rsinfo->setDesc;

	qualified_name = temporal_get_qualified_name(relid);
	history_name = temporal_get_history_name(relid);

	/* Convert timestamps to strings */
	ts_start_str = DatumGetCString(DirectFunctionCall1(timestamptz_out,
													   TimestampTzGetDatum(ts_start)));
	ts_end_str = DatumGetCString(DirectFunctionCall1(timestamptz_out,
													 TimestampTzGetDatum(ts_end)));

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/*
	 * Build the UNION ALL query with overlap condition.
	 */
	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT * FROM %s "
					 "WHERE %s < %s AND %s > %s "
					 "UNION ALL "
					 "SELECT * FROM %s "
					 "WHERE %s < %s AND %s > %s",
					 qualified_name,
					 quote_identifier(TEMPORAL_ROW_START),
					 quote_literal_cstr(ts_end_str),
					 quote_identifier(TEMPORAL_ROW_END),
					 quote_literal_cstr(ts_start_str),
					 history_name,
					 quote_identifier(TEMPORAL_ROW_START),
					 quote_literal_cstr(ts_end_str),
					 quote_identifier(TEMPORAL_ROW_END),
					 quote_literal_cstr(ts_start_str));

	ret = SPI_execute(buf.data, true, 0);
	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_versions_between: failed to execute time-range query")));

	/* Copy results into the tuplestore */
	for (i = 0; i < SPI_processed; i++)
	{
		HeapTuple	tuple;

		tuple = SPI_tuptable->vals[i];
		tuplestore_puttuple(tupstore, tuple);
	}

	pfree(buf.data);

	PopActiveSnapshot();
	SPI_finish();

	return (Datum) 0;
}

/* ----------------------------------------------------------------
 * alohadb_temporal_status()
 *
 * Show all tables with system versioning enabled.
 *
 * Returns: table_name text, history_table text, enabled_at timestamptz
 * ---------------------------------------------------------------- */
Datum
alohadb_temporal_status(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Tuplestorestate *tupstore;
	TupleDesc	tupdesc;
	int			ret;
	uint64		i;

	/* Set up the tuplestore using InitMaterializedSRF */
	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC | MAT_SRF_BLESS);
	tupstore = rsinfo->setResult;
	tupdesc = rsinfo->setDesc;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(
		"SELECT table_name::regclass::text, history_table, enabled_at "
		"FROM alohadb_temporal_tables "
		"ORDER BY enabled_at",
		true, 0);

	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_temporal_status: failed to query registry")));

	/* Copy results into the tuplestore */
	for (i = 0; i < SPI_processed; i++)
	{
		Datum		values[3];
		bool		nulls[3];
		bool		isnull;

		values[0] = SPI_getbinval(SPI_tuptable->vals[i],
								  SPI_tuptable->tupdesc, 1, &isnull);
		nulls[0] = isnull;

		values[1] = SPI_getbinval(SPI_tuptable->vals[i],
								  SPI_tuptable->tupdesc, 2, &isnull);
		nulls[1] = isnull;

		values[2] = SPI_getbinval(SPI_tuptable->vals[i],
								  SPI_tuptable->tupdesc, 3, &isnull);
		nulls[2] = isnull;

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	PopActiveSnapshot();
	SPI_finish();

	return (Datum) 0;
}
