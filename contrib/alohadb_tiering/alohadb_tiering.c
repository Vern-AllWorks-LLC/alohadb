/*-------------------------------------------------------------------------
 *
 * alohadb_tiering.c
 *	  Main entry point for the alohadb_tiering extension.
 *
 *	  Implements automatic storage tiering for partitioned tables.
 *	  A background worker periodically scans the alohadb_tiering_rules
 *	  table and moves partitions whose upper boundary is older than
 *	  the configured age_threshold to the specified target_tablespace
 *	  via ALTER TABLE <partition> SET TABLESPACE <target>.
 *
 *	  Time-based only -- NO access-frequency heat maps.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_tiering/alohadb_tiering.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

/* Background worker essentials */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/latch.h"

/* SPI and transaction management */
#include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#include "alohadb_tiering.h"

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_tiering",
					.version = "1.0"
);

/* SQL-callable function declarations */
PG_FUNCTION_INFO_V1(alohadb_tiering_status);
PG_FUNCTION_INFO_V1(alohadb_tiering_check_now);

/* ----------------------------------------------------------------
 * GUC variables
 * ---------------------------------------------------------------- */

/* Check interval in seconds; default 3600 (1 hour) */
static int	tiering_check_interval = TIERING_DEFAULT_INTERVAL_S;

/* Database to connect to */
static char *tiering_database = NULL;

/* ----------------------------------------------------------------
 * Backend-local status tracking
 * ---------------------------------------------------------------- */

static TieringRuleStatus rule_status[TIERING_MAX_RULES];
static int	num_rule_status = 0;

/* Custom wait event for WaitLatch */
static uint32 tiering_wait_event = 0;

/* ----------------------------------------------------------------
 * Forward declarations
 * ---------------------------------------------------------------- */

static void tiering_perform_check(void);
static int	tiering_process_rule(int rule_id, const char *parent_table,
								 const char *age_threshold,
								 const char *target_tablespace);
static TieringRuleStatus *tiering_find_or_create_status(int rule_id,
														const char *parent_table);

/* ----------------------------------------------------------------
 * tiering_perform_check
 *
 * Execute one full scan of the alohadb_tiering_rules table and
 * move qualifying partitions.  Called from the bgworker main loop
 * and also from the alohadb_tiering_check_now() SQL function.
 * ---------------------------------------------------------------- */
static void
tiering_perform_check(void)
{
	int			ret;
	uint64		nrules;
	uint64		i;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "alohadb_tiering: scanning rules");

	/*
	 * Check whether the rules table exists.  It won't be there until
	 * CREATE EXTENSION alohadb_tiering has been run in this database.
	 */
	ret = SPI_execute(
		"SELECT 1 FROM pg_catalog.pg_class c "
		"JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
		"WHERE n.nspname = 'public' AND c.relname = 'alohadb_tiering_rules'",
		true, 1);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
	{
		/* Rules table does not exist yet; nothing to do. */
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		pgstat_report_activity(STATE_IDLE, NULL);
		return;
	}

	/*
	 * Fetch all enabled rules.
	 */
	ret = SPI_execute(
		"SELECT id, parent_table::text, "
		"       age_threshold::text, target_tablespace "
		"FROM public.alohadb_tiering_rules "
		"WHERE enabled = true "
		"ORDER BY id",
		true, 0);

	if (ret != SPI_OK_SELECT)
	{
		elog(WARNING, "alohadb_tiering: failed to read rules table: error code %d", ret);
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		pgstat_report_activity(STATE_IDLE, NULL);
		return;
	}

	nrules = SPI_processed;

	/*
	 * Process each rule.  We must copy the SPI results out because
	 * tiering_process_rule() will itself run SPI queries that will
	 * overwrite the current SPI result set.
	 */
	if (nrules > 0)
	{
		typedef struct RuleCopy
		{
			int			id;
			char		parent_table[NAMEDATALEN];
			char		age_threshold[64];
			char		target_tablespace[NAMEDATALEN];
		} RuleCopy;

		RuleCopy   *rules;
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(CurTransactionContext);
		rules = palloc(sizeof(RuleCopy) * nrules);
		MemoryContextSwitchTo(oldcxt);

		for (i = 0; i < nrules; i++)
		{
			bool		isnull;
			char	   *val;

			rules[i].id = DatumGetInt32(
				SPI_getbinval(SPI_tuptable->vals[i],
							  SPI_tuptable->tupdesc, 1, &isnull));

			val = SPI_getvalue(SPI_tuptable->vals[i],
							   SPI_tuptable->tupdesc, 2);
			strlcpy(rules[i].parent_table, val ? val : "", NAMEDATALEN);

			val = SPI_getvalue(SPI_tuptable->vals[i],
							   SPI_tuptable->tupdesc, 3);
			strlcpy(rules[i].age_threshold, val ? val : "", 64);

			val = SPI_getvalue(SPI_tuptable->vals[i],
							   SPI_tuptable->tupdesc, 4);
			strlcpy(rules[i].target_tablespace, val ? val : "", NAMEDATALEN);
		}

		/* Done reading from the initial SPI result set. */
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();

		/*
		 * Now process each rule in its own transaction so that a failure
		 * in one rule does not abort the others.
		 */
		for (i = 0; i < nrules; i++)
		{
			int		moved;

			moved = tiering_process_rule(rules[i].id,
										 rules[i].parent_table,
										 rules[i].age_threshold,
										 rules[i].target_tablespace);

			if (moved > 0)
				elog(LOG, "alohadb_tiering: rule %d (%s) moved %d partition(s) to tablespace \"%s\"",
					 rules[i].id, rules[i].parent_table, moved,
					 rules[i].target_tablespace);
		}
	}
	else
	{
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
	}

	pgstat_report_activity(STATE_IDLE, NULL);
}

/* ----------------------------------------------------------------
 * tiering_process_rule
 *
 * For a single rule, find partitions of parent_table whose upper
 * range boundary timestamp is older than (now - age_threshold) and
 * that are not already on target_tablespace.  Move each one via
 * ALTER TABLE ... SET TABLESPACE.
 *
 * Returns the number of partitions moved.
 * ---------------------------------------------------------------- */
static int
tiering_process_rule(int rule_id, const char *parent_table,
					 const char *age_threshold,
					 const char *target_tablespace)
{
	StringInfoData buf;
	int			ret;
	int			moved = 0;
	uint64		nparts;
	uint64		j;
	TieringRuleStatus *st;

	/*
	 * Build a query that finds child partitions whose upper range bound
	 * (as text, cast to timestamptz) is older than now - age_threshold,
	 * and whose current tablespace differs from the target.
	 *
	 * We look at pg_class.relispartition and the partition range upper
	 * bound from pg_catalog.pg_partition_tree() combined with the
	 * range bound exposed by pg_get_expr on pg_class.relpartbound.
	 *
	 * Strategy: parse the range upper bound from the partition bound
	 * expression.  The expression looks like:
	 *   FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')
	 * We extract the TO value.
	 */
	initStringInfo(&buf);
	appendStringInfo(&buf,
		"SELECT c.oid::regclass::text AS partition_name "
		"FROM pg_catalog.pg_inherits i "
		"JOIN pg_catalog.pg_class c ON c.oid = i.inhrelid "
		"JOIN pg_catalog.pg_class p ON p.oid = i.inhparent "
		"JOIN pg_catalog.pg_namespace pn ON pn.oid = p.relnamespace "
		"LEFT JOIN pg_catalog.pg_tablespace ts ON ts.oid = c.reltablespace "
		"WHERE i.inhparent = %s::regclass "
		"  AND c.relispartition "
		"  AND pg_catalog.pg_get_expr(c.relpartbound, c.oid) LIKE 'FOR VALUES FROM%%' "
		"  AND ("
		"    SELECT (regexp_match(pg_catalog.pg_get_expr(c.relpartbound, c.oid), "
		"            'TO \\(''([^'']+)''\\)'))[1]::timestamptz"
		"  ) <= (current_timestamp - %s::interval) "
		"  AND coalesce(ts.spcname, 'pg_default') <> %s",
		quote_literal_cstr(parent_table),
		quote_literal_cstr(age_threshold),
		quote_literal_cstr(target_tablespace));

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "alohadb_tiering: evaluating rule");

	debug_query_string = buf.data;
	ret = SPI_execute(buf.data, true, 0);

	if (ret != SPI_OK_SELECT)
	{
		elog(WARNING, "alohadb_tiering: rule %d: failed to query partitions for %s: error code %d",
			 rule_id, parent_table, ret);
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		debug_query_string = NULL;
		pfree(buf.data);
		return 0;
	}

	nparts = SPI_processed;

	if (nparts == 0)
	{
		/* Nothing to move. */
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		debug_query_string = NULL;

		st = tiering_find_or_create_status(rule_id, parent_table);
		st->last_check = GetCurrentTimestamp();

		pfree(buf.data);
		return 0;
	}

	/*
	 * Copy partition names out of SPI memory before we run further
	 * SPI commands.
	 */
	{
		char	  **part_names;
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(CurTransactionContext);
		part_names = palloc(sizeof(char *) * nparts);
		for (j = 0; j < nparts; j++)
		{
			bool	isnull;
			char   *val;

			val = SPI_getvalue(SPI_tuptable->vals[j],
							   SPI_tuptable->tupdesc, 1);
			part_names[j] = pstrdup(val ? val : "");
		}
		MemoryContextSwitchTo(oldcxt);

		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		debug_query_string = NULL;

		/*
		 * Now issue ALTER TABLE ... SET TABLESPACE for each partition,
		 * each in its own transaction.
		 */
		for (j = 0; j < nparts; j++)
		{
			StringInfoData alter;

			initStringInfo(&alter);
			appendStringInfo(&alter,
				"ALTER TABLE %s SET TABLESPACE %s",
				quote_identifier(part_names[j]),
				quote_identifier(target_tablespace));

			SetCurrentStatementStartTimestamp();
			StartTransactionCommand();
			SPI_connect();
			PushActiveSnapshot(GetTransactionSnapshot());

			debug_query_string = alter.data;
			pgstat_report_activity(STATE_RUNNING, alter.data);

			ret = SPI_execute(alter.data, false, 0);

			if (ret == SPI_OK_UTILITY)
			{
				moved++;
				elog(LOG, "alohadb_tiering: moved partition %s to tablespace \"%s\"",
					 part_names[j], target_tablespace);
			}
			else
			{
				elog(WARNING, "alohadb_tiering: failed to move partition %s to tablespace \"%s\": error code %d",
					 part_names[j], target_tablespace, ret);
			}

			SPI_finish();
			PopActiveSnapshot();
			CommitTransactionCommand();
			debug_query_string = NULL;
			pgstat_report_activity(STATE_IDLE, NULL);

			pfree(alter.data);
		}
	}

	/* Update status tracking */
	st = tiering_find_or_create_status(rule_id, parent_table);
	st->partitions_moved += moved;
	st->last_check = GetCurrentTimestamp();

	pfree(buf.data);
	return moved;
}

/* ----------------------------------------------------------------
 * tiering_find_or_create_status
 *
 * Find or allocate a status tracking entry for a given rule_id.
 * ---------------------------------------------------------------- */
static TieringRuleStatus *
tiering_find_or_create_status(int rule_id, const char *parent_table)
{
	int		i;

	for (i = 0; i < num_rule_status; i++)
	{
		if (rule_status[i].rule_id == rule_id)
			return &rule_status[i];
	}

	if (num_rule_status >= TIERING_MAX_RULES)
	{
		/* Wrap around to first slot if we exceed the limit. */
		elog(WARNING, "alohadb_tiering: status tracking table full, reusing slot 0");
		num_rule_status = 0;
	}

	i = num_rule_status++;
	memset(&rule_status[i], 0, sizeof(TieringRuleStatus));
	rule_status[i].rule_id = rule_id;
	strlcpy(rule_status[i].parent_table,
			parent_table ? parent_table : "",
			NAMEDATALEN);
	return &rule_status[i];
}

/* ----------------------------------------------------------------
 * Background worker entry point
 * ---------------------------------------------------------------- */
void
alohadb_tiering_worker_main(Datum main_arg)
{
	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to the configured database */
	BackgroundWorkerInitializeConnection(tiering_database, NULL, 0);

	elog(LOG, "alohadb_tiering: background worker started, "
		 "check interval = %d s, database = \"%s\"",
		 tiering_check_interval,
		 tiering_database ? tiering_database : "postgres");

	/*
	 * Main loop: periodically check tiering rules until SIGTERM is
	 * received and processed by ProcessInterrupts (via die/CHECK_FOR_INTERRUPTS).
	 */
	for (;;)
	{
		/* Allocate or fetch the custom wait event on first use. */
		if (tiering_wait_event == 0)
			tiering_wait_event = WaitEventExtensionNew("AlohadbTieringMain");

		/*
		 * Sleep for the configured interval, waking on latch set
		 * (SIGHUP, SIGTERM) or postmaster death.
		 */
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 tiering_check_interval * 1000L,
						 tiering_wait_event);
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		/* Reload configuration on SIGHUP. */
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
			elog(LOG, "alohadb_tiering: configuration reloaded, "
				 "check interval = %d s", tiering_check_interval);
		}

		/* Perform the actual tiering check. */
		tiering_perform_check();

		pgstat_report_stat(true);
	}

	/* Not reachable */
}

/* ----------------------------------------------------------------
 * _PG_init
 *
 * Module load callback.  Registers GUC variables and, if loaded
 * via shared_preload_libraries, registers the background worker.
 * ---------------------------------------------------------------- */
void
_PG_init(void)
{
	BackgroundWorker worker;

	/*
	 * Define GUC: alohadb.tiering_check_interval
	 *
	 * This GUC can be changed via SIGHUP, so we define it even when
	 * the module is loaded outside of shared_preload_libraries (e.g.
	 * for the SQL-callable check_now function).
	 */
	DefineCustomIntVariable("alohadb.tiering_check_interval",
							"Interval between tiering rule checks (in seconds).",
							"Set to 0 to disable periodic checks; "
							"the worker will sleep indefinitely until signaled.",
							&tiering_check_interval,
							TIERING_DEFAULT_INTERVAL_S,
							0,
							INT_MAX / 1000,
							PGC_SIGHUP,
							GUC_UNIT_S,
							NULL,
							NULL,
							NULL);

	DefineCustomStringVariable("alohadb.tiering_database",
							   "Database the tiering background worker connects to.",
							   NULL,
							   &tiering_database,
							   "postgres",
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	if (!process_shared_preload_libraries_in_progress)
		return;

	MarkGUCPrefixReserved("alohadb.tiering");

	/* Register the background worker. */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
	snprintf(worker.bgw_library_name, MAXPGPATH, "alohadb_tiering");
	snprintf(worker.bgw_function_name, BGW_MAXLEN,
			 "alohadb_tiering_worker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "alohadb_tiering worker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "alohadb_tiering");
	worker.bgw_main_arg = (Datum) 0;
	worker.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&worker);
}

/* ----------------------------------------------------------------
 * alohadb_tiering_check_now
 *
 * SQL-callable function to trigger an immediate tiering check
 * from the current backend (not the background worker).
 * ---------------------------------------------------------------- */
Datum
alohadb_tiering_check_now(PG_FUNCTION_ARGS)
{
	tiering_perform_check();
	PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * alohadb_tiering_status
 *
 * SQL-callable function returning one row per tracked rule with
 * the number of partitions moved and the last check timestamp.
 * ---------------------------------------------------------------- */
Datum
alohadb_tiering_status(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcxt;
	int			i;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context "
						"that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not "
						"allowed in this context")));

	/* Build tupdesc for result set */
	tupdesc = CreateTemplateTupleDesc(4);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "rule_id",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "parent_table",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "partitions_moved",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "last_check",
					   TIMESTAMPTZOID, -1, 0);

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcxt = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = BlessTupleDesc(tupdesc);

	MemoryContextSwitchTo(oldcxt);

	for (i = 0; i < num_rule_status; i++)
	{
		Datum		values[4];
		bool		nulls[4];

		memset(nulls, 0, sizeof(nulls));

		values[0] = Int32GetDatum(rule_status[i].rule_id);
		values[1] = CStringGetTextDatum(rule_status[i].parent_table);
		values[2] = Int32GetDatum(rule_status[i].partitions_moved);

		if (rule_status[i].last_check == 0)
			nulls[3] = true;
		else
			values[3] = TimestampTzGetDatum(rule_status[i].last_check);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	return (Datum) 0;
}
