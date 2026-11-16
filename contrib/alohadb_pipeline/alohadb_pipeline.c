/*-------------------------------------------------------------------------
 *
 * alohadb_pipeline.c
 *	  DAG pipeline orchestration background worker.
 *
 *	  A background worker polls pipeline_task_runs for ready tasks
 *	  and dispatches them via SPI.  Dependency resolution uses
 *	  Kahn's topological-sort algorithm (1962), which is prior art
 *	  well outside any patent window.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_pipeline/alohadb_pipeline.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/latch.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "tcop/tcopprot.h"

#include "alohadb_pipeline.h"

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_pipeline",
					.version = "1.0"
);

/* GUC variables */
int			pipeline_check_interval = 5;
int			pipeline_max_parallel = 4;
char	   *pipeline_database = NULL;

static uint32 pipeline_wait_event = 0;

/* ----------------------------------------------------------------
 * pipeline_table_exists
 * ---------------------------------------------------------------- */
static bool
pipeline_table_exists(void)
{
	int		ret;

	ret = SPI_execute(
		"SELECT 1 FROM pg_catalog.pg_class c "
		"JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
		"WHERE n.nspname = 'public' AND c.relname = 'pipeline_task_runs'",
		true, 1);
	return (ret == SPI_OK_SELECT && SPI_processed > 0);
}

/* ----------------------------------------------------------------
 * pipeline_advance
 *
 * Mark newly-eligible task_run rows as 'ready' (all dependencies
 * completed) and execute up to pipeline_max_parallel 'ready' tasks.
 * Each task is run in its own sub-transaction.
 * ---------------------------------------------------------------- */
static void
pipeline_advance(void)
{
	int		ret;
	uint64	n;
	uint64	i;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "alohadb_pipeline: advancing tasks");

	if (!pipeline_table_exists())
		goto done;

	/*
	 * Step 1: promote waiting tasks whose every dependency has
	 * status='completed'.  This is Kahn's in-degree-zeroing step.
	 */
	ret = SPI_execute(
		"UPDATE public.pipeline_task_runs tr "
		"SET    status = 'ready', started_at = NULL "
		"WHERE  tr.status = 'waiting' "
		"  AND  NOT EXISTS ("
		"    SELECT 1 FROM public.pipeline_task_deps d "
		"    JOIN   public.pipeline_task_runs dr "
		"           ON  dr.run_id  = tr.run_id "
		"           AND dr.task_id = d.dep_task_id "
		"    WHERE  d.task_id = tr.task_id "
		"      AND  dr.status <> 'completed'"
		"  )",
		false, 0);

	if (ret < 0)
		elog(WARNING, "alohadb_pipeline: failed to promote waiting tasks");

	/*
	 * Step 2: dispatch up to max_parallel 'ready' tasks.
	 */
	{
		StringInfoData sql;

		initStringInfo(&sql);
		appendStringInfo(&sql,
			"SELECT tr.task_run_id, t.task_name, t.command "
			"FROM   public.pipeline_task_runs tr "
			"JOIN   public.pipeline_tasks t ON t.task_id = tr.task_id "
			"WHERE  tr.status = 'ready' "
			"ORDER BY tr.task_run_id "
			"LIMIT  %d "
			"FOR UPDATE SKIP LOCKED",
			pipeline_max_parallel);

		ret = SPI_execute(sql.data, false, pipeline_max_parallel);
		pfree(sql.data);
	}

	if (ret != SPI_OK_SELECT)
		goto done;

	n = SPI_processed;
	if (n == 0)
		goto done;

	/* Copy task data before closing the outer SPI query */
	{
		typedef struct TaskRec
		{
			int64	task_run_id;
			char	task_name[256];
			char	command[8192];
		} TaskRec;

		TaskRec    *tasks;
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(CurTransactionContext);
		tasks = palloc(sizeof(TaskRec) * n);
		MemoryContextSwitchTo(oldcxt);

		for (i = 0; i < n; i++)
		{
			bool	isnull;
			char   *val;

			tasks[i].task_run_id = DatumGetInt64(
				SPI_getbinval(SPI_tuptable->vals[i],
							  SPI_tuptable->tupdesc, 1, &isnull));

			val = SPI_getvalue(SPI_tuptable->vals[i],
							   SPI_tuptable->tupdesc, 2);
			strlcpy(tasks[i].task_name, val ? val : "", sizeof(tasks[i].task_name));

			val = SPI_getvalue(SPI_tuptable->vals[i],
							   SPI_tuptable->tupdesc, 3);
			strlcpy(tasks[i].command, val ? val : "", sizeof(tasks[i].command));
		}

		PopActiveSnapshot();
		SPI_finish();
		CommitTransactionCommand();

		/* Run each task in its own transaction */
		for (i = 0; i < n; i++)
		{
			const char *status = "completed";
			const char *errmsg_str = NULL;

			SetCurrentStatementStartTimestamp();
			StartTransactionCommand();
			SPI_connect();
			PushActiveSnapshot(GetTransactionSnapshot());

			/* Mark as running */
			{
				StringInfoData sql;
				initStringInfo(&sql);
				appendStringInfo(&sql,
					"UPDATE public.pipeline_task_runs "
					"SET status = 'running', started_at = now() "
					"WHERE task_run_id = " INT64_FORMAT,
					tasks[i].task_run_id);
				SPI_execute(sql.data, false, 0);
				pfree(sql.data);
			}

			PG_TRY();
			{
				pgstat_report_activity(STATE_RUNNING, tasks[i].command);
				ret = SPI_execute(tasks[i].command, false, 0);
				if (ret < 0)
				{
					status = "failed";
					errmsg_str = "SPI_execute returned error";
				}
			}
			PG_CATCH();
			{
				ErrorData  *edata;
				MemoryContextSwitchTo(CurTransactionContext);
				edata = CopyErrorData();
				FlushErrorState();
				status = "failed";
				errmsg_str = edata->message ? edata->message : "unknown error";
				elog(WARNING, "alohadb_pipeline: task \"%s\" failed: %s",
					 tasks[i].task_name, errmsg_str);
			}
			PG_END_TRY();

			/* Record result */
			{
				StringInfoData sql;
				initStringInfo(&sql);
				appendStringInfo(&sql,
					"UPDATE public.pipeline_task_runs "
					"SET status = %s, finished_at = now(), "
					"    error_message = %s "
					"WHERE task_run_id = " INT64_FORMAT,
					quote_literal_cstr(status),
					errmsg_str ? quote_literal_cstr(errmsg_str) : "NULL",
					tasks[i].task_run_id);
				SPI_execute(sql.data, false, 0);
				pfree(sql.data);
			}

			PopActiveSnapshot();
			SPI_finish();
			CommitTransactionCommand();
		}

		pgstat_report_activity(STATE_IDLE, NULL);
		return;
	}

done:
	PopActiveSnapshot();
	SPI_finish();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
}

/* ----------------------------------------------------------------
 * Background worker entry point
 * ---------------------------------------------------------------- */
PGDLLEXPORT void
alohadb_pipeline_worker_main(Datum main_arg)
{
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection(
		pipeline_database ? pipeline_database : "postgres", NULL, 0);

	elog(LOG, "alohadb_pipeline: background worker started, "
		 "interval=%ds, max_parallel=%d, database=\"%s\"",
		 pipeline_check_interval, pipeline_max_parallel,
		 pipeline_database ? pipeline_database : "postgres");

	for (;;)
	{
		if (pipeline_wait_event == 0)
			pipeline_wait_event = WaitEventExtensionNew("AlohadbPipelineMain");

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 (long) pipeline_check_interval * 1000L,
						 pipeline_wait_event);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		pipeline_advance();
		pgstat_report_stat(true);
	}
}

/* ----------------------------------------------------------------
 * _PG_init
 * ---------------------------------------------------------------- */
void
_PG_init(void)
{
	BackgroundWorker worker;

	DefineCustomIntVariable("alohadb.pipeline_check_interval",
							"Interval between pipeline task polls (seconds).",
							NULL,
							&pipeline_check_interval,
							5, 1, 3600,
							PGC_SIGHUP, GUC_UNIT_S,
							NULL, NULL, NULL);

	DefineCustomIntVariable("alohadb.pipeline_max_parallel",
							"Maximum number of pipeline tasks to run in parallel.",
							NULL,
							&pipeline_max_parallel,
							4, 1, 64,
							PGC_SIGHUP, 0,
							NULL, NULL, NULL);

	DefineCustomStringVariable("alohadb.pipeline_database",
							   "Database the pipeline background worker connects to.",
							   NULL,
							   &pipeline_database,
							   "postgres",
							   PGC_SIGHUP, 0,
							   NULL, NULL, NULL);

	if (!process_shared_preload_libraries_in_progress)
		return;

	MarkGUCPrefixReserved("alohadb.pipeline");

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
	snprintf(worker.bgw_library_name, MAXPGPATH, "alohadb_pipeline");
	snprintf(worker.bgw_function_name, BGW_MAXLEN,
			 "alohadb_pipeline_worker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "alohadb_pipeline worker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "alohadb_pipeline");
	worker.bgw_main_arg = (Datum) 0;
	worker.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&worker);
}
