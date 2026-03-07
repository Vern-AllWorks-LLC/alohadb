/*-------------------------------------------------------------------------
 *
 * alohadb_cron.c
 *	  Main entry point for the alohadb_cron extension.
 *
 *	  Implements job scheduling with standard cron syntax.  A background
 *	  worker periodically scans the alohadb_cron_jobs table, parses
 *	  each job's cron schedule, and executes due jobs via SPI.
 *	  Results are logged to alohadb_cron_job_run_details.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_cron/alohadb_cron.c
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

#include "alohadb_cron.h"

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_cron",
					.version = "1.0"
);

/* SQL-callable function declarations */
PG_FUNCTION_INFO_V1(cron_schedule);
PG_FUNCTION_INFO_V1(cron_schedule_named);
PG_FUNCTION_INFO_V1(cron_unschedule);
PG_FUNCTION_INFO_V1(cron_unschedule_named);
PG_FUNCTION_INFO_V1(cron_job_status);

/* ----------------------------------------------------------------
 * GUC variables
 * ---------------------------------------------------------------- */

/* Check interval in seconds; default 60 (1 minute) */
static int	cron_check_interval = CRON_DEFAULT_CHECK_INTERVAL;

/* Database to connect to */
static char *cron_database = NULL;

/* Custom wait event for WaitLatch */
static uint32 cron_wait_event = 0;

/* ----------------------------------------------------------------
 * Forward declarations
 * ---------------------------------------------------------------- */

static void cron_check_jobs(void);
static bool cron_table_exists(void);
static void cron_execute_job(int jobid, const char *command,
							 const char *database, const char *username);

/* ----------------------------------------------------------------
 * cron_table_exists
 *
 * Check whether the alohadb_cron_jobs table has been created
 * (i.e. CREATE EXTENSION alohadb_cron has been run).
 * Must be called within an active SPI session and transaction.
 * ---------------------------------------------------------------- */
static bool
cron_table_exists(void)
{
	int		ret;

	ret = SPI_execute(
		"SELECT 1 FROM pg_catalog.pg_class c "
		"JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
		"WHERE n.nspname = 'public' AND c.relname = 'alohadb_cron_jobs'",
		true, 1);

	return (ret == SPI_OK_SELECT && SPI_processed > 0);
}

/* ----------------------------------------------------------------
 * cron_execute_job
 *
 * Execute a single job command via SPI within a PG_TRY/PG_CATCH
 * block, recording the result in alohadb_cron_job_run_details.
 *
 * This is called while already inside an SPI session and
 * transaction (from cron_check_jobs).
 * ---------------------------------------------------------------- */
static void
cron_execute_job(int jobid, const char *command,
				 const char *database, const char *username)
{
	StringInfoData	insert_sql;
	TimestampTz		start_time;
	TimestampTz		end_time;
	const char	   *status;
	const char	   *return_message;

	start_time = GetCurrentTimestamp();
	status = "succeeded";
	return_message = "OK";

	PG_TRY();
	{
		int		ret;

		pgstat_report_activity(STATE_RUNNING, command);
		ret = SPI_execute(command, false, 0);

		if (ret < 0)
		{
			status = "failed";
			return_message = "SPI_execute returned error";
		}
		else
		{
			/*
			 * Build a return message with the number of rows processed.
			 */
			StringInfoData msg;

			initStringInfo(&msg);
			appendStringInfo(&msg, "OK: %" PRIu64 " row(s) affected",
							 SPI_processed);
			return_message = msg.data;
		}
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		/* Save error info */
		MemoryContextSwitchTo(CurTransactionContext);
		edata = CopyErrorData();
		FlushErrorState();

		status = "failed";
		return_message = edata->message ? edata->message : "unknown error";

		elog(WARNING, "alohadb_cron: job %d failed: %s", jobid, return_message);
	}
	PG_END_TRY();

	end_time = GetCurrentTimestamp();

	/*
	 * Record the run details.  We use SPI_execute_with_args to safely
	 * pass parameter values.
	 */
	initStringInfo(&insert_sql);
	appendStringInfo(&insert_sql,
		"INSERT INTO public.alohadb_cron_job_run_details "
		"(jobid, status, return_message, start_time, end_time) "
		"VALUES (%d, %s, %s, %s, %s)",
		jobid,
		quote_literal_cstr(status),
		quote_literal_cstr(return_message),
		quote_literal_cstr(timestamptz_to_str(start_time)),
		quote_literal_cstr(timestamptz_to_str(end_time)));

	SPI_execute(insert_sql.data, false, 0);

	pfree(insert_sql.data);
}

/* ----------------------------------------------------------------
 * cron_check_jobs
 *
 * Execute one full scan of the alohadb_cron_jobs table.
 * For each active job whose schedule matches, execute it.
 * ---------------------------------------------------------------- */
static void
cron_check_jobs(void)
{
	int				ret;
	uint64			njobs;
	uint64			i;
	TimestampTz		now;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "alohadb_cron: checking jobs");

	/* Check whether the jobs table exists */
	if (!cron_table_exists())
	{
		PopActiveSnapshot();
		SPI_finish();
		CommitTransactionCommand();
		pgstat_report_activity(STATE_IDLE, NULL);
		return;
	}

	/* Fetch all active jobs */
	ret = SPI_execute(
		"SELECT jobid, schedule, command, database, username "
		"FROM public.alohadb_cron_jobs "
		"WHERE active = true "
		"ORDER BY jobid",
		true, 0);

	if (ret != SPI_OK_SELECT)
	{
		elog(WARNING, "alohadb_cron: failed to read jobs table: error code %d",
			 ret);
		PopActiveSnapshot();
		SPI_finish();
		CommitTransactionCommand();
		pgstat_report_activity(STATE_IDLE, NULL);
		return;
	}

	njobs = SPI_processed;
	now = GetCurrentTimestamp();

	if (njobs > 0)
	{
		/*
		 * Copy job data out of SPI memory before executing individual
		 * jobs (which will run their own SPI queries).
		 */
		typedef struct JobCopy
		{
			int			jobid;
			char		schedule[256];
			char		command[8192];
			char		database[NAMEDATALEN];
			char		username[NAMEDATALEN];
		} JobCopy;

		JobCopy	   *jobs;
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(CurTransactionContext);
		jobs = palloc(sizeof(JobCopy) * njobs);
		MemoryContextSwitchTo(oldcxt);

		for (i = 0; i < njobs; i++)
		{
			bool	isnull;
			char   *val;

			jobs[i].jobid = DatumGetInt32(
				SPI_getbinval(SPI_tuptable->vals[i],
							  SPI_tuptable->tupdesc, 1, &isnull));

			val = SPI_getvalue(SPI_tuptable->vals[i],
							   SPI_tuptable->tupdesc, 2);
			strlcpy(jobs[i].schedule, val ? val : "", sizeof(jobs[i].schedule));

			val = SPI_getvalue(SPI_tuptable->vals[i],
							   SPI_tuptable->tupdesc, 3);
			strlcpy(jobs[i].command, val ? val : "", sizeof(jobs[i].command));

			val = SPI_getvalue(SPI_tuptable->vals[i],
							   SPI_tuptable->tupdesc, 4);
			strlcpy(jobs[i].database, val ? val : "", NAMEDATALEN);

			val = SPI_getvalue(SPI_tuptable->vals[i],
							   SPI_tuptable->tupdesc, 5);
			strlcpy(jobs[i].username, val ? val : "", NAMEDATALEN);
		}

		PopActiveSnapshot();
		SPI_finish();
		CommitTransactionCommand();

		/*
		 * Process each job in its own transaction so that a failure
		 * in one job does not abort the others.
		 */
		for (i = 0; i < njobs; i++)
		{
			CronSchedule	sched;
			TimestampTz		next_run;

			/* Parse the cron schedule */
			if (!cron_parse(jobs[i].schedule, &sched))
			{
				elog(WARNING, "alohadb_cron: job %d has invalid schedule: %s",
					 jobs[i].jobid, jobs[i].schedule);
				continue;
			}

			/*
			 * Compute next run time based on (now - check_interval).
			 * A job is due if its next_run time falls within the
			 * current check window, i.e., next_run <= now.
			 *
			 * We compute next_run from (now - check_interval) to find
			 * the earliest possible run time within this check period.
			 */
			next_run = cron_next_run(&sched,
									 now - (int64) cron_check_interval * USECS_PER_SEC);

			if (next_run == DT_NOBEGIN)
				continue;

			/* Job is due if its next_run <= now */
			if (timestamptz_cmp_internal(next_run, now) <= 0)
			{
				SetCurrentStatementStartTimestamp();
				StartTransactionCommand();
				SPI_connect();
				PushActiveSnapshot(GetTransactionSnapshot());

				elog(LOG, "alohadb_cron: executing job %d: %.128s",
					 jobs[i].jobid, jobs[i].command);

				cron_execute_job(jobs[i].jobid, jobs[i].command,
								jobs[i].database, jobs[i].username);

				PopActiveSnapshot();
				SPI_finish();
				CommitTransactionCommand();
			}
		}
	}
	else
	{
		PopActiveSnapshot();
		SPI_finish();
		CommitTransactionCommand();
	}

	pgstat_report_activity(STATE_IDLE, NULL);
}

/* ----------------------------------------------------------------
 * Background worker entry point
 * ---------------------------------------------------------------- */
PGDLLEXPORT void
alohadb_cron_worker_main(Datum main_arg)
{
	/* Establish signal handlers before unblocking signals */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to the configured database */
	BackgroundWorkerInitializeConnection(cron_database, NULL, 0);

	elog(LOG, "alohadb_cron: background worker started, "
		 "check interval = %d s, database = \"%s\"",
		 cron_check_interval,
		 cron_database ? cron_database : "postgres");

	/*
	 * Main loop: periodically check cron jobs until SIGTERM is received.
	 */
	for (;;)
	{
		/* Allocate or fetch the custom wait event on first use */
		if (cron_wait_event == 0)
			cron_wait_event = WaitEventExtensionNew("AlohadbCronMain");

		/*
		 * Sleep for the configured interval, waking on latch set
		 * (SIGHUP, SIGTERM) or postmaster death.
		 */
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 cron_check_interval * 1000L,
						 cron_wait_event);
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		/* Reload configuration on SIGHUP */
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
			elog(LOG, "alohadb_cron: configuration reloaded, "
				 "check interval = %d s", cron_check_interval);
		}

		/* Perform the actual job check */
		cron_check_jobs();

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
	 * Define GUC: alohadb.cron_check_interval
	 */
	DefineCustomIntVariable("alohadb.cron_check_interval",
							"Interval between cron job checks (in seconds).",
							"The background worker sleeps for this duration "
							"between scans of the job table.",
							&cron_check_interval,
							CRON_DEFAULT_CHECK_INTERVAL,
							10,
							3600,
							PGC_SIGHUP,
							GUC_UNIT_S,
							NULL,
							NULL,
							NULL);

	/*
	 * Define GUC: alohadb.cron_database
	 */
	DefineCustomStringVariable("alohadb.cron_database",
							   "Database the cron background worker connects to.",
							   NULL,
							   &cron_database,
							   "postgres",
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	if (!process_shared_preload_libraries_in_progress)
		return;

	MarkGUCPrefixReserved("alohadb.cron");

	/* Register the background worker */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
	snprintf(worker.bgw_library_name, MAXPGPATH, "alohadb_cron");
	snprintf(worker.bgw_function_name, BGW_MAXLEN,
			 "alohadb_cron_worker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "alohadb_cron worker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "alohadb_cron");
	worker.bgw_main_arg = (Datum) 0;
	worker.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&worker);
}

/* ----------------------------------------------------------------
 * SQL-callable functions
 * ---------------------------------------------------------------- */

/*
 * cron_schedule(schedule text, command text) RETURNS int
 *
 * Insert a new cron job and return its jobid.
 */
Datum
cron_schedule(PG_FUNCTION_ARGS)
{
	text	   *schedule_text = PG_GETARG_TEXT_PP(0);
	text	   *command_text = PG_GETARG_TEXT_PP(1);
	char	   *schedule = text_to_cstring(schedule_text);
	char	   *command = text_to_cstring(command_text);
	CronSchedule sched;
	StringInfoData sql;
	int			ret;
	int			jobid;
	bool		isnull;

	/* Validate the schedule expression */
	if (!cron_parse(schedule, &sched))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid cron expression: \"%s\"", schedule)));

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	initStringInfo(&sql);
	appendStringInfo(&sql,
		"INSERT INTO public.alohadb_cron_jobs (schedule, command) "
		"VALUES (%s, %s) RETURNING jobid",
		quote_literal_cstr(schedule),
		quote_literal_cstr(command));

	ret = SPI_execute(sql.data, false, 1);

	if (ret != SPI_OK_INSERT_RETURNING || SPI_processed != 1)
	{
		PopActiveSnapshot();
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_cron: failed to insert job")));
	}

	jobid = DatumGetInt32(
		SPI_getbinval(SPI_tuptable->vals[0],
					  SPI_tuptable->tupdesc, 1, &isnull));

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_INT32(jobid);
}

/*
 * cron_schedule_named(job_name text, schedule text, command text) RETURNS int
 *
 * Insert a named cron job and return its jobid.
 * If a job with the same name exists, update it.
 */
Datum
cron_schedule_named(PG_FUNCTION_ARGS)
{
	text	   *jobname_text = PG_GETARG_TEXT_PP(0);
	text	   *schedule_text = PG_GETARG_TEXT_PP(1);
	text	   *command_text = PG_GETARG_TEXT_PP(2);
	char	   *jobname = text_to_cstring(jobname_text);
	char	   *schedule = text_to_cstring(schedule_text);
	char	   *command = text_to_cstring(command_text);
	CronSchedule sched;
	StringInfoData sql;
	int			ret;
	int			jobid;
	bool		isnull;

	/* Validate the schedule expression */
	if (!cron_parse(schedule, &sched))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid cron expression: \"%s\"", schedule)));

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	initStringInfo(&sql);
	appendStringInfo(&sql,
		"INSERT INTO public.alohadb_cron_jobs (jobname, schedule, command) "
		"VALUES (%s, %s, %s) "
		"ON CONFLICT (jobname) DO UPDATE "
		"SET schedule = EXCLUDED.schedule, "
		"    command = EXCLUDED.command, "
		"    active = true "
		"RETURNING jobid",
		quote_literal_cstr(jobname),
		quote_literal_cstr(schedule),
		quote_literal_cstr(command));

	ret = SPI_execute(sql.data, false, 1);

	if (ret != SPI_OK_INSERT_RETURNING || SPI_processed != 1)
	{
		PopActiveSnapshot();
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_cron: failed to insert/update named job")));
	}

	jobid = DatumGetInt32(
		SPI_getbinval(SPI_tuptable->vals[0],
					  SPI_tuptable->tupdesc, 1, &isnull));

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_INT32(jobid);
}

/*
 * cron_unschedule(job_id int) RETURNS boolean
 *
 * Delete a cron job by jobid.  Returns true if a row was deleted.
 */
Datum
cron_unschedule(PG_FUNCTION_ARGS)
{
	int			jobid = PG_GETARG_INT32(0);
	StringInfoData sql;
	int			ret;
	uint64		nprocessed;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	initStringInfo(&sql);
	appendStringInfo(&sql,
		"DELETE FROM public.alohadb_cron_jobs WHERE jobid = %d",
		jobid);

	ret = SPI_execute(sql.data, false, 0);
	nprocessed = SPI_processed;

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_BOOL(ret == SPI_OK_DELETE && nprocessed > 0);
}

/*
 * cron_unschedule_named(job_name text) RETURNS boolean
 *
 * Delete a cron job by name.  Returns true if a row was deleted.
 */
Datum
cron_unschedule_named(PG_FUNCTION_ARGS)
{
	text	   *jobname_text = PG_GETARG_TEXT_PP(0);
	char	   *jobname = text_to_cstring(jobname_text);
	StringInfoData sql;
	int			ret;
	uint64		nprocessed;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	initStringInfo(&sql);
	appendStringInfo(&sql,
		"DELETE FROM public.alohadb_cron_jobs WHERE jobname = %s",
		quote_literal_cstr(jobname));

	ret = SPI_execute(sql.data, false, 0);
	nprocessed = SPI_processed;

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_BOOL(ret == SPI_OK_DELETE && nprocessed > 0);
}

/*
 * cron_job_status() RETURNS TABLE(...)
 *
 * Return a set of rows describing all scheduled jobs, including
 * the last run status and next computed run time.
 */
Datum
cron_job_status(PG_FUNCTION_ARGS)
{
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext	per_query_ctx;
	MemoryContext	oldcxt;
	int				ret;
	uint64			njobs;
	uint64			i;
	TimestampTz		now;

	/* Check that caller supports us returning a tuplestore */
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
	tupdesc = CreateTemplateTupleDesc(8);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "jobid",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "jobname",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "schedule",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "command",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 5, "active",
					   BOOLOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 6, "last_run",
					   TIMESTAMPTZOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 7, "last_status",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 8, "next_run",
					   TIMESTAMPTZOID, -1, 0);

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcxt = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = BlessTupleDesc(tupdesc);

	MemoryContextSwitchTo(oldcxt);

	/* Query jobs with their last run details */
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(
		"SELECT j.jobid, j.jobname, j.schedule, j.command, j.active, "
		"       d.start_time AS last_run, d.status AS last_status "
		"FROM public.alohadb_cron_jobs j "
		"LEFT JOIN LATERAL ("
		"    SELECT start_time, status "
		"    FROM public.alohadb_cron_job_run_details "
		"    WHERE jobid = j.jobid "
		"    ORDER BY start_time DESC LIMIT 1"
		") d ON true "
		"ORDER BY j.jobid",
		true, 0);

	if (ret != SPI_OK_SELECT)
	{
		PopActiveSnapshot();
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_cron: failed to query job status")));
	}

	njobs = SPI_processed;
	now = GetCurrentTimestamp();

	for (i = 0; i < njobs; i++)
	{
		Datum		values[8];
		bool		nulls[8];
		bool		isnull;
		char	   *val;

		memset(nulls, 0, sizeof(nulls));

		/* jobid */
		values[0] = SPI_getbinval(SPI_tuptable->vals[i],
								  SPI_tuptable->tupdesc, 1, &isnull);
		nulls[0] = isnull;

		/* jobname */
		val = SPI_getvalue(SPI_tuptable->vals[i],
						   SPI_tuptable->tupdesc, 2);
		if (val)
			values[1] = CStringGetTextDatum(val);
		else
			nulls[1] = true;

		/* schedule */
		val = SPI_getvalue(SPI_tuptable->vals[i],
						   SPI_tuptable->tupdesc, 3);
		if (val)
			values[2] = CStringGetTextDatum(val);
		else
			nulls[2] = true;

		/* command */
		val = SPI_getvalue(SPI_tuptable->vals[i],
						   SPI_tuptable->tupdesc, 4);
		if (val)
			values[3] = CStringGetTextDatum(val);
		else
			nulls[3] = true;

		/* active */
		values[4] = SPI_getbinval(SPI_tuptable->vals[i],
								  SPI_tuptable->tupdesc, 5, &isnull);
		nulls[4] = isnull;

		/* last_run */
		values[5] = SPI_getbinval(SPI_tuptable->vals[i],
								  SPI_tuptable->tupdesc, 6, &isnull);
		nulls[5] = isnull;

		/* last_status */
		val = SPI_getvalue(SPI_tuptable->vals[i],
						   SPI_tuptable->tupdesc, 7);
		if (val)
			values[6] = CStringGetTextDatum(val);
		else
			nulls[6] = true;

		/* next_run: compute from C using the cron parser */
		{
			CronSchedule	sched;
			char		   *schedule_str;

			schedule_str = SPI_getvalue(SPI_tuptable->vals[i],
										SPI_tuptable->tupdesc, 3);

			if (schedule_str && cron_parse(schedule_str, &sched))
			{
				TimestampTz	next = cron_next_run(&sched, now);

				if (next != DT_NOBEGIN)
					values[7] = TimestampTzGetDatum(next);
				else
					nulls[7] = true;
			}
			else
			{
				nulls[7] = true;
			}
		}

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	PopActiveSnapshot();
	SPI_finish();

	return (Datum) 0;
}
