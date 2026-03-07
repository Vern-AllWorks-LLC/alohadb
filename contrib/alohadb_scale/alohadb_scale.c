/*-------------------------------------------------------------------------
 *
 * alohadb_scale.c
 *	  AlohaDB Scale - scale-to-zero management extension.
 *
 *	  Provides a background worker that monitors database connection
 *	  activity and can auto-suspend the database when idle.  An external
 *	  orchestrator reads the suspend marker file and performs the actual
 *	  shutdown.  SQL functions expose status, manual suspend, configuration,
 *	  and activity queries.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_scale/alohadb_scale.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "executor/spi.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/wait_classes.h"
#include "lib/stringinfo.h"

#include <unistd.h>

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_scale",
					.version = "1.0"
);

/* SQL-callable function declarations -- must be in same file as implementation */
PG_FUNCTION_INFO_V1(scale_status);
PG_FUNCTION_INFO_V1(scale_suspend);
PG_FUNCTION_INFO_V1(scale_configure);
PG_FUNCTION_INFO_V1(scale_activity);

/* ----------------------------------------------------------------
 * Shared memory state
 * ---------------------------------------------------------------- */

typedef struct ScaleSharedState
{
	LWLock	   *lock;
	bool		auto_suspend_enabled;
	int			suspend_after_secs;
	int			min_connections;
	TimestampTz last_activity;
	int			current_connections;
	int			active_connections;
	bool		suspended;
} ScaleSharedState;

static ScaleSharedState *scale_shared = NULL;

/* Hook save pointers for chaining */
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* ----------------------------------------------------------------
 * GUC variables
 * ---------------------------------------------------------------- */

static int	scale_suspend_after_guc = 3600;		/* seconds, default 1 hour */
static int	scale_min_connections_guc = 0;
static bool scale_auto_suspend_guc = false;
static char *scale_suspend_command_guc = NULL;

/* Marker file path for external orchestrators */
#define SUSPEND_MARKER_FILE "/tmp/alohadb_suspended"

/* ----------------------------------------------------------------
 * scale_shmem_size
 * ---------------------------------------------------------------- */
static Size
scale_shmem_size(void)
{
	return MAXALIGN(sizeof(ScaleSharedState));
}

/* ----------------------------------------------------------------
 * scale_shmem_request
 * ---------------------------------------------------------------- */
static void
scale_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(scale_shmem_size());
	RequestNamedLWLockTranche("alohadb_scale", 1);
}

/* ----------------------------------------------------------------
 * scale_shmem_startup
 * ---------------------------------------------------------------- */
static void
scale_shmem_startup(void)
{
	bool	found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	scale_shared = ShmemInitStruct("alohadb_scale",
								   scale_shmem_size(),
								   &found);

	if (!found)
	{
		memset(scale_shared, 0, sizeof(ScaleSharedState));
		scale_shared->lock =
			&(GetNamedLWLockTranche("alohadb_scale"))->lock;
		scale_shared->auto_suspend_enabled = scale_auto_suspend_guc;
		scale_shared->suspend_after_secs = scale_suspend_after_guc;
		scale_shared->min_connections = scale_min_connections_guc;
		scale_shared->last_activity = GetCurrentTimestamp();
		scale_shared->current_connections = 0;
		scale_shared->active_connections = 0;
		scale_shared->suspended = false;
	}

	LWLockRelease(AddinShmemInitLock);
}

/* ----------------------------------------------------------------
 * scale_main -- background worker entry point
 * ---------------------------------------------------------------- */
PGDLLEXPORT void scale_main(Datum main_arg);

void
scale_main(Datum main_arg)
{
	/* Set up signal handlers */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);

	BackgroundWorkerUnblockSignals();

	/* Connect to the default database for SPI queries */
	BackgroundWorkerInitializeConnection("postgres", NULL, 0);

	elog(LOG, "alohadb_scale: background worker started "
		 "(suspend_after=%ds, auto_suspend=%s)",
		 scale_suspend_after_guc,
		 scale_auto_suspend_guc ? "on" : "off");

	for (;;)
	{
		int		rc;
		int		total_conns = 0;
		int		active_conns = 0;
		TimestampTz last_active = 0;
		int		ret;

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   10000L,		/* 10 second timeout */
					   PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		CHECK_FOR_INTERRUPTS();

		/* Handle SIGHUP */
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);

			/* Push reloaded GUC values into shared memory */
			if (scale_shared)
			{
				LWLockAcquire(scale_shared->lock, LW_EXCLUSIVE);
				scale_shared->auto_suspend_enabled = scale_auto_suspend_guc;
				scale_shared->suspend_after_secs = scale_suspend_after_guc;
				scale_shared->min_connections = scale_min_connections_guc;
				LWLockRelease(scale_shared->lock);
			}

			elog(LOG, "alohadb_scale: configuration reloaded");
		}

		/*
		 * Query pg_stat_activity for connection counts and last activity.
		 */
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());

		ret = SPI_execute(
			"SELECT count(*)::int, "
			"count(*) FILTER (WHERE state = 'active')::int, "
			"max(CASE WHEN state != 'idle' THEN state_change END) "
			"FROM pg_stat_activity "
			"WHERE backend_type = 'client backend'",
			true, 0);

		if (ret == SPI_OK_SELECT && SPI_processed > 0)
		{
			bool	isnull;

			total_conns = DatumGetInt32(
				SPI_getbinval(SPI_tuptable->vals[0],
							  SPI_tuptable->tupdesc, 1, &isnull));
			if (isnull)
				total_conns = 0;

			active_conns = DatumGetInt32(
				SPI_getbinval(SPI_tuptable->vals[0],
							  SPI_tuptable->tupdesc, 2, &isnull));
			if (isnull)
				active_conns = 0;

			if (!isnull)
			{
				Datum	d = SPI_getbinval(SPI_tuptable->vals[0],
										  SPI_tuptable->tupdesc, 3, &isnull);
				if (!isnull)
					last_active = DatumGetTimestampTz(d);
			}
		}

		PopActiveSnapshot();
		SPI_finish();
		CommitTransactionCommand();

		/* Update shared memory */
		if (scale_shared)
		{
			LWLockAcquire(scale_shared->lock, LW_EXCLUSIVE);
			scale_shared->current_connections = total_conns;
			scale_shared->active_connections = active_conns;
			if (last_active != 0)
				scale_shared->last_activity = last_active;
			else if (active_conns > 0)
				scale_shared->last_activity = GetCurrentTimestamp();

			/*
			 * Check auto-suspend conditions: auto_suspend enabled, not
			 * already suspended, idle long enough, few enough connections.
			 */
			if (scale_shared->auto_suspend_enabled &&
				!scale_shared->suspended &&
				total_conns <= scale_shared->min_connections)
			{
				TimestampTz now = GetCurrentTimestamp();
				long		idle_secs;
				int			idle_usecs;

				TimestampDifference(scale_shared->last_activity, now,
									&idle_secs, &idle_usecs);

				if (idle_secs >= scale_shared->suspend_after_secs)
				{
					scale_shared->suspended = true;
					LWLockRelease(scale_shared->lock);

					elog(LOG, "alohadb_scale: auto-suspending database "
						 "(idle for %ld seconds, connections=%d)",
						 idle_secs, total_conns);

					/* Write marker file for external orchestrator */
					{
						FILE   *fp = fopen(SUSPEND_MARKER_FILE, "w");
						if (fp)
						{
							fprintf(fp, "suspended\n");
							fclose(fp);
						}
						else
							elog(WARNING,
								 "alohadb_scale: could not write suspend "
								 "marker file: %m");
					}

					/* Run external suspend command if configured */
					if (scale_suspend_command_guc &&
						scale_suspend_command_guc[0] != '\0')
					{
						int		sysret;

						sysret = system(scale_suspend_command_guc);
						if (sysret != 0)
							elog(WARNING,
								 "alohadb_scale: suspend command returned %d",
								 sysret);
					}

					/* Skip the LWLockRelease below since we already released */
					continue;
				}
			}

			LWLockRelease(scale_shared->lock);
		}
	}
}

/* ----------------------------------------------------------------
 * _PG_init
 * ---------------------------------------------------------------- */
void
_PG_init(void)
{
	BackgroundWorker worker;

	if (!process_shared_preload_libraries_in_progress)
		return;

	/* ---- GUC: alohadb.scale_suspend_after ---- */
	DefineCustomIntVariable("alohadb.scale_suspend_after",
							"Seconds of inactivity before auto-suspend.",
							NULL,
							&scale_suspend_after_guc,
							3600,	/* default: 1 hour */
							10,		/* min: 10 seconds */
							86400,	/* max: 24 hours */
							PGC_SIGHUP,
							GUC_UNIT_S,
							NULL, NULL, NULL);

	/* ---- GUC: alohadb.scale_min_connections ---- */
	DefineCustomIntVariable("alohadb.scale_min_connections",
							"Minimum connections threshold for auto-suspend.",
							"Auto-suspend triggers only when connections are "
							"at or below this value.",
							&scale_min_connections_guc,
							0,		/* default */
							0,		/* min */
							1000,	/* max */
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	/* ---- GUC: alohadb.scale_auto_suspend ---- */
	DefineCustomBoolVariable("alohadb.scale_auto_suspend",
							 "Enable automatic database suspension on idle.",
							 NULL,
							 &scale_auto_suspend_guc,
							 false,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	/* ---- GUC: alohadb.scale_suspend_command ---- */
	DefineCustomStringVariable("alohadb.scale_suspend_command",
							   "External command to run when suspending.",
							   "If empty, only the marker file is written.",
							   &scale_suspend_command_guc,
							   "",
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	MarkGUCPrefixReserved("alohadb.scale");

	/* ---- Shared memory hooks ---- */
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = scale_shmem_request;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = scale_shmem_startup;

	/* ---- Register background worker ---- */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
	snprintf(worker.bgw_library_name, MAXPGPATH, "alohadb_scale");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "scale_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "alohadb_scale worker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "alohadb_scale");
	worker.bgw_main_arg = (Datum) 0;
	worker.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&worker);
}

/* ----------------------------------------------------------------
 * scale_status()
 *
 * Returns key-value pairs of current scale settings and state.
 * ---------------------------------------------------------------- */
Datum
scale_status(PG_FUNCTION_ARGS)
{
	ReturnSetInfo  *rsinfo;
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
	rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	tupstore = rsinfo->setResult;
	tupdesc = rsinfo->setDesc;

	if (!scale_shared)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("alohadb_scale shared memory not initialized"),
				 errhint("Add alohadb_scale to shared_preload_libraries.")));
	}

	LWLockAcquire(scale_shared->lock, LW_SHARED);

	{
		Datum		values[2];
		bool		nulls[2] = {false, false};
		char		buf[128];

		/* auto_suspend_enabled */
		values[0] = CStringGetTextDatum("auto_suspend_enabled");
		values[1] = CStringGetTextDatum(
			scale_shared->auto_suspend_enabled ? "true" : "false");
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);

		/* suspend_after_secs */
		values[0] = CStringGetTextDatum("suspend_after_secs");
		snprintf(buf, sizeof(buf), "%d", scale_shared->suspend_after_secs);
		values[1] = CStringGetTextDatum(buf);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);

		/* min_connections */
		values[0] = CStringGetTextDatum("min_connections");
		snprintf(buf, sizeof(buf), "%d", scale_shared->min_connections);
		values[1] = CStringGetTextDatum(buf);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);

		/* current_connections */
		values[0] = CStringGetTextDatum("current_connections");
		snprintf(buf, sizeof(buf), "%d", scale_shared->current_connections);
		values[1] = CStringGetTextDatum(buf);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);

		/* active_connections */
		values[0] = CStringGetTextDatum("active_connections");
		snprintf(buf, sizeof(buf), "%d", scale_shared->active_connections);
		values[1] = CStringGetTextDatum(buf);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);

		/* idle_seconds */
		{
			TimestampTz now = GetCurrentTimestamp();
			long		idle_secs;
			int			idle_usecs;

			TimestampDifference(scale_shared->last_activity, now,
								&idle_secs, &idle_usecs);

			values[0] = CStringGetTextDatum("idle_seconds");
			snprintf(buf, sizeof(buf), "%ld.%d", idle_secs,
					 idle_usecs / 1000);
			values[1] = CStringGetTextDatum(buf);
			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}

		/* last_activity */
		values[0] = CStringGetTextDatum("last_activity");
		if (scale_shared->last_activity != 0)
		{
			char *ts_str = DatumGetCString(
				DirectFunctionCall1(timestamptz_out,
					TimestampTzGetDatum(scale_shared->last_activity)));
			values[1] = CStringGetTextDatum(ts_str);
		}
		else
		{
			values[1] = CStringGetTextDatum("never");
		}
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);

		/* suspended */
		values[0] = CStringGetTextDatum("suspended");
		values[1] = CStringGetTextDatum(
			scale_shared->suspended ? "true" : "false");
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	LWLockRelease(scale_shared->lock);

	return (Datum) 0;
}

/* ----------------------------------------------------------------
 * scale_suspend()
 *
 * Manually trigger a suspend.  Writes marker file and runs the
 * external suspend command if configured.  Returns a status string.
 * ---------------------------------------------------------------- */
Datum
scale_suspend(PG_FUNCTION_ARGS)
{
	StringInfoData result;
	FILE	   *fp;

	if (!scale_shared)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("alohadb_scale shared memory not initialized"),
				 errhint("Add alohadb_scale to shared_preload_libraries.")));
	}

	initStringInfo(&result);

	/* Write marker file */
	fp = fopen(SUSPEND_MARKER_FILE, "w");
	if (fp)
	{
		fprintf(fp, "suspended\n");
		fclose(fp);
		appendStringInfo(&result, "Marker file written to %s. ",
						 SUSPEND_MARKER_FILE);
	}
	else
	{
		appendStringInfo(&result, "WARNING: Could not write marker file. ");
	}

	/* Mark as suspended in shared memory */
	LWLockAcquire(scale_shared->lock, LW_EXCLUSIVE);
	scale_shared->suspended = true;
	LWLockRelease(scale_shared->lock);

	/* Run external suspend command if configured */
	if (scale_suspend_command_guc &&
		scale_suspend_command_guc[0] != '\0')
	{
		int		sysret;

		sysret = system(scale_suspend_command_guc);
		if (sysret == 0)
			appendStringInfo(&result, "Suspend command executed successfully.");
		else
			appendStringInfo(&result,
							 "Suspend command returned exit code %d.",
							 sysret);
	}
	else
	{
		appendStringInfo(&result,
						 "No suspend command configured; "
						 "database is marked for external orchestrator.");
	}

	PG_RETURN_TEXT_P(cstring_to_text(result.data));
}

/* ----------------------------------------------------------------
 * scale_configure(suspend_after interval, min_connections int)
 *
 * Update shared memory state.  NULL parameters are skipped.
 * Note: NOT marked STRICT so NULLs are passed through.
 * ---------------------------------------------------------------- */
Datum
scale_configure(PG_FUNCTION_ARGS)
{
	if (!scale_shared)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("alohadb_scale shared memory not initialized"),
				 errhint("Add alohadb_scale to shared_preload_libraries.")));
	}

	LWLockAcquire(scale_shared->lock, LW_EXCLUSIVE);

	/* suspend_after interval -> convert to seconds */
	if (!PG_ARGISNULL(0))
	{
		Interval   *iv = PG_GETARG_INTERVAL_P(0);
		int64		total_secs;

		/*
		 * Convert interval to total seconds.
		 * Interval has months, days, and microseconds.
		 */
		total_secs = (int64) iv->month * 30 * 86400 +
					 (int64) iv->day * 86400 +
					 iv->time / 1000000;

		if (total_secs < 10)
		{
			LWLockRelease(scale_shared->lock);
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("suspend_after must be at least 10 seconds")));
		}
		if (total_secs > 86400)
		{
			LWLockRelease(scale_shared->lock);
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("suspend_after must be at most 24 hours")));
		}

		scale_shared->suspend_after_secs = (int) total_secs;
	}

	/* min_connections int */
	if (!PG_ARGISNULL(1))
	{
		int		min_conns = PG_GETARG_INT32(1);

		if (min_conns < 0)
		{
			LWLockRelease(scale_shared->lock);
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("min_connections must be non-negative")));
		}

		scale_shared->min_connections = min_conns;
	}

	LWLockRelease(scale_shared->lock);

	PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * scale_activity()
 *
 * Query pg_stat_activity via SPI and return connection breakdown.
 * ---------------------------------------------------------------- */
Datum
scale_activity(PG_FUNCTION_ARGS)
{
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	int				ret;
	int				total = 0;
	int				active = 0;
	int				idle = 0;
	float8			idle_secs = 0.0;
	TimestampTz		last_act = 0;
	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
	rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	tupstore = rsinfo->setResult;
	tupdesc = rsinfo->setDesc;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(
		"SELECT "
		"count(*)::int AS total, "
		"count(*) FILTER (WHERE state = 'active')::int AS active, "
		"count(*) FILTER (WHERE state = 'idle')::int AS idle, "
		"EXTRACT(EPOCH FROM (now() - max(CASE WHEN state != 'idle' "
		"THEN state_change END)))::float8 AS idle_seconds, "
		"max(CASE WHEN state != 'idle' THEN state_change END) "
		"AS last_activity "
		"FROM pg_stat_activity "
		"WHERE backend_type = 'client backend'",
		true, 0);

	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		bool	isnull;

		total = DatumGetInt32(
			SPI_getbinval(SPI_tuptable->vals[0],
						  SPI_tuptable->tupdesc, 1, &isnull));
		if (isnull) total = 0;

		active = DatumGetInt32(
			SPI_getbinval(SPI_tuptable->vals[0],
						  SPI_tuptable->tupdesc, 2, &isnull));
		if (isnull) active = 0;

		idle = DatumGetInt32(
			SPI_getbinval(SPI_tuptable->vals[0],
						  SPI_tuptable->tupdesc, 3, &isnull));
		if (isnull) idle = 0;

		{
			Datum	d = SPI_getbinval(SPI_tuptable->vals[0],
									  SPI_tuptable->tupdesc, 4, &isnull);
			if (!isnull)
				idle_secs = DatumGetFloat8(d);
		}

		{
			Datum	d = SPI_getbinval(SPI_tuptable->vals[0],
									  SPI_tuptable->tupdesc, 5, &isnull);
			if (!isnull)
				last_act = DatumGetTimestampTz(d);
		}
	}

	PopActiveSnapshot();
	SPI_finish();

	/* Build result row */
	{
		Datum		values[5];
		bool		nulls[5] = {false, false, false, false, false};

		values[0] = Int32GetDatum(total);
		values[1] = Int32GetDatum(active);
		values[2] = Int32GetDatum(idle);
		values[3] = Float8GetDatum(idle_secs);

		if (last_act != 0)
			values[4] = TimestampTzGetDatum(last_act);
		else
			nulls[4] = true;

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	return (Datum) 0;
}
