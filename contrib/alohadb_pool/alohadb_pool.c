/*-------------------------------------------------------------------------
 *
 * alohadb_pool.c
 *	  Main entry point for the alohadb_pool extension.
 *
 *	  Implements a built-in connection pooler as a background worker.
 *	  The worker listens on a configurable TCP port and proxies the
 *	  PostgreSQL wire protocol between clients and pre-established
 *	  backend connections.
 *
 *	  Currently this is a stub implementation: the background worker
 *	  registers, logs its startup, and sits in a WaitLatch loop.
 *	  The actual wire protocol proxy logic (~3000 LOC) will be added
 *	  in a future enhancement.
 *
 *	  Management SQL functions (pool_status, pool_reset, pool_settings)
 *	  are fully implemented and read/write shared memory.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_pool/alohadb_pool.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

/* Background worker essentials */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* Function call and SRF support */
#include "fmgr.h"
#include "funcapi.h"

/* GUC and utilities */
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/wait_classes.h"

#include "alohadb_pool.h"

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_pool",
					.version = "1.0"
);

/* PG_FUNCTION_INFO_V1 must be in the SAME file as the implementation */
PG_FUNCTION_INFO_V1(pool_status);
PG_FUNCTION_INFO_V1(pool_reset);
PG_FUNCTION_INFO_V1(pool_settings);

/* ----------------------------------------------------------------
 * GUC variables
 * ---------------------------------------------------------------- */

/* TCP port the pooler listens on; default 6432 (pgbouncer convention) */
static int	pool_port_guc = 6432;

/* Default pool size (max backend connections per pool) */
static int	pool_size_guc = 20;

/* ----------------------------------------------------------------
 * Shared memory state
 * ---------------------------------------------------------------- */

static PoolSharedState *pool_shared = NULL;

/* Hook save pointers for chaining */
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* ----------------------------------------------------------------
 * pool_shmem_size
 *
 * Compute the amount of shared memory needed.
 * ---------------------------------------------------------------- */
static Size
pool_shmem_size(void)
{
	return MAXALIGN(sizeof(PoolSharedState));
}

/* ----------------------------------------------------------------
 * pool_shmem_request
 *
 * Called during shared memory size negotiation (postmaster startup).
 * ---------------------------------------------------------------- */
static void
pool_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(pool_shmem_size());
	RequestNamedLWLockTranche("alohadb_pool", 1);
}

/* ----------------------------------------------------------------
 * pool_shmem_startup
 *
 * Called when shared memory is initialized.  Allocate and initialize
 * the PoolSharedState structure.
 * ---------------------------------------------------------------- */
static void
pool_shmem_startup(void)
{
	bool	found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	pool_shared = ShmemInitStruct("alohadb_pool",
								  pool_shmem_size(),
								  &found);

	if (!found)
	{
		/* First time: zero out and initialize */
		memset(pool_shared, 0, sizeof(PoolSharedState));
		pool_shared->lock =
			&(GetNamedLWLockTranche("alohadb_pool"))->lock;
		pool_shared->pool_port = pool_port_guc;
		pool_shared->pool_size = pool_size_guc;
		pool_shared->num_pools = 0;
		pool_shared->running = false;
	}

	LWLockRelease(AddinShmemInitLock);
}

/* ----------------------------------------------------------------
 * pool_main
 *
 * Background worker entry point.  Currently a stub that logs its
 * startup and sits in a WaitLatch loop until SIGTERM or postmaster
 * death.  The actual wire protocol proxy will be implemented here
 * in a future enhancement.
 * ---------------------------------------------------------------- */
void
pool_main(Datum main_arg)
{
	/* Establish signal handlers before unblocking signals */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	elog(LOG, "alohadb_pool: connection pooler started on port %d "
		 "(pool_size=%d)",
		 pool_port_guc, pool_size_guc);

	/* Mark the pooler as running in shared memory */
	if (pool_shared)
	{
		LWLockAcquire(pool_shared->lock, LW_EXCLUSIVE);
		pool_shared->running = true;
		pool_shared->pool_port = pool_port_guc;
		pool_shared->pool_size = pool_size_guc;
		LWLockRelease(pool_shared->lock);
	}

	/*
	 * Main loop: sleep until signaled or postmaster death.
	 *
	 * In the future, this loop will accept client connections on
	 * pool_port_guc, perform authentication, and proxy the wire
	 * protocol to/from backend connections.  For transaction-level
	 * pooling, the backend is released back to the pool when
	 * ReadyForQuery arrives with status 'I' (idle).
	 */
	for (;;)
	{
		int		rc;

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   1000L,
					   PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		/* Exit if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/* Check for SIGTERM (processed by die() -> CHECK_FOR_INTERRUPTS) */
		CHECK_FOR_INTERRUPTS();

		/* Reload configuration on SIGHUP */
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
			elog(LOG, "alohadb_pool: configuration reloaded "
				 "(port=%d, pool_size=%d)",
				 pool_port_guc, pool_size_guc);

			/* Update shared state with new settings */
			if (pool_shared)
			{
				LWLockAcquire(pool_shared->lock, LW_EXCLUSIVE);
				pool_shared->pool_port = pool_port_guc;
				pool_shared->pool_size = pool_size_guc;
				LWLockRelease(pool_shared->lock);
			}
		}
	}

	/* Mark pooler as stopped (not reachable, but good practice) */
	if (pool_shared)
	{
		LWLockAcquire(pool_shared->lock, LW_EXCLUSIVE);
		pool_shared->running = false;
		LWLockRelease(pool_shared->lock);
	}

	elog(LOG, "alohadb_pool: connection pooler shutting down");
	proc_exit(0);
}

/* ----------------------------------------------------------------
 * _PG_init
 *
 * Module load callback.  Registers GUC variables and, if loaded
 * via shared_preload_libraries, registers shmem hooks and the
 * background worker.
 * ---------------------------------------------------------------- */
void
_PG_init(void)
{
	BackgroundWorker worker;

	if (!process_shared_preload_libraries_in_progress)
		return;

	/* ---- GUC: alohadb.pool_port ---- */
	DefineCustomIntVariable("alohadb.pool_port",
							"TCP port the connection pooler listens on.",
							NULL,
							&pool_port_guc,
							6432,
							1024,
							65535,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	/* ---- GUC: alohadb.pool_size ---- */
	DefineCustomIntVariable("alohadb.pool_size",
							"Maximum number of backend connections per pool.",
							NULL,
							&pool_size_guc,
							20,
							1,
							1000,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	MarkGUCPrefixReserved("alohadb.pool");

	/* ---- Shared memory hooks ---- */
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = pool_shmem_request;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pool_shmem_startup;

	/* ---- Register background worker ---- */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
	snprintf(worker.bgw_library_name, MAXPGPATH, "alohadb_pool");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "pool_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "alohadb_pool worker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "alohadb_pool");
	worker.bgw_main_arg = (Datum) 0;
	worker.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&worker);
}

/* ----------------------------------------------------------------
 * pool_status()
 *
 * SQL-callable set-returning function.  Returns one row per tracked
 * connection pool with current statistics from shared memory.
 * ---------------------------------------------------------------- */
Datum
pool_status(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			i;

	if (!pool_shared)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("alohadb_pool must be loaded via shared_preload_libraries")));

	InitMaterializedSRF(fcinfo, 0);

	LWLockAcquire(pool_shared->lock, LW_SHARED);

	for (i = 0; i < pool_shared->num_pools; i++)
	{
		Datum		values[6];
		bool		nulls[6] = {false};
		PoolEntry  *pe = &pool_shared->pools[i];

		values[0] = CStringGetTextDatum(pe->name);
		values[1] = Int32GetDatum(pe->active);
		values[2] = Int32GetDatum(pe->idle);
		values[3] = Int32GetDatum(pe->waiting);
		values[4] = Int64GetDatum(pe->total_served);
		values[5] = Int32GetDatum(pe->pool_size);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	LWLockRelease(pool_shared->lock);

	PG_RETURN_NULL();
}

/* ----------------------------------------------------------------
 * pool_reset(pool_name text DEFAULT NULL)
 *
 * Reset statistics for the named pool, or all pools if NULL.
 * ---------------------------------------------------------------- */
Datum
pool_reset(PG_FUNCTION_ARGS)
{
	char   *target_name = NULL;
	int		i;

	if (!pool_shared)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("alohadb_pool must be loaded via shared_preload_libraries")));

	if (!PG_ARGISNULL(0))
		target_name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	LWLockAcquire(pool_shared->lock, LW_EXCLUSIVE);

	if (target_name == NULL)
	{
		/* Reset all pools */
		for (i = 0; i < pool_shared->num_pools; i++)
		{
			pool_shared->pools[i].active = 0;
			pool_shared->pools[i].idle = 0;
			pool_shared->pools[i].waiting = 0;
			pool_shared->pools[i].total_served = 0;
		}
	}
	else
	{
		/* Reset the specific named pool */
		for (i = 0; i < pool_shared->num_pools; i++)
		{
			if (strcmp(pool_shared->pools[i].name, target_name) == 0)
			{
				pool_shared->pools[i].active = 0;
				pool_shared->pools[i].idle = 0;
				pool_shared->pools[i].waiting = 0;
				pool_shared->pools[i].total_served = 0;
				break;
			}
		}

		if (i >= pool_shared->num_pools)
		{
			LWLockRelease(pool_shared->lock);
			ereport(WARNING,
					(errmsg("alohadb_pool: pool \"%s\" not found",
							target_name)));
			pfree(target_name);
			PG_RETURN_VOID();
		}
	}

	LWLockRelease(pool_shared->lock);

	if (target_name)
		pfree(target_name);

	PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * pool_settings()
 *
 * SQL-callable set-returning function.  Returns the current GUC
 * settings for the connection pooler.
 * ---------------------------------------------------------------- */
Datum
pool_settings(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Datum		values[2];
	bool		nulls[2] = {false};
	char		buf[64];

	if (!pool_shared)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("alohadb_pool must be loaded via shared_preload_libraries")));

	InitMaterializedSRF(fcinfo, 0);

	/* pool_port */
	values[0] = CStringGetTextDatum("pool_port");
	snprintf(buf, sizeof(buf), "%d", pool_port_guc);
	values[1] = CStringGetTextDatum(buf);
	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);

	/* pool_size */
	values[0] = CStringGetTextDatum("pool_size");
	snprintf(buf, sizeof(buf), "%d", pool_size_guc);
	values[1] = CStringGetTextDatum(buf);
	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);

	/* running */
	LWLockAcquire(pool_shared->lock, LW_SHARED);
	values[0] = CStringGetTextDatum("running");
	values[1] = CStringGetTextDatum(pool_shared->running ? "true" : "false");
	LWLockRelease(pool_shared->lock);
	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);

	/* num_pools */
	LWLockAcquire(pool_shared->lock, LW_SHARED);
	values[0] = CStringGetTextDatum("num_pools");
	snprintf(buf, sizeof(buf), "%d", pool_shared->num_pools);
	LWLockRelease(pool_shared->lock);
	values[1] = CStringGetTextDatum(buf);
	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);

	PG_RETURN_NULL();
}
