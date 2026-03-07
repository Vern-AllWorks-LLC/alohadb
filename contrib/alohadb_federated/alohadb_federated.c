/*-------------------------------------------------------------------------
 *
 * alohadb_federated.c
 *	  Federated Learning extension for PostgreSQL.
 *
 *	  Enables federated learning where training data never leaves the
 *	  database.  A background worker acts as a Flower framework client,
 *	  communicating with a Flower aggregation server via HTTP REST:
 *
 *	    1. Receive global model weights from the Flower server
 *	    2. Train on local data using an in-database SPI query
 *	    3. Send gradient deltas back to the Flower server
 *	    4. Data never leaves the database -- only gradients transmitted
 *
 *	  SQL functions:
 *	    alohadb_fl_start()  - start the FL background worker
 *	    alohadb_fl_stop()   - stop the FL background worker
 *	    alohadb_fl_status() - query worker status and training state
 *
 *	  GUCs:
 *	    alohadb.flower_server     - URL of the Flower REST bridge
 *	    alohadb.fl_model_name     - name of the FL model
 *	    alohadb.fl_training_query - SQL query returning (features..., label)
 *	    alohadb.fl_learning_rate  - learning rate for local training
 *	    alohadb.fl_batch_size     - mini-batch size for local training
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_federated/alohadb_federated.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <string.h>

#include "access/xact.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/dsm_registry.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/procsignal.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#include "alohadb_federated.h"

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_federated",
					.version = "1.0"
);

/* UDF declarations */
PG_FUNCTION_INFO_V1(alohadb_fl_start);
PG_FUNCTION_INFO_V1(alohadb_fl_stop);
PG_FUNCTION_INFO_V1(alohadb_fl_status);

/* Background worker main entry point */
PGDLLEXPORT void alohadb_fl_worker_main(Datum main_arg);

/* GUC variables */
char	   *fl_flower_server = NULL;
char	   *fl_model_name = NULL;
char	   *fl_training_query = NULL;
double		fl_learning_rate = 0.01;
int			fl_batch_size = 32;

/* Pointer to shared state (set up via DSM registry) */
static FLSharedState *fl_shared = NULL;

/* Background worker handle, kept by the launching backend */
static BackgroundWorkerHandle *fl_worker_handle = NULL;

/* ----------------------------------------------------------------
 * fl_init_state
 *
 * Initialize shared state when the DSM segment is first created.
 * ----------------------------------------------------------------
 */
static void
fl_init_state(void *ptr)
{
	FLSharedState *state = (FLSharedState *) ptr;

	LWLockInitialize(&state->lock, LWLockNewTrancheId());
	state->worker_pid = InvalidPid;
	state->status = FL_STATUS_STOPPED;
	state->last_round = 0;
	state->last_train_time = 0;
	state->server_url[0] = '\0';
	state->model_name[0] = '\0';
	state->error_msg[0] = '\0';
}

/* ----------------------------------------------------------------
 * fl_init_shmem
 *
 * Get or create the FL shared memory segment.
 * Returns true if an existing segment was found.
 * ----------------------------------------------------------------
 */
static bool
fl_init_shmem(void)
{
	bool	found;

	fl_shared = GetNamedDSMSegment("alohadb_federated",
								   sizeof(FLSharedState),
								   fl_init_state,
								   &found);
	LWLockRegisterTranche(fl_shared->lock.tranche, "alohadb_federated");

	return found;
}

/* ----------------------------------------------------------------
 * fl_detach_shmem
 *
 * before_shmem_exit callback: clear our PID from shared state.
 * ----------------------------------------------------------------
 */
static void
fl_detach_shmem(int code, Datum arg)
{
	if (fl_shared == NULL)
		return;

	LWLockAcquire(&fl_shared->lock, LW_EXCLUSIVE);
	if (fl_shared->worker_pid == MyProcPid)
	{
		fl_shared->worker_pid = InvalidPid;
		fl_shared->status = FL_STATUS_STOPPED;
	}
	LWLockRelease(&fl_shared->lock);
}

/* ================================================================
 * Background Worker Main
 *
 * This is the main loop for the FL background worker.  It:
 *   1. Connects to the database (to enable SPI)
 *   2. Loops:
 *      a. Fetch global weights from Flower server
 *      b. Train locally via SPI query
 *      c. Send gradient updates back
 *      d. Sleep until next round
 *   3. Exits on shutdown signal
 * ================================================================
 */
void
alohadb_fl_worker_main(Datum main_arg)
{
	char		server_url[256];
	char		model_name_buf[FL_MODEL_NAME_MAXLEN];
	char		training_query_buf[4096];
	double		learning_rate;
	int			batch_size;

	/* Establish signal handlers */
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	BackgroundWorkerUnblockSignals();

	/* Initialize shared memory */
	fl_init_shmem();
	before_shmem_exit(fl_detach_shmem, 0);

	/* Register ourselves in shared state */
	LWLockAcquire(&fl_shared->lock, LW_EXCLUSIVE);
	if (fl_shared->worker_pid != InvalidPid)
	{
		LWLockRelease(&fl_shared->lock);
		ereport(LOG,
				(errmsg("alohadb_federated: FL worker already running "
						"under PID %d", (int) fl_shared->worker_pid)));
		return;
	}
	fl_shared->worker_pid = MyProcPid;
	fl_shared->status = FL_STATUS_STARTING;
	fl_shared->error_msg[0] = '\0';
	LWLockRelease(&fl_shared->lock);

	/*
	 * Connect to the database specified in bgw_extra, or the default
	 * database.  We need a database connection for SPI.
	 */
	BackgroundWorkerInitializeConnection("postgres", NULL, 0);

	ereport(LOG,
			(errmsg("alohadb_federated: FL background worker started "
					"(PID %d)", MyProcPid)));

	/* Mark as running */
	LWLockAcquire(&fl_shared->lock, LW_EXCLUSIVE);
	fl_shared->status = FL_STATUS_RUNNING;
	LWLockRelease(&fl_shared->lock);

	/* Main training loop */
	while (!ShutdownRequestPending)
	{
		FLModelType		model_type;
		float		   *global_weights = NULL;
		int				num_weights = 0;
		FLTrainResult  *train_result;

		/* Reload configuration on SIGHUP */
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/* Snapshot GUC values under shared lock */
		LWLockAcquire(&fl_shared->lock, LW_EXCLUSIVE);
		if (fl_flower_server != NULL)
			strlcpy(fl_shared->server_url, fl_flower_server,
					sizeof(fl_shared->server_url));
		if (fl_model_name != NULL)
			strlcpy(fl_shared->model_name, fl_model_name,
					sizeof(fl_shared->model_name));
		LWLockRelease(&fl_shared->lock);

		/* Take local copies of GUC values for this round */
		strlcpy(server_url,
				(fl_flower_server != NULL) ? fl_flower_server : "",
				sizeof(server_url));
		strlcpy(model_name_buf,
				(fl_model_name != NULL) ? fl_model_name : "default",
				sizeof(model_name_buf));
		strlcpy(training_query_buf,
				(fl_training_query != NULL) ? fl_training_query : "",
				sizeof(training_query_buf));
		learning_rate = fl_learning_rate;
		batch_size = fl_batch_size;

		/* Check that required GUCs are configured */
		if (server_url[0] == '\0' || training_query_buf[0] == '\0')
		{
			elog(DEBUG1, "alohadb_federated: waiting for configuration "
				 "(flower_server and fl_training_query must be set)");
			goto wait_next_round;
		}

		/* Detect model type from the model name */
		model_type = fl_detect_model_type(model_name_buf);

		/* Step 1: Fetch global weights from the Flower server */
		global_weights = fl_client_get_weights(server_url, model_name_buf,
											   &num_weights);
		/* If no weights yet (first round), that's OK -- train from zero */

		/* Step 2: Local training via SPI */
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());

		train_result = fl_local_train(training_query_buf,
									  global_weights, num_weights,
									  model_type, learning_rate,
									  batch_size);

		PopActiveSnapshot();
		CommitTransactionCommand();

		if (global_weights != NULL)
			pfree(global_weights);

		if (!train_result->success)
		{
			LWLockAcquire(&fl_shared->lock, LW_EXCLUSIVE);
			strlcpy(fl_shared->error_msg, "local training failed",
					sizeof(fl_shared->error_msg));
			LWLockRelease(&fl_shared->lock);

			if (train_result->gradients)
				pfree(train_result->gradients);
			pfree(train_result);
			goto wait_next_round;
		}

		/* Step 3: Send gradient updates to the Flower server */
		if (!fl_client_send_gradients(server_url, model_name_buf,
									  train_result->gradients,
									  train_result->num_weights,
									  train_result->num_samples,
									  train_result->loss))
		{
			LWLockAcquire(&fl_shared->lock, LW_EXCLUSIVE);
			strlcpy(fl_shared->error_msg,
					"failed to send gradients to Flower server",
					sizeof(fl_shared->error_msg));
			LWLockRelease(&fl_shared->lock);
		}
		else
		{
			/* Success: update shared state */
			LWLockAcquire(&fl_shared->lock, LW_EXCLUSIVE);
			fl_shared->last_round++;
			fl_shared->last_train_time = GetCurrentTimestamp();
			fl_shared->error_msg[0] = '\0';
			LWLockRelease(&fl_shared->lock);

			elog(LOG, "alohadb_federated: completed FL round %d "
				 "(samples=" INT64_FORMAT ", loss=%.6f)",
				 fl_shared->last_round, train_result->num_samples,
				 train_result->loss);
		}

		/* Clean up training result */
		if (train_result->gradients)
			pfree(train_result->gradients);
		pfree(train_result);

wait_next_round:
		/* Wait for the next round or shutdown/config reload signal */
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 FL_ROUND_DELAY_MS,
						 PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
	}

	/* Mark as stopped */
	LWLockAcquire(&fl_shared->lock, LW_EXCLUSIVE);
	fl_shared->status = FL_STATUS_STOPPED;
	fl_shared->worker_pid = InvalidPid;
	LWLockRelease(&fl_shared->lock);

	ereport(LOG,
			(errmsg("alohadb_federated: FL background worker shutting down")));
}

/* ================================================================
 * alohadb_fl_start() RETURNS void
 *
 * Launch the FL background worker dynamically.
 * ================================================================
 */
Datum
alohadb_fl_start(PG_FUNCTION_ARGS)
{
	BackgroundWorker	worker = {0};
	BackgroundWorkerHandle *handle;
	BgwHandleStatus		status;
	pid_t				pid;

	/* Initialize shared memory */
	fl_init_shmem();

	/* Check if a worker is already running */
	LWLockAcquire(&fl_shared->lock, LW_SHARED);
	if (fl_shared->worker_pid != InvalidPid)
	{
		pid_t	existing_pid = fl_shared->worker_pid;

		LWLockRelease(&fl_shared->lock);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("alohadb_federated: FL worker is already running "
						"under PID %d", (int) existing_pid)));
	}
	LWLockRelease(&fl_shared->lock);

	/* Validate that required GUCs are set */
	if (fl_flower_server == NULL || fl_flower_server[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("alohadb.flower_server is not configured"),
				 errhint("SET alohadb.flower_server = 'http://localhost:8080';")));

	if (fl_training_query == NULL || fl_training_query[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("alohadb.fl_training_query is not configured"),
				 errhint("SET alohadb.fl_training_query = "
						 "'SELECT feature1::float8, feature2::float8, "
						 "label::float8 FROM training_data';")));

	/* Register the dynamic background worker */
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	strcpy(worker.bgw_library_name, "alohadb_federated");
	strcpy(worker.bgw_function_name, "alohadb_fl_worker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "alohadb FL worker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "alohadb FL worker");
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not register FL background worker"),
				 errhint("You may need to increase "
						 "\"max_worker_processes\".")));

	/* Save the handle for later stop */
	fl_worker_handle = handle;

	/* Wait for it to start */
	status = WaitForBackgroundWorkerStartup(handle, &pid);
	if (status != BGWH_STARTED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start FL background worker"),
				 errhint("Check the server log for details.")));

	ereport(NOTICE,
			(errmsg("alohadb_federated: FL worker started (PID %d)",
					(int) pid)));

	PG_RETURN_VOID();
}

/* ================================================================
 * alohadb_fl_stop() RETURNS void
 *
 * Stop the FL background worker.
 * ================================================================
 */
Datum
alohadb_fl_stop(PG_FUNCTION_ARGS)
{
	/* Initialize shared memory */
	fl_init_shmem();

	LWLockAcquire(&fl_shared->lock, LW_SHARED);
	if (fl_shared->worker_pid == InvalidPid)
	{
		LWLockRelease(&fl_shared->lock);
		ereport(NOTICE,
				(errmsg("alohadb_federated: no FL worker is running")));
		PG_RETURN_VOID();
	}
	LWLockRelease(&fl_shared->lock);

	/* Terminate via the handle if we have it */
	if (fl_worker_handle != NULL)
	{
		TerminateBackgroundWorker(fl_worker_handle);
		(void) WaitForBackgroundWorkerShutdown(fl_worker_handle);
		fl_worker_handle = NULL;
	}
	else
	{
		/*
		 * If we don't have the handle (e.g. different session), signal
		 * the worker via its PID.
		 */
		pid_t	worker_pid;

		LWLockAcquire(&fl_shared->lock, LW_SHARED);
		worker_pid = fl_shared->worker_pid;
		LWLockRelease(&fl_shared->lock);

		if (worker_pid != InvalidPid)
			kill(worker_pid, SIGTERM);
	}

	ereport(NOTICE,
			(errmsg("alohadb_federated: FL worker stop signal sent")));

	PG_RETURN_VOID();
}

/* ================================================================
 * alohadb_fl_status()
 *     RETURNS TABLE(status text, server text, model text,
 *                   last_round int, last_train_time timestamptz)
 *
 * Query the current FL worker status.
 * ================================================================
 */
Datum
alohadb_fl_status(PG_FUNCTION_ARGS)
{
	TupleDesc		tupdesc;
	Datum			values[5];
	bool			nulls[5];
	HeapTuple		tuple;
	const char	   *status_str;
	char			server_buf[256];
	char			model_buf[FL_MODEL_NAME_MAXLEN];
	int				last_round;
	TimestampTz		last_train_time;

	/* Build the result tuple descriptor */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));
	tupdesc = BlessTupleDesc(tupdesc);

	/* Initialize shared memory */
	fl_init_shmem();

	/* Read shared state */
	LWLockAcquire(&fl_shared->lock, LW_SHARED);

	switch (fl_shared->status)
	{
		case FL_STATUS_STOPPED:
			status_str = "stopped";
			break;
		case FL_STATUS_STARTING:
			status_str = "starting";
			break;
		case FL_STATUS_RUNNING:
			status_str = "running";
			break;
		case FL_STATUS_STOPPING:
			status_str = "stopping";
			break;
		case FL_STATUS_ERROR:
			status_str = "error";
			break;
		default:
			status_str = "unknown";
			break;
	}

	strlcpy(server_buf, fl_shared->server_url, sizeof(server_buf));
	strlcpy(model_buf, fl_shared->model_name, sizeof(model_buf));
	last_round = fl_shared->last_round;
	last_train_time = fl_shared->last_train_time;

	LWLockRelease(&fl_shared->lock);

	/* Build the result tuple */
	memset(nulls, 0, sizeof(nulls));

	values[0] = CStringGetTextDatum(status_str);
	values[1] = CStringGetTextDatum(server_buf);
	values[2] = CStringGetTextDatum(model_buf);
	values[3] = Int32GetDatum(last_round);

	if (last_train_time == 0)
		nulls[4] = true;
	else
		values[4] = TimestampTzGetDatum(last_train_time);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/* ================================================================
 * _PG_init
 *
 * Module load callback: register GUCs.
 * ================================================================
 */
void
_PG_init(void)
{
	DefineCustomStringVariable("alohadb.flower_server",
							   "URL of the Flower aggregation server REST endpoint.",
							   "The FL background worker will communicate with "
							   "this server to exchange model weights and gradients.",
							   &fl_flower_server,
							   "",
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable("alohadb.fl_model_name",
							   "Name of the federated learning model.",
							   "Used to identify the model on the Flower server.",
							   &fl_model_name,
							   "default",
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable("alohadb.fl_training_query",
							   "SQL query that returns training data.",
							   "Must return numeric columns where the last column "
							   "is the label and all preceding columns are features. "
							   "All columns must be castable to float8.",
							   &fl_training_query,
							   "",
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	DefineCustomRealVariable("alohadb.fl_learning_rate",
							 "Learning rate for local model training.",
							 NULL,
							 &fl_learning_rate,
							 0.01,
							 0.0,
							 10.0,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("alohadb.fl_batch_size",
							"Mini-batch size for local model training.",
							NULL,
							&fl_batch_size,
							32,
							1,
							100000,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	MarkGUCPrefixReserved("alohadb");
}
