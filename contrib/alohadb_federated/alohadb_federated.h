/*-------------------------------------------------------------------------
 *
 * alohadb_federated.h
 *	  Shared declarations for the alohadb_federated extension.
 *
 *	  Implements federated learning where training data stays in the
 *	  database and only model gradient updates are transmitted to a
 *	  Flower aggregation server via HTTP REST.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * contrib/alohadb_federated/alohadb_federated.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef ALOHADB_FEDERATED_H
#define ALOHADB_FEDERATED_H

#include "postgres.h"
#include "fmgr.h"
#include "storage/lwlock.h"
#include "utils/timestamp.h"

/*
 * Maximum number of model weights we support.  This caps the size of
 * weight arrays exchanged with the Flower server.
 */
#define FL_MAX_WEIGHTS			65536

/*
 * Maximum length of the model name string.
 */
#define FL_MODEL_NAME_MAXLEN	128

/*
 * HTTP timeout for Flower server communication, in seconds.
 */
#define FL_HTTP_TIMEOUT_SECS	30

/*
 * Maximum HTTP response buffer size from the Flower server.
 */
#define FL_MAX_RESPONSE_LEN		(4 * 1024 * 1024)

/*
 * Delay between FL training rounds when idle, in milliseconds.
 */
#define FL_ROUND_DELAY_MS		5000

/*
 * Supported model types for local training.
 */
typedef enum FLModelType
{
	FL_MODEL_LINEAR_REGRESSION = 0,
	FL_MODEL_LOGISTIC_REGRESSION
} FLModelType;

/*
 * FLWorkerStatus - status of the FL background worker.
 */
typedef enum FLWorkerStatus
{
	FL_STATUS_STOPPED = 0,
	FL_STATUS_STARTING,
	FL_STATUS_RUNNING,
	FL_STATUS_STOPPING,
	FL_STATUS_ERROR
} FLWorkerStatus;

/*
 * FLSharedState - shared memory state for FL worker coordination.
 *
 * Allocated in a named DSM segment so both the worker and regular
 * backends can inspect and control it.
 */
typedef struct FLSharedState
{
	LWLock		lock;					/* mutual exclusion */
	pid_t		worker_pid;				/* PID of the FL background worker */
	FLWorkerStatus status;				/* current worker status */
	int			last_round;				/* last completed FL round number */
	TimestampTz last_train_time;		/* timestamp of last training round */
	char		server_url[256];		/* Flower server URL snapshot */
	char		model_name[FL_MODEL_NAME_MAXLEN];	/* model name snapshot */
	char		error_msg[256];			/* last error message, if any */
} FLSharedState;

/*
 * FLTrainResult - result of a local training round.
 *
 * Contains the computed gradient deltas as a float array, along with
 * metadata about the training run.
 */
typedef struct FLTrainResult
{
	float	   *gradients;			/* gradient delta array (palloc'd) */
	int			num_weights;		/* number of elements in gradients */
	int64		num_samples;		/* number of training samples used */
	double		loss;				/* training loss for this round */
	bool		success;			/* whether training succeeded */
} FLTrainResult;

/* GUC variables (defined in alohadb_federated.c) */
extern char *fl_flower_server;
extern char *fl_model_name;
extern char *fl_training_query;
extern double fl_learning_rate;
extern int fl_batch_size;

/* ----------------------------------------------------------------
 * flower_client.c: Flower server HTTP REST communication
 * ----------------------------------------------------------------
 */

/*
 * fl_client_get_weights
 *	  Fetch aggregated model weights from the Flower server.
 *	  Returns a palloc'd float array, sets *num_weights.
 *	  Returns NULL on failure (with ereport at WARNING level).
 */
extern float *fl_client_get_weights(const char *server_url,
									const char *model_name,
									int *num_weights);

/*
 * fl_client_send_gradients
 *	  POST gradient updates to the Flower server.
 *	  Returns true on success, false on failure.
 */
extern bool fl_client_send_gradients(const char *server_url,
									 const char *model_name,
									 const float *gradients,
									 int num_weights,
									 int64 num_samples,
									 double loss);

/* ----------------------------------------------------------------
 * local_train.c: in-database local model training
 * ----------------------------------------------------------------
 */

/*
 * fl_local_train
 *	  Execute the training query via SPI, compute gradient updates
 *	  using the provided global weights.
 *
 *	  The training query must return rows where the last column is
 *	  the label (target) and all preceding columns are features,
 *	  all as float8.
 *
 *	  model_type selects the loss function and gradient computation:
 *	    FL_MODEL_LINEAR_REGRESSION  - MSE loss, linear gradients
 *	    FL_MODEL_LOGISTIC_REGRESSION - cross-entropy loss, sigmoid
 *
 *	  Returns a palloc'd FLTrainResult.
 */
extern FLTrainResult *fl_local_train(const char *training_query,
									 const float *weights,
									 int num_weights,
									 FLModelType model_type,
									 double learning_rate,
									 int batch_size);

/*
 * fl_detect_model_type
 *	  Heuristic to detect model type from the model name string.
 *	  If name contains "logistic" or "classification", returns
 *	  FL_MODEL_LOGISTIC_REGRESSION; otherwise FL_MODEL_LINEAR_REGRESSION.
 */
extern FLModelType fl_detect_model_type(const char *model_name);

#endif							/* ALOHADB_FEDERATED_H */
