/*-------------------------------------------------------------------------
 *
 * local_train.c
 *	  Local model training for federated learning.
 *
 *	  Executes the configured training query via SPI to fetch training
 *	  data, then computes gradient updates using simple built-in ML
 *	  primitives (no external ML library dependency).
 *
 *	  Supported model types:
 *	    - Linear regression: MSE loss, analytic gradients
 *	    - Logistic regression: sigmoid + binary cross-entropy loss
 *
 *	  The training query must return rows where:
 *	    - All columns are numeric (castable to float8)
 *	    - The last column is the label/target
 *	    - All preceding columns are features
 *
 *	  The weight vector layout is:
 *	    weights[0..n_features-1] = feature coefficients
 *	    weights[n_features]      = bias term
 *
 *	  Total weight count = n_features + 1
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_federated/local_train.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <math.h>
#include <string.h>

#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

#include "alohadb_federated.h"

/* ----------------------------------------------------------------
 * sigmoid
 *
 * Numerically stable sigmoid function.
 * ----------------------------------------------------------------
 */
static inline double
sigmoid(double x)
{
	if (x >= 0.0)
	{
		double		ez = exp(-x);

		return 1.0 / (1.0 + ez);
	}
	else
	{
		double		ez = exp(x);

		return ez / (1.0 + ez);
	}
}

/* ----------------------------------------------------------------
 * clamp_gradient
 *
 * Clip gradient values to prevent numerical explosion.
 * ----------------------------------------------------------------
 */
static inline double
clamp_gradient(double g)
{
	if (g > 100.0)
		return 100.0;
	if (g < -100.0)
		return -100.0;
	return g;
}

/* ----------------------------------------------------------------
 * fl_detect_model_type
 *
 * Simple heuristic to detect model type from the name.
 * ----------------------------------------------------------------
 */
FLModelType
fl_detect_model_type(const char *model_name)
{
	if (model_name == NULL)
		return FL_MODEL_LINEAR_REGRESSION;

	if (strstr(model_name, "logistic") != NULL ||
		strstr(model_name, "Logistic") != NULL ||
		strstr(model_name, "LOGISTIC") != NULL ||
		strstr(model_name, "classification") != NULL ||
		strstr(model_name, "Classification") != NULL ||
		strstr(model_name, "CLASSIFICATION") != NULL ||
		strstr(model_name, "classifier") != NULL ||
		strstr(model_name, "Classifier") != NULL)
	{
		return FL_MODEL_LOGISTIC_REGRESSION;
	}

	return FL_MODEL_LINEAR_REGRESSION;
}

/* ----------------------------------------------------------------
 * compute_linear_gradients
 *
 * Compute gradients for linear regression using MSE loss.
 *
 * For each sample (x, y):
 *   prediction = w^T x + bias
 *   loss = (prediction - y)^2
 *   dL/dw_j = 2 * (prediction - y) * x_j
 *   dL/dbias = 2 * (prediction - y)
 *
 * Gradients are averaged over all samples in the batch.
 * ----------------------------------------------------------------
 */
static void
compute_linear_gradients(double **features, double *labels,
						 int n_samples, int n_features,
						 const float *weights, float *gradients,
						 double *out_loss)
{
	double		total_loss = 0.0;
	int			i,
				j;

	/* Zero out gradients */
	memset(gradients, 0, sizeof(float) * (n_features + 1));

	for (i = 0; i < n_samples; i++)
	{
		double		prediction = 0.0;
		double		error;

		/* Compute prediction: w^T x + bias */
		for (j = 0; j < n_features; j++)
			prediction += (double) weights[j] * features[i][j];
		prediction += (double) weights[n_features];	/* bias */

		/* Error = prediction - label */
		error = prediction - labels[i];
		total_loss += error * error;

		/* Accumulate gradients */
		for (j = 0; j < n_features; j++)
			gradients[j] += (float) clamp_gradient(2.0 * error * features[i][j]);
		gradients[n_features] += (float) clamp_gradient(2.0 * error);
	}

	/* Average over samples */
	if (n_samples > 0)
	{
		double	scale = 1.0 / (double) n_samples;

		for (j = 0; j <= n_features; j++)
			gradients[j] = (float) ((double) gradients[j] * scale);
		total_loss *= scale;
	}

	*out_loss = total_loss;
}

/* ----------------------------------------------------------------
 * compute_logistic_gradients
 *
 * Compute gradients for logistic regression using binary
 * cross-entropy loss.
 *
 * For each sample (x, y) where y in {0, 1}:
 *   z = w^T x + bias
 *   p = sigmoid(z)
 *   loss = -[y * log(p) + (1-y) * log(1-p)]
 *   dL/dw_j = (p - y) * x_j
 *   dL/dbias = (p - y)
 *
 * Gradients are averaged over all samples in the batch.
 * ----------------------------------------------------------------
 */
static void
compute_logistic_gradients(double **features, double *labels,
						   int n_samples, int n_features,
						   const float *weights, float *gradients,
						   double *out_loss)
{
	double		total_loss = 0.0;
	int			i,
				j;
	double		epsilon = 1e-15;

	/* Zero out gradients */
	memset(gradients, 0, sizeof(float) * (n_features + 1));

	for (i = 0; i < n_samples; i++)
	{
		double		z = 0.0;
		double		p;
		double		error;
		double		y = labels[i];

		/* Compute logit: w^T x + bias */
		for (j = 0; j < n_features; j++)
			z += (double) weights[j] * features[i][j];
		z += (double) weights[n_features];	/* bias */

		/* Sigmoid activation */
		p = sigmoid(z);

		/* Binary cross-entropy loss */
		total_loss += -(y * log(p + epsilon) + (1.0 - y) * log(1.0 - p + epsilon));

		/* Gradient: (p - y) */
		error = p - y;

		/* Accumulate gradients */
		for (j = 0; j < n_features; j++)
			gradients[j] += (float) clamp_gradient(error * features[i][j]);
		gradients[n_features] += (float) clamp_gradient(error);
	}

	/* Average over samples */
	if (n_samples > 0)
	{
		double	scale = 1.0 / (double) n_samples;

		for (j = 0; j <= n_features; j++)
			gradients[j] = (float) ((double) gradients[j] * scale);
		total_loss *= scale;
	}

	*out_loss = total_loss;
}

/* ================================================================
 * fl_local_train
 *
 * Main local training function.  Executes the training query via
 * SPI, fetches all rows, and computes gradient updates.
 * ================================================================
 */
FLTrainResult *
fl_local_train(const char *training_query, const float *weights,
			   int num_weights, FLModelType model_type,
			   double learning_rate, int batch_size)
{
	FLTrainResult *result;
	int				ret;
	uint64			proc;
	TupleDesc		tupdesc;
	int				n_features;
	int				n_cols;
	int				n_samples;
	int				n_batches;
	int				batch_start;
	double		  **features = NULL;
	double		   *labels = NULL;
	float		   *gradients;
	float		   *batch_gradients;
	double			total_loss = 0.0;
	int				total_samples = 0;
	MemoryContext	oldctx;
	MemoryContext	trainctx;
	int				i,
					j;

	result = (FLTrainResult *) palloc0(sizeof(FLTrainResult));
	result->success = false;

	if (training_query == NULL || training_query[0] == '\0')
	{
		ereport(WARNING,
				(errmsg("alohadb_federated: training query is not configured")));
		return result;
	}

	/* Create a temporary memory context for training data */
	trainctx = AllocSetContextCreate(CurrentMemoryContext,
									 "FL training context",
									 ALLOCSET_DEFAULT_SIZES);
	oldctx = MemoryContextSwitchTo(trainctx);

	/* Connect to SPI and execute the training query (read-only) */
	SPI_connect();

	ret = SPI_execute(training_query, true, 0);
	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		MemoryContextSwitchTo(oldctx);
		MemoryContextDelete(trainctx);
		ereport(WARNING,
				(errmsg("alohadb_federated: training query failed: %s",
						SPI_result_code_string(ret))));
		return result;
	}

	proc = SPI_processed;
	if (proc == 0)
	{
		SPI_finish();
		MemoryContextSwitchTo(oldctx);
		MemoryContextDelete(trainctx);
		ereport(WARNING,
				(errmsg("alohadb_federated: training query returned no rows")));
		return result;
	}

	tupdesc = SPI_tuptable->tupdesc;
	n_cols = tupdesc->natts;

	if (n_cols < 2)
	{
		SPI_finish();
		MemoryContextSwitchTo(oldctx);
		MemoryContextDelete(trainctx);
		ereport(WARNING,
				(errmsg("alohadb_federated: training query must return at least "
						"2 columns (features + label)")));
		return result;
	}

	n_features = n_cols - 1;	/* last column is the label */

	/*
	 * Verify the weight vector size matches.  num_weights should be
	 * n_features + 1 (features + bias).  If weights are empty (first
	 * round), we'll initialize them to zero.
	 */
	if (num_weights > 0 && num_weights != n_features + 1)
	{
		SPI_finish();
		MemoryContextSwitchTo(oldctx);
		MemoryContextDelete(trainctx);
		ereport(WARNING,
				(errmsg("alohadb_federated: weight vector size %d does not "
						"match expected %d (features=%d + bias)",
						num_weights, n_features + 1, n_features)));
		return result;
	}

	n_samples = (int) proc;

	/* Allocate feature and label arrays */
	features = (double **) palloc(sizeof(double *) * n_samples);
	labels = (double *) palloc(sizeof(double) * n_samples);

	for (i = 0; i < n_samples; i++)
	{
		HeapTuple	tuple = SPI_tuptable->vals[i];

		features[i] = (double *) palloc(sizeof(double) * n_features);

		/* Extract feature columns */
		for (j = 0; j < n_features; j++)
		{
			bool	isnull;
			Datum	val;

			val = SPI_getbinval(tuple, tupdesc, j + 1, &isnull);
			if (isnull)
				features[i][j] = 0.0;
			else
				features[i][j] = DatumGetFloat8(val);
		}

		/* Extract label (last column) */
		{
			bool	isnull;
			Datum	val;

			val = SPI_getbinval(tuple, tupdesc, n_cols, &isnull);
			if (isnull)
				labels[i] = 0.0;
			else
				labels[i] = DatumGetFloat8(val);
		}
	}

	SPI_finish();

	/*
	 * If no weights were provided (first round), use zeros.
	 * We need a local mutable copy regardless.
	 */
	{
		float  *local_weights;

		local_weights = (float *) palloc0(sizeof(float) * (n_features + 1));
		if (weights != NULL && num_weights == n_features + 1)
			memcpy(local_weights, weights, sizeof(float) * (n_features + 1));

		/* Allocate gradient accumulator and per-batch gradient buffer */
		gradients = (float *) palloc0(sizeof(float) * (n_features + 1));
		batch_gradients = (float *) palloc0(sizeof(float) * (n_features + 1));

		/* Process data in batches */
		n_batches = 0;
		for (batch_start = 0; batch_start < n_samples; batch_start += batch_size)
		{
			int		batch_end = batch_start + batch_size;
			int		current_batch_size;
			double	batch_loss = 0.0;

			if (batch_end > n_samples)
				batch_end = n_samples;
			current_batch_size = batch_end - batch_start;

			/* Compute gradients for this batch */
			switch (model_type)
			{
				case FL_MODEL_LINEAR_REGRESSION:
					compute_linear_gradients(features + batch_start,
											 labels + batch_start,
											 current_batch_size,
											 n_features,
											 local_weights,
											 batch_gradients,
											 &batch_loss);
					break;

				case FL_MODEL_LOGISTIC_REGRESSION:
					compute_logistic_gradients(features + batch_start,
											   labels + batch_start,
											   current_batch_size,
											   n_features,
											   local_weights,
											   batch_gradients,
											   &batch_loss);
					break;
			}

			/* Accumulate batch gradients into total gradient */
			for (j = 0; j <= n_features; j++)
				gradients[j] += batch_gradients[j];

			total_loss += batch_loss * current_batch_size;
			total_samples += current_batch_size;
			n_batches++;
		}

		/* Average gradients across all batches */
		if (n_batches > 0)
		{
			double	scale = 1.0 / (double) n_batches;

			for (j = 0; j <= n_features; j++)
				gradients[j] = (float) ((double) gradients[j] * scale);
		}

		/* Apply learning rate to gradients (these are the deltas to send) */
		for (j = 0; j <= n_features; j++)
			gradients[j] = (float) ((double) gradients[j] * learning_rate);

		pfree(local_weights);
		pfree(batch_gradients);
	}

	/* Switch back to caller's memory context for the result */
	MemoryContextSwitchTo(oldctx);

	/* Copy gradients to the caller's memory context */
	result->gradients = (float *) palloc(sizeof(float) * (n_features + 1));
	memcpy(result->gradients, gradients, sizeof(float) * (n_features + 1));
	result->num_weights = n_features + 1;
	result->num_samples = total_samples;
	result->loss = (total_samples > 0) ? (total_loss / total_samples) : 0.0;
	result->success = true;

	/* Clean up the training context */
	MemoryContextDelete(trainctx);

	elog(DEBUG1, "alohadb_federated: local training completed - "
		 "samples=%d, features=%d, loss=%.6f",
		 total_samples, n_features, result->loss);

	return result;
}
