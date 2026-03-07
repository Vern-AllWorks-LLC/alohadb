/*-------------------------------------------------------------------------
 *
 * alohadb_gpu.c
 *	  GPU-offloaded vector operations extension for PostgreSQL.
 *
 *	  Provides batch distance computation, top-K selection, and matrix
 *	  multiplication on GPU (CUDA) with automatic CPU fallback.
 *
 *	  UDFs:
 *	    alohadb_gpu_batch_l2(query vector, candidates vector[])
 *	        RETURNS float4[]
 *	    alohadb_gpu_batch_cosine(query vector, candidates vector[])
 *	        RETURNS float4[]
 *	    alohadb_gpu_topk(query vector, candidates vector[], k int)
 *	        RETURNS TABLE(idx int, distance float4)
 *	    alohadb_gpu_matmul(a float4[][], b float4[][])
 *	        RETURNS float4[][]
 *	    alohadb_gpu_available()
 *	        RETURNS bool
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_gpu/alohadb_gpu.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "catalog/pg_type.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/vector.h"

#include "gpu_vector_ops.h"

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_gpu",
					.version = "1.0"
);

/* ----------------------------------------------------------------
 *		GUC variables
 * ----------------------------------------------------------------
 */
static int	alohadb_gpu_device_id = 0;
static int	alohadb_gpu_min_batch_size = 100;

/* Track whether GPU backend has been initialised in this process */
static bool gpu_backend_initialised = false;

/* ----------------------------------------------------------------
 *		OID for the built-in vector type (from pg_type.dat)
 * ----------------------------------------------------------------
 */
#define VECTOROID		6000
#define VECTORARRAYOID	6001

/* ----------------------------------------------------------------
 *		Function declarations
 * ----------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(alohadb_gpu_batch_l2);
PG_FUNCTION_INFO_V1(alohadb_gpu_batch_cosine);
PG_FUNCTION_INFO_V1(alohadb_gpu_topk);
PG_FUNCTION_INFO_V1(alohadb_gpu_matmul);
PG_FUNCTION_INFO_V1(alohadb_gpu_available);

/* ----------------------------------------------------------------
 *		Module initialisation
 * ----------------------------------------------------------------
 */

void		_PG_init(void);

void
_PG_init(void)
{
	/* Define GUCs */
	DefineCustomIntVariable("alohadb.gpu_device_id",
							"CUDA device ID to use for GPU operations.",
							NULL,
							&alohadb_gpu_device_id,
							0,		/* default */
							0,		/* min */
							255,	/* max */
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("alohadb.gpu_min_batch_size",
							"Minimum batch size to offload to GPU; below this threshold CPU is used.",
							NULL,
							&alohadb_gpu_min_batch_size,
							100,	/* default */
							1,		/* min */
							INT_MAX,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	MarkGUCPrefixReserved("alohadb");
}

/*
 * Ensure the GPU compute backend is initialised (lazy, once per process).
 */
static void
ensure_gpu_init(void)
{
	if (!gpu_backend_initialised)
	{
		(void) gpu_ops_init(alohadb_gpu_device_id);
		gpu_backend_initialised = true;
	}
}

/* ----------------------------------------------------------------
 *		Internal helpers
 * ----------------------------------------------------------------
 */

/*
 * Extract an array of Vector pointers from a vector[] Datum.
 *
 * Returns the number of elements.  *vectors is palloc'd in the
 * current memory context.
 */
static int
decon_vector_array(Datum arr_datum, Vector ***vectors)
{
	ArrayType  *arr = DatumGetArrayTypeP(arr_datum);
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;
	int			i;

	deconstruct_array(arr, VECTOROID, -1, false, TYPALIGN_INT,
					  &elems, &nulls, &nelems);

	*vectors = (Vector **) palloc(sizeof(Vector *) * nelems);

	for (i = 0; i < nelems; i++)
	{
		if (nulls[i])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("NULL not allowed in vector array")));
		(*vectors)[i] = DatumGetVector(elems[i]);
	}

	return nelems;
}

/*
 * Flatten an array of Vector pointers into a contiguous row-major
 * float buffer suitable for the GPU ops API.
 *
 * All vectors must have the same dimensionality.  Returns palloc'd
 * buffer of n_candidates * dim floats.
 */
static float *
flatten_vectors(Vector **vectors, int n_candidates, int dim)
{
	float	   *buf;
	int			i;

	buf = (float *) palloc(sizeof(float) * (size_t) n_candidates * dim);

	for (i = 0; i < n_candidates; i++)
	{
		if (vectors[i]->dim != dim)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("candidate vector %d has %d dimensions, expected %d",
							i + 1, vectors[i]->dim, dim)));

		memcpy(buf + (size_t) i * dim, vectors[i]->x, sizeof(float) * dim);
	}

	return buf;
}

/*
 * Build a float4[] result from a C float array.
 */
static Datum
build_float4_array(const float *vals, int n)
{
	Datum	   *elems;
	ArrayType  *result;
	int			i;

	elems = (Datum *) palloc(sizeof(Datum) * n);
	for (i = 0; i < n; i++)
		elems[i] = Float4GetDatum(vals[i]);

	result = construct_array(elems, n, FLOAT4OID, sizeof(float4),
							 true, TYPALIGN_INT);

	pfree(elems);

	PG_RETURN_ARRAYTYPE_P(result);
}

/*
 * Map a GpuOpsStatus to an appropriate ereport.
 */
static void
check_gpu_status(GpuOpsStatus status, const char *op_name)
{
	switch (status)
	{
		case GPU_OPS_OK:
			return;
		case GPU_OPS_ERR_NO_DEVICE:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("%s: no CUDA device available", op_name)));
			break;
		case GPU_OPS_ERR_ALLOC:
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("%s: GPU memory allocation failed", op_name)));
			break;
		case GPU_OPS_ERR_LAUNCH:
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("%s: GPU kernel launch failed", op_name)));
			break;
		case GPU_OPS_ERR_MEMCPY:
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("%s: GPU memory transfer failed", op_name)));
			break;
		case GPU_OPS_ERR_INVALID_ARG:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s: invalid argument", op_name)));
			break;
	}
}

/* ----------------------------------------------------------------
 *		UDF: alohadb_gpu_batch_l2
 * ----------------------------------------------------------------
 */

/*
 * alohadb_gpu_batch_l2(query vector, candidates vector[]) RETURNS float4[]
 *
 * Compute the L2 (Euclidean) distance from the query vector to each
 * candidate vector.  Returns an array of float4 distances in the same
 * order as the input candidates array.
 */
Datum
alohadb_gpu_batch_l2(PG_FUNCTION_ARGS)
{
	Vector	   *query = PG_GETARG_VECTOR_P(0);
	Datum		cand_datum = PG_GETARG_DATUM(1);
	Vector	  **candidates;
	int			n_candidates;
	int			dim;
	float	   *flat;
	float	   *distances;
	GpuOpsStatus status;

	ensure_gpu_init();

	dim = query->dim;
	n_candidates = decon_vector_array(cand_datum, &candidates);

	if (n_candidates == 0)
		PG_RETURN_ARRAYTYPE_P(construct_empty_array(FLOAT4OID));

	flat = flatten_vectors(candidates, n_candidates, dim);

	distances = (float *) palloc(sizeof(float) * n_candidates);

	status = gpu_ops_batch_l2(query->x, flat, dim, n_candidates, distances);
	check_gpu_status(status, "alohadb_gpu_batch_l2");

	pfree(flat);
	pfree(candidates);

	return build_float4_array(distances, n_candidates);
}

/* ----------------------------------------------------------------
 *		UDF: alohadb_gpu_batch_cosine
 * ----------------------------------------------------------------
 */

/*
 * alohadb_gpu_batch_cosine(query vector, candidates vector[]) RETURNS float4[]
 *
 * Compute the cosine distance (1 - cosine_similarity) from the query
 * vector to each candidate vector.
 */
Datum
alohadb_gpu_batch_cosine(PG_FUNCTION_ARGS)
{
	Vector	   *query = PG_GETARG_VECTOR_P(0);
	Datum		cand_datum = PG_GETARG_DATUM(1);
	Vector	  **candidates;
	int			n_candidates;
	int			dim;
	float	   *flat;
	float	   *distances;
	GpuOpsStatus status;

	ensure_gpu_init();

	dim = query->dim;
	n_candidates = decon_vector_array(cand_datum, &candidates);

	if (n_candidates == 0)
		PG_RETURN_ARRAYTYPE_P(construct_empty_array(FLOAT4OID));

	flat = flatten_vectors(candidates, n_candidates, dim);

	distances = (float *) palloc(sizeof(float) * n_candidates);

	status = gpu_ops_batch_cosine(query->x, flat, dim, n_candidates, distances);
	check_gpu_status(status, "alohadb_gpu_batch_cosine");

	pfree(flat);
	pfree(candidates);

	return build_float4_array(distances, n_candidates);
}

/* ----------------------------------------------------------------
 *		UDF: alohadb_gpu_topk
 * ----------------------------------------------------------------
 */

/*
 * Per-call state for the top-K set-returning function.
 */
typedef struct GpuTopKState
{
	GpuTopKEntry   *entries;
	int				count;
	int				current;
} GpuTopKState;

/*
 * alohadb_gpu_topk(query vector, candidates vector[], k int)
 *     RETURNS TABLE(idx int, distance float4)
 *
 * Compute L2 distances on GPU then return the k nearest candidates
 * as a set of (index, distance) rows.
 */
Datum
alohadb_gpu_topk(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	GpuTopKState   *state;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext	oldctx;
		Vector		   *query;
		Datum			cand_datum;
		Vector		  **candidates;
		int				n_candidates;
		int				dim;
		int				k;
		float		   *flat;
		GpuOpsStatus	status;
		TupleDesc		tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();
		oldctx = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		ensure_gpu_init();

		query = PG_GETARG_VECTOR_P(0);
		cand_datum = PG_GETARG_DATUM(1);
		k = PG_GETARG_INT32(2);

		if (k <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("k must be a positive integer")));

		dim = query->dim;
		n_candidates = decon_vector_array(cand_datum, &candidates);

		state = (GpuTopKState *) palloc0(sizeof(GpuTopKState));

		if (n_candidates == 0)
		{
			state->count = 0;
			state->current = 0;
			state->entries = NULL;
		}
		else
		{
			int		actual_k = (k < n_candidates) ? k : n_candidates;

			flat = flatten_vectors(candidates, n_candidates, dim);

			state->entries = (GpuTopKEntry *) palloc(
				sizeof(GpuTopKEntry) * actual_k);

			status = gpu_ops_topk(query->x, flat, dim, n_candidates, k,
								  state->entries, &state->count);
			check_gpu_status(status, "alohadb_gpu_topk");

			pfree(flat);
			pfree(candidates);
		}

		state->current = 0;

		/* Build result tuple descriptor */
		tupdesc = CreateTemplateTupleDesc(2);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "idx",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "distance",
						   FLOAT4OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		funcctx->user_fctx = state;

		MemoryContextSwitchTo(oldctx);
	}

	funcctx = SRF_PERCALL_SETUP();
	state = (GpuTopKState *) funcctx->user_fctx;

	if (state->current < state->count)
	{
		Datum		values[2];
		bool		nulls[2] = {false, false};
		HeapTuple	tuple;
		Datum		result;

		values[0] = Int32GetDatum(state->entries[state->current].idx);
		values[1] = Float4GetDatum(state->entries[state->current].distance);

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		state->current++;

		SRF_RETURN_NEXT(funcctx, result);
	}
	else
	{
		SRF_RETURN_DONE(funcctx);
	}
}

/* ----------------------------------------------------------------
 *		UDF: alohadb_gpu_matmul
 * ----------------------------------------------------------------
 */

/*
 * Helper: deconstruct a 2-D float4[][] array into a flat row-major
 * float buffer.  Sets *rows and *cols.
 */
static float *
decon_float4_matrix(Datum arr_datum, int *rows, int *cols)
{
	ArrayType  *arr = DatumGetArrayTypeP(arr_datum);
	int			ndim = ARR_NDIM(arr);
	int		   *dims;
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;
	float	   *buf;
	int			i;

	if (ndim != 2)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("matrix must be a 2-dimensional array")));

	dims = ARR_DIMS(arr);
	*rows = dims[0];
	*cols = dims[1];

	if (*rows <= 0 || *cols <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("matrix dimensions must be positive")));

	deconstruct_array(arr, FLOAT4OID, sizeof(float4), true, TYPALIGN_INT,
					  &elems, &nulls, &nelems);

	if (nelems != (*rows) * (*cols))
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("matrix element count mismatch")));

	buf = (float *) palloc(sizeof(float) * nelems);

	for (i = 0; i < nelems; i++)
	{
		if (nulls[i])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("NULL not allowed in matrix")));
		buf[i] = DatumGetFloat4(elems[i]);
	}

	pfree(elems);
	pfree(nulls);

	return buf;
}

/*
 * alohadb_gpu_matmul(a float4[][], b float4[][]) RETURNS float4[][]
 *
 * Perform matrix multiplication C = A * B on the GPU (or CPU fallback).
 */
Datum
alohadb_gpu_matmul(PG_FUNCTION_ARGS)
{
	Datum		a_datum = PG_GETARG_DATUM(0);
	Datum		b_datum = PG_GETARG_DATUM(1);
	int			m,
				p_a,
				p_b,
				n;
	float	   *a_buf;
	float	   *b_buf;
	float	   *c_buf;
	GpuOpsStatus status;
	Datum	   *elems;
	ArrayType  *result;
	int			dims[2];
	int			lbs[2];
	int			i;
	int			total;

	ensure_gpu_init();

	a_buf = decon_float4_matrix(a_datum, &m, &p_a);
	b_buf = decon_float4_matrix(b_datum, &p_b, &n);

	if (p_a != p_b)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("matrix inner dimensions must match: %d vs %d",
						p_a, p_b)));

	total = m * n;
	c_buf = (float *) palloc(sizeof(float) * total);

	status = gpu_ops_matmul(a_buf, m, p_a, b_buf, p_b, n, c_buf);
	check_gpu_status(status, "alohadb_gpu_matmul");

	pfree(a_buf);
	pfree(b_buf);

	/* Build 2-D result array */
	elems = (Datum *) palloc(sizeof(Datum) * total);
	for (i = 0; i < total; i++)
		elems[i] = Float4GetDatum(c_buf[i]);

	pfree(c_buf);

	dims[0] = m;
	dims[1] = n;
	lbs[0] = 1;
	lbs[1] = 1;

	result = construct_md_array(elems, NULL, 2, dims, lbs,
								FLOAT4OID, sizeof(float4),
								true, TYPALIGN_INT);

	pfree(elems);

	PG_RETURN_ARRAYTYPE_P(result);
}

/* ----------------------------------------------------------------
 *		UDF: alohadb_gpu_available
 * ----------------------------------------------------------------
 */

/*
 * alohadb_gpu_available() RETURNS bool
 *
 * Returns true if a CUDA-capable GPU is present and the extension was
 * compiled with CUDA support.
 */
Datum
alohadb_gpu_available(PG_FUNCTION_ARGS)
{
	ensure_gpu_init();

	PG_RETURN_BOOL(gpu_ops_available());
}
