/*-------------------------------------------------------------------------
 *
 * gpu_vector_ops.h
 *	  C API for GPU-accelerated (or CPU-fallback) vector operations.
 *
 *	  This header defines the interface between the PostgreSQL extension
 *	  (alohadb_gpu.c) and the compute backend.  When CUDA is available
 *	  the implementations live in gpu_vector_ops.cu; otherwise the
 *	  CPU-only fallback in gpu_cpu_fallback.c is linked instead.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * contrib/alohadb_gpu/gpu_vector_ops.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GPU_VECTOR_OPS_H
#define GPU_VECTOR_OPS_H

#include <stdbool.h>
#include <stdint.h>

/*
 * Result codes returned by every GPU-ops function.
 */
typedef enum GpuOpsStatus
{
	GPU_OPS_OK = 0,
	GPU_OPS_ERR_NO_DEVICE,
	GPU_OPS_ERR_ALLOC,
	GPU_OPS_ERR_LAUNCH,
	GPU_OPS_ERR_MEMCPY,
	GPU_OPS_ERR_INVALID_ARG
} GpuOpsStatus;

/*
 * Element of a top-K result set.
 */
typedef struct GpuTopKEntry
{
	int			idx;			/* 0-based index into the candidate array */
	float		distance;		/* computed distance value */
} GpuTopKEntry;

/* ----------------------------------------------------------------
 *		Lifecycle
 * ----------------------------------------------------------------
 */

/*
 * gpu_ops_init
 *		Initialise the compute backend for the given device id.
 *		For the CPU fallback this is a no-op that always succeeds.
 */
extern GpuOpsStatus gpu_ops_init(int device_id);

/*
 * gpu_ops_available
 *		Returns true when a usable CUDA device is present.
 */
extern bool gpu_ops_available(void);

/* ----------------------------------------------------------------
 *		Batch distance functions
 *
 * All distance functions accept:
 *   query      - pointer to the query vector (dim floats)
 *   candidates - pointer to n_candidates * dim floats, stored row-major
 *   dim        - dimensionality of each vector
 *   n_candidates - number of candidate vectors
 *   out        - caller-allocated array of n_candidates floats for results
 * ----------------------------------------------------------------
 */

/*
 * gpu_ops_batch_l2
 *		Compute L2 (Euclidean) distance from query to every candidate.
 */
extern GpuOpsStatus gpu_ops_batch_l2(const float *query,
									 const float *candidates,
									 int dim,
									 int n_candidates,
									 float *out);

/*
 * gpu_ops_batch_cosine
 *		Compute cosine distance (1 - cosine_similarity) from query to
 *		every candidate.
 */
extern GpuOpsStatus gpu_ops_batch_cosine(const float *query,
										 const float *candidates,
										 int dim,
										 int n_candidates,
										 float *out);

/* ----------------------------------------------------------------
 *		Top-K
 * ----------------------------------------------------------------
 */

/*
 * gpu_ops_topk
 *		Compute distances (L2) from query to every candidate, then
 *		return the k nearest indices and distances.
 *
 *		out must be pre-allocated for k GpuTopKEntry elements.
 *		*out_k is set to the actual number of results (min(k, n_candidates)).
 */
extern GpuOpsStatus gpu_ops_topk(const float *query,
								 const float *candidates,
								 int dim,
								 int n_candidates,
								 int k,
								 GpuTopKEntry *out,
								 int *out_k);

/* ----------------------------------------------------------------
 *		Matrix multiplication
 * ----------------------------------------------------------------
 */

/*
 * gpu_ops_matmul
 *		C = A * B   where A is (m x p), B is (p x n), C is (m x n).
 *		All matrices are stored row-major as flat float arrays.
 *		out must be pre-allocated for m * n floats.
 */
extern GpuOpsStatus gpu_ops_matmul(const float *a, int m, int p,
								   const float *b, int p2, int n,
								   float *out);

#endif							/* GPU_VECTOR_OPS_H */
