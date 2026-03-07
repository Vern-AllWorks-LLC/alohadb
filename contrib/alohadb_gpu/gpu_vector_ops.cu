/*-------------------------------------------------------------------------
 *
 * gpu_vector_ops.cu
 *	  CUDA kernel implementations for GPU-accelerated vector operations.
 *
 *	  This file is only compiled when nvcc is available and HAVE_CUDA is
 *	  defined.  It provides the same C API as gpu_cpu_fallback.c.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_gpu/gpu_vector_ops.cu
 *
 *-------------------------------------------------------------------------
 */

#include <cuda_runtime.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <float.h>

extern "C" {
#include "gpu_vector_ops.h"
}

/* ----------------------------------------------------------------
 *		Constants
 * ----------------------------------------------------------------
 */
#define GPU_BLOCK_SIZE		256
#define GPU_MAX_SHARED_BYTES	(48 * 1024)		/* 48 KiB default shared mem */

/* ----------------------------------------------------------------
 *		Device state
 * ----------------------------------------------------------------
 */
static bool		cuda_initialised = false;
static bool		cuda_device_ok = false;
static int		cuda_device_id = 0;

/* ----------------------------------------------------------------
 *		Lifecycle
 * ----------------------------------------------------------------
 */

extern "C" GpuOpsStatus
gpu_ops_init(int device_id)
{
	int			device_count = 0;
	cudaError_t	err;

	err = cudaGetDeviceCount(&device_count);
	if (err != cudaSuccess || device_count == 0)
	{
		cuda_initialised = true;
		cuda_device_ok = false;
		return GPU_OPS_ERR_NO_DEVICE;
	}

	if (device_id < 0 || device_id >= device_count)
	{
		cuda_initialised = true;
		cuda_device_ok = false;
		return GPU_OPS_ERR_NO_DEVICE;
	}

	err = cudaSetDevice(device_id);
	if (err != cudaSuccess)
	{
		cuda_initialised = true;
		cuda_device_ok = false;
		return GPU_OPS_ERR_NO_DEVICE;
	}

	cuda_device_id = device_id;
	cuda_device_ok = true;
	cuda_initialised = true;

	return GPU_OPS_OK;
}

extern "C" bool
gpu_ops_available(void)
{
	if (!cuda_initialised)
		gpu_ops_init(0);

	return cuda_device_ok;
}

/* ----------------------------------------------------------------
 *		CUDA error-checking helper
 * ----------------------------------------------------------------
 */
#define CUDA_CHECK(call, cleanup)									\
	do {															\
		cudaError_t _err = (call);									\
		if (_err != cudaSuccess) {									\
			cleanup;												\
			return GPU_OPS_ERR_LAUNCH;								\
		}															\
	} while (0)

/* ----------------------------------------------------------------
 *		Batch L2 distance kernel
 *
 * One thread per candidate vector.  The query is stored in constant
 * or global memory (small enough to fit in L1 cache on most GPUs).
 * ----------------------------------------------------------------
 */
__global__ void
kernel_batch_l2(const float *query,
				const float *candidates,
				int dim,
				int n_candidates,
				float *out)
{
	int		tid = blockIdx.x * blockDim.x + threadIdx.x;

	if (tid >= n_candidates)
		return;

	const float *cand = candidates + (size_t) tid * dim;
	float		sum = 0.0f;

	for (int d = 0; d < dim; d++)
	{
		float	diff = query[d] - cand[d];
		sum += diff * diff;
	}

	out[tid] = sqrtf(sum);
}

extern "C" GpuOpsStatus
gpu_ops_batch_l2(const float *query,
				 const float *candidates,
				 int dim,
				 int n_candidates,
				 float *out)
{
	float	   *d_query = NULL;
	float	   *d_candidates = NULL;
	float	   *d_out = NULL;
	size_t		query_bytes = sizeof(float) * dim;
	size_t		cand_bytes = sizeof(float) * (size_t) n_candidates * dim;
	size_t		out_bytes = sizeof(float) * n_candidates;
	int			grid_size;

	if (!query || !candidates || !out || dim <= 0 || n_candidates <= 0)
		return GPU_OPS_ERR_INVALID_ARG;

	/* Allocate device memory */
	CUDA_CHECK(cudaMalloc(&d_query, query_bytes), (void)0);
	CUDA_CHECK(cudaMalloc(&d_candidates, cand_bytes),
			   cudaFree(d_query));
	CUDA_CHECK(cudaMalloc(&d_out, out_bytes),
			   (cudaFree(d_query), cudaFree(d_candidates)));

	/* Copy input to device */
	CUDA_CHECK(cudaMemcpy(d_query, query, query_bytes, cudaMemcpyHostToDevice),
			   (cudaFree(d_query), cudaFree(d_candidates), cudaFree(d_out)));
	CUDA_CHECK(cudaMemcpy(d_candidates, candidates, cand_bytes, cudaMemcpyHostToDevice),
			   (cudaFree(d_query), cudaFree(d_candidates), cudaFree(d_out)));

	/* Launch kernel */
	grid_size = (n_candidates + GPU_BLOCK_SIZE - 1) / GPU_BLOCK_SIZE;
	kernel_batch_l2<<<grid_size, GPU_BLOCK_SIZE>>>(
		d_query, d_candidates, dim, n_candidates, d_out);

	CUDA_CHECK(cudaGetLastError(),
			   (cudaFree(d_query), cudaFree(d_candidates), cudaFree(d_out)));
	CUDA_CHECK(cudaDeviceSynchronize(),
			   (cudaFree(d_query), cudaFree(d_candidates), cudaFree(d_out)));

	/* Copy results back */
	CUDA_CHECK(cudaMemcpy(out, d_out, out_bytes, cudaMemcpyDeviceToHost),
			   (cudaFree(d_query), cudaFree(d_candidates), cudaFree(d_out)));

	/* Free device memory */
	cudaFree(d_query);
	cudaFree(d_candidates);
	cudaFree(d_out);

	return GPU_OPS_OK;
}

/* ----------------------------------------------------------------
 *		Batch cosine distance kernel
 * ----------------------------------------------------------------
 */
__global__ void
kernel_batch_cosine(const float *query,
					const float *candidates,
					int dim,
					int n_candidates,
					float query_norm,
					float *out)
{
	int		tid = blockIdx.x * blockDim.x + threadIdx.x;

	if (tid >= n_candidates)
		return;

	const float *cand = candidates + (size_t) tid * dim;
	float		dot = 0.0f;
	float		cand_norm_sq = 0.0f;

	for (int d = 0; d < dim; d++)
	{
		dot += query[d] * cand[d];
		cand_norm_sq += cand[d] * cand[d];
	}

	float	denom = query_norm * sqrtf(cand_norm_sq);

	if (denom < 1e-38f)
	{
		out[tid] = 1.0f;
		return;
	}

	float	similarity = dot / denom;

	/* Clamp */
	if (similarity > 1.0f)
		similarity = 1.0f;
	if (similarity < -1.0f)
		similarity = -1.0f;

	out[tid] = 1.0f - similarity;
}

extern "C" GpuOpsStatus
gpu_ops_batch_cosine(const float *query,
					 const float *candidates,
					 int dim,
					 int n_candidates,
					 float *out)
{
	float	   *d_query = NULL;
	float	   *d_candidates = NULL;
	float	   *d_out = NULL;
	size_t		query_bytes = sizeof(float) * dim;
	size_t		cand_bytes = sizeof(float) * (size_t) n_candidates * dim;
	size_t		out_bytes = sizeof(float) * n_candidates;
	float		query_norm;
	int			grid_size;

	if (!query || !candidates || !out || dim <= 0 || n_candidates <= 0)
		return GPU_OPS_ERR_INVALID_ARG;

	/* Precompute query norm on CPU (single vector, cheap) */
	{
		double	norm_sq = 0.0;

		for (int d = 0; d < dim; d++)
			norm_sq += (double) query[d] * (double) query[d];
		query_norm = (float) sqrt(norm_sq);
	}

	/* Allocate device memory */
	CUDA_CHECK(cudaMalloc(&d_query, query_bytes), (void)0);
	CUDA_CHECK(cudaMalloc(&d_candidates, cand_bytes),
			   cudaFree(d_query));
	CUDA_CHECK(cudaMalloc(&d_out, out_bytes),
			   (cudaFree(d_query), cudaFree(d_candidates)));

	/* Copy input to device */
	CUDA_CHECK(cudaMemcpy(d_query, query, query_bytes, cudaMemcpyHostToDevice),
			   (cudaFree(d_query), cudaFree(d_candidates), cudaFree(d_out)));
	CUDA_CHECK(cudaMemcpy(d_candidates, candidates, cand_bytes, cudaMemcpyHostToDevice),
			   (cudaFree(d_query), cudaFree(d_candidates), cudaFree(d_out)));

	/* Launch kernel */
	grid_size = (n_candidates + GPU_BLOCK_SIZE - 1) / GPU_BLOCK_SIZE;
	kernel_batch_cosine<<<grid_size, GPU_BLOCK_SIZE>>>(
		d_query, d_candidates, dim, n_candidates, query_norm, d_out);

	CUDA_CHECK(cudaGetLastError(),
			   (cudaFree(d_query), cudaFree(d_candidates), cudaFree(d_out)));
	CUDA_CHECK(cudaDeviceSynchronize(),
			   (cudaFree(d_query), cudaFree(d_candidates), cudaFree(d_out)));

	/* Copy results back */
	CUDA_CHECK(cudaMemcpy(out, d_out, out_bytes, cudaMemcpyDeviceToHost),
			   (cudaFree(d_query), cudaFree(d_candidates), cudaFree(d_out)));

	cudaFree(d_query);
	cudaFree(d_candidates);
	cudaFree(d_out);

	return GPU_OPS_OK;
}

/* ----------------------------------------------------------------
 *		Top-K selection kernel
 *
 * Strategy: compute all distances on GPU, copy back, then do
 * partial sort on CPU.  For truly huge candidate sets a GPU-side
 * radix-select would be better, but this is simpler and still
 * offloads the expensive distance computation.
 * ----------------------------------------------------------------
 */

static int
topk_cmp(const void *pa, const void *pb)
{
	const GpuTopKEntry *a = (const GpuTopKEntry *) pa;
	const GpuTopKEntry *b = (const GpuTopKEntry *) pb;

	if (a->distance < b->distance)
		return -1;
	if (a->distance > b->distance)
		return 1;
	return 0;
}

extern "C" GpuOpsStatus
gpu_ops_topk(const float *query,
			 const float *candidates,
			 int dim,
			 int n_candidates,
			 int k,
			 GpuTopKEntry *out,
			 int *out_k)
{
	float	   *distances;
	int			actual_k;
	GpuOpsStatus status;

	if (!query || !candidates || !out || !out_k ||
		dim <= 0 || n_candidates <= 0 || k <= 0)
		return GPU_OPS_ERR_INVALID_ARG;

	actual_k = (k < n_candidates) ? k : n_candidates;

	/* Compute all L2 distances on GPU */
	distances = (float *) malloc(sizeof(float) * n_candidates);
	if (!distances)
		return GPU_OPS_ERR_ALLOC;

	status = gpu_ops_batch_l2(query, candidates, dim, n_candidates, distances);
	if (status != GPU_OPS_OK)
	{
		free(distances);
		return status;
	}

	/* Build entries and partial-sort on CPU */
	{
		GpuTopKEntry *entries;
		int			i;

		entries = (GpuTopKEntry *) malloc(sizeof(GpuTopKEntry) * n_candidates);
		if (!entries)
		{
			free(distances);
			return GPU_OPS_ERR_ALLOC;
		}

		for (i = 0; i < n_candidates; i++)
		{
			entries[i].idx = i;
			entries[i].distance = distances[i];
		}

		qsort(entries, n_candidates, sizeof(GpuTopKEntry), topk_cmp);
		memcpy(out, entries, sizeof(GpuTopKEntry) * actual_k);

		free(entries);
	}

	free(distances);

	*out_k = actual_k;
	return GPU_OPS_OK;
}

/* ----------------------------------------------------------------
 *		Matrix multiply kernel
 *
 *	  Simple tiled implementation.  For production use, calling
 *	  cuBLAS sgemm would be preferable; this avoids the cuBLAS
 *	  dependency for the extension.
 * ----------------------------------------------------------------
 */
#define TILE_SIZE	16

__global__ void
kernel_matmul(const float *a, const float *b, float *c,
			  int m, int p, int n)
{
	__shared__ float tileA[TILE_SIZE][TILE_SIZE];
	__shared__ float tileB[TILE_SIZE][TILE_SIZE];

	int		row = blockIdx.y * TILE_SIZE + threadIdx.y;
	int		col = blockIdx.x * TILE_SIZE + threadIdx.x;
	float	sum = 0.0f;

	for (int t = 0; t < (p + TILE_SIZE - 1) / TILE_SIZE; t++)
	{
		int		tCol = t * TILE_SIZE + threadIdx.x;
		int		tRow = t * TILE_SIZE + threadIdx.y;

		/* Load tile of A */
		if (row < m && tCol < p)
			tileA[threadIdx.y][threadIdx.x] = a[row * p + tCol];
		else
			tileA[threadIdx.y][threadIdx.x] = 0.0f;

		/* Load tile of B */
		if (tRow < p && col < n)
			tileB[threadIdx.y][threadIdx.x] = b[tRow * n + col];
		else
			tileB[threadIdx.y][threadIdx.x] = 0.0f;

		__syncthreads();

		for (int k = 0; k < TILE_SIZE; k++)
			sum += tileA[threadIdx.y][k] * tileB[k][threadIdx.x];

		__syncthreads();
	}

	if (row < m && col < n)
		c[row * n + col] = sum;
}

extern "C" GpuOpsStatus
gpu_ops_matmul(const float *a, int m, int p,
			   const float *b, int p2, int n,
			   float *out)
{
	float	   *d_a = NULL;
	float	   *d_b = NULL;
	float	   *d_c = NULL;
	size_t		a_bytes = sizeof(float) * m * p;
	size_t		b_bytes = sizeof(float) * p * n;
	size_t		c_bytes = sizeof(float) * m * n;
	dim3		block(TILE_SIZE, TILE_SIZE);
	dim3		grid((n + TILE_SIZE - 1) / TILE_SIZE,
					 (m + TILE_SIZE - 1) / TILE_SIZE);

	if (!a || !b || !out)
		return GPU_OPS_ERR_INVALID_ARG;
	if (m <= 0 || p <= 0 || n <= 0)
		return GPU_OPS_ERR_INVALID_ARG;
	if (p != p2)
		return GPU_OPS_ERR_INVALID_ARG;

	/* Allocate */
	CUDA_CHECK(cudaMalloc(&d_a, a_bytes), (void)0);
	CUDA_CHECK(cudaMalloc(&d_b, b_bytes), cudaFree(d_a));
	CUDA_CHECK(cudaMalloc(&d_c, c_bytes),
			   (cudaFree(d_a), cudaFree(d_b)));

	/* Copy inputs */
	CUDA_CHECK(cudaMemcpy(d_a, a, a_bytes, cudaMemcpyHostToDevice),
			   (cudaFree(d_a), cudaFree(d_b), cudaFree(d_c)));
	CUDA_CHECK(cudaMemcpy(d_b, b, b_bytes, cudaMemcpyHostToDevice),
			   (cudaFree(d_a), cudaFree(d_b), cudaFree(d_c)));

	/* Launch */
	kernel_matmul<<<grid, block>>>(d_a, d_b, d_c, m, p, n);

	CUDA_CHECK(cudaGetLastError(),
			   (cudaFree(d_a), cudaFree(d_b), cudaFree(d_c)));
	CUDA_CHECK(cudaDeviceSynchronize(),
			   (cudaFree(d_a), cudaFree(d_b), cudaFree(d_c)));

	/* Copy result */
	CUDA_CHECK(cudaMemcpy(out, d_c, c_bytes, cudaMemcpyDeviceToHost),
			   (cudaFree(d_a), cudaFree(d_b), cudaFree(d_c)));

	cudaFree(d_a);
	cudaFree(d_b);
	cudaFree(d_c);

	return GPU_OPS_OK;
}
