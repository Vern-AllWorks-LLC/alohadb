/*-------------------------------------------------------------------------
 *
 * gpu_cpu_fallback.c
 *	  CPU fallback implementations of the GPU vector ops API.
 *
 *	  These routines are linked when CUDA is not available (i.e. when
 *	  HAVE_CUDA is not defined).  They use SIMD (AVX2 on x86-64, NEON
 *	  on ARM64) wherever possible, mirroring the patterns in
 *	  src/backend/utils/adt/vector.c.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_gpu/gpu_cpu_fallback.c
 *
 *-------------------------------------------------------------------------
 */
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <float.h>

#include "gpu_vector_ops.h"

/* ----------------------------------------------------------------
 *		SIMD detection (same logic as vector.c)
 * ----------------------------------------------------------------
 */
#if defined(__x86_64__) || defined(_M_AMD64)
#ifdef __AVX2__
#include <immintrin.h>
#define USE_AVX2_FALLBACK
#endif
#endif

#if defined(__aarch64__) && defined(__ARM_NEON)
#include <arm_neon.h>
#define USE_NEON_FALLBACK
#endif

/* ----------------------------------------------------------------
 *		Lifecycle (no-ops for CPU)
 * ----------------------------------------------------------------
 */

GpuOpsStatus
gpu_ops_init(int device_id)
{
	/* CPU fallback has nothing to initialise. */
	(void) device_id;
	return GPU_OPS_OK;
}

bool
gpu_ops_available(void)
{
	/* No CUDA device available in the CPU-only build. */
	return false;
}

/* ----------------------------------------------------------------
 *		Internal SIMD helpers
 * ----------------------------------------------------------------
 */

/*
 * Dot product of two float arrays of length dim.
 */
static inline double
cpu_dot_product(const float *a, const float *b, int dim)
{
	double		result = 0.0;
	int			i = 0;

#ifdef USE_AVX2_FALLBACK
	{
		__m256		sum = _mm256_setzero_ps();

		for (; i + 7 < dim; i += 8)
		{
			__m256		va = _mm256_loadu_ps(&a[i]);
			__m256		vb = _mm256_loadu_ps(&b[i]);

			sum = _mm256_fmadd_ps(va, vb, sum);
		}

		/* Horizontal sum of 8 floats */
		__m128		lo = _mm256_castps256_ps128(sum);
		__m128		hi = _mm256_extractf128_ps(sum, 1);

		lo = _mm_add_ps(lo, hi);
		lo = _mm_hadd_ps(lo, lo);
		lo = _mm_hadd_ps(lo, lo);
		result = (double) _mm_cvtss_f32(lo);
	}
#elif defined(USE_NEON_FALLBACK)
	{
		float32x4_t sum = vdupq_n_f32(0.0f);

		for (; i + 3 < dim; i += 4)
		{
			float32x4_t va = vld1q_f32(&a[i]);
			float32x4_t vb = vld1q_f32(&b[i]);

			sum = vfmaq_f32(sum, va, vb);
		}

		result = (double) vaddvq_f32(sum);
	}
#endif

	/* Scalar tail */
	for (; i < dim; i++)
		result += (double) a[i] * (double) b[i];

	return result;
}

/*
 * L2 squared distance between two float arrays of length dim.
 */
static inline double
cpu_l2_squared(const float *a, const float *b, int dim)
{
	double		result = 0.0;
	int			i = 0;

#ifdef USE_AVX2_FALLBACK
	{
		__m256		sum = _mm256_setzero_ps();

		for (; i + 7 < dim; i += 8)
		{
			__m256		va = _mm256_loadu_ps(&a[i]);
			__m256		vb = _mm256_loadu_ps(&b[i]);
			__m256		diff = _mm256_sub_ps(va, vb);

			sum = _mm256_fmadd_ps(diff, diff, sum);
		}

		__m128		lo = _mm256_castps256_ps128(sum);
		__m128		hi = _mm256_extractf128_ps(sum, 1);

		lo = _mm_add_ps(lo, hi);
		lo = _mm_hadd_ps(lo, lo);
		lo = _mm_hadd_ps(lo, lo);
		result = (double) _mm_cvtss_f32(lo);
	}
#elif defined(USE_NEON_FALLBACK)
	{
		float32x4_t sum = vdupq_n_f32(0.0f);

		for (; i + 3 < dim; i += 4)
		{
			float32x4_t va = vld1q_f32(&a[i]);
			float32x4_t vb = vld1q_f32(&b[i]);
			float32x4_t diff = vsubq_f32(va, vb);

			sum = vfmaq_f32(sum, diff, diff);
		}

		result = (double) vaddvq_f32(sum);
	}
#endif

	/* Scalar tail */
	for (; i < dim; i++)
	{
		double		diff = (double) a[i] - (double) b[i];

		result += diff * diff;
	}

	return result;
}

/*
 * Squared L2 norm of a float array.
 */
static inline double
cpu_norm_squared(const float *a, int dim)
{
	return cpu_dot_product(a, a, dim);
}

/* ----------------------------------------------------------------
 *		Batch distance implementations
 * ----------------------------------------------------------------
 */

GpuOpsStatus
gpu_ops_batch_l2(const float *query,
				 const float *candidates,
				 int dim,
				 int n_candidates,
				 float *out)
{
	int			i;

	if (!query || !candidates || !out || dim <= 0 || n_candidates <= 0)
		return GPU_OPS_ERR_INVALID_ARG;

	for (i = 0; i < n_candidates; i++)
	{
		const float *cand = candidates + (size_t) i * dim;
		double		dist_sq = cpu_l2_squared(query, cand, dim);

		out[i] = (float) sqrt(dist_sq);
	}

	return GPU_OPS_OK;
}

GpuOpsStatus
gpu_ops_batch_cosine(const float *query,
					 const float *candidates,
					 int dim,
					 int n_candidates,
					 float *out)
{
	double		query_norm_sq;
	int			i;

	if (!query || !candidates || !out || dim <= 0 || n_candidates <= 0)
		return GPU_OPS_ERR_INVALID_ARG;

	query_norm_sq = cpu_norm_squared(query, dim);

	for (i = 0; i < n_candidates; i++)
	{
		const float *cand = candidates + (size_t) i * dim;
		double		dot = cpu_dot_product(query, cand, dim);
		double		cand_norm_sq = cpu_norm_squared(cand, dim);
		double		denom = sqrt(query_norm_sq) * sqrt(cand_norm_sq);
		double		similarity;

		if (denom < DBL_EPSILON)
		{
			out[i] = 1.0f;
			continue;
		}

		similarity = dot / denom;

		/* Clamp to [-1, 1] for floating-point safety */
		if (similarity > 1.0)
			similarity = 1.0;
		if (similarity < -1.0)
			similarity = -1.0;

		out[i] = (float) (1.0 - similarity);
	}

	return GPU_OPS_OK;
}

/* ----------------------------------------------------------------
 *		Top-K via partial selection (quickselect partition)
 * ----------------------------------------------------------------
 */

/*
 * Comparison function for qsort on GpuTopKEntry by distance ascending.
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

GpuOpsStatus
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
	int			i;

	if (!query || !candidates || !out || !out_k ||
		dim <= 0 || n_candidates <= 0 || k <= 0)
		return GPU_OPS_ERR_INVALID_ARG;

	actual_k = (k < n_candidates) ? k : n_candidates;

	/* Compute all L2 distances first */
	distances = (float *) malloc(sizeof(float) * n_candidates);
	if (!distances)
		return GPU_OPS_ERR_ALLOC;

	for (i = 0; i < n_candidates; i++)
	{
		const float *cand = candidates + (size_t) i * dim;
		double		dist_sq = cpu_l2_squared(query, cand, dim);

		distances[i] = (float) sqrt(dist_sq);
	}

	/*
	 * Build (idx, distance) pairs and partial sort.
	 *
	 * For simplicity we use a max-heap of size k.  When the number of
	 * candidates is modest (which it usually is after an index scan) this
	 * is perfectly adequate and avoids a full sort.
	 *
	 * For the fallback we just build the full array and sort it; the
	 * overhead of a full sort vs. partial select is acceptable for a
	 * CPU-only path.
	 */
	{
		GpuTopKEntry *entries;

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
 *		Matrix multiplication (row-major)
 * ----------------------------------------------------------------
 */

GpuOpsStatus
gpu_ops_matmul(const float *a, int m, int p,
			   const float *b, int p2, int n,
			   float *out)
{
	int			i,
				j,
				kk;

	if (!a || !b || !out)
		return GPU_OPS_ERR_INVALID_ARG;
	if (m <= 0 || p <= 0 || n <= 0)
		return GPU_OPS_ERR_INVALID_ARG;
	if (p != p2)
		return GPU_OPS_ERR_INVALID_ARG;

	/*
	 * Straightforward i-k-j (row-major friendly) loop with SIMD for the
	 * innermost accumulation where possible.
	 */
	memset(out, 0, sizeof(float) * m * n);

	for (i = 0; i < m; i++)
	{
		for (kk = 0; kk < p; kk++)
		{
			float		a_ik = a[i * p + kk];
			int			jj = 0;

#ifdef USE_AVX2_FALLBACK
			{
				__m256		va = _mm256_set1_ps(a_ik);

				for (; jj + 7 < n; jj += 8)
				{
					__m256		vb = _mm256_loadu_ps(&b[kk * n + jj]);
					__m256		vc = _mm256_loadu_ps(&out[i * n + jj]);

					vc = _mm256_fmadd_ps(va, vb, vc);
					_mm256_storeu_ps(&out[i * n + jj], vc);
				}
			}
#elif defined(USE_NEON_FALLBACK)
			{
				float32x4_t va = vdupq_n_f32(a_ik);

				for (; jj + 3 < n; jj += 4)
				{
					float32x4_t vb = vld1q_f32(&b[kk * n + jj]);
					float32x4_t vc = vld1q_f32(&out[i * n + jj]);

					vc = vfmaq_f32(vc, va, vb);
					vst1q_f32(&out[i * n + jj], vc);
				}
			}
#endif

			/* Scalar tail */
			for (; jj < n; jj++)
				out[i * n + jj] += a_ik * b[kk * n + jj];
		}
	}

	return GPU_OPS_OK;
}
