/*-------------------------------------------------------------------------
 *
 * vector.c
 *	  Functions for the built-in vector data type.
 *
 *	  The vector type stores a fixed-dimension array of float4 values,
 *	  intended for embedding storage and similarity search. It supports
 *	  L2, cosine, inner product, and L1 distance computations with
 *	  SIMD acceleration on x86-64 (SSE2/AVX2) and ARM64 (NEON).
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/vector.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>
#include <float.h>

#include "catalog/pg_type.h"
#include "common/shortest_dec.h"
#include "libpq/pqformat.h"
#include "port/pg_bitutils.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/float.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/vector.h"

/* Try to use AVX2 if available on x86-64 */
#if defined(__x86_64__) || defined(_M_AMD64)
#ifdef __AVX2__
#include <immintrin.h>
#define USE_AVX2_VECTOR
#endif
#endif

/* ARM NEON */
#if defined(__aarch64__) && defined(__ARM_NEON)
#include <arm_neon.h>
#define USE_NEON_VECTOR
#endif

/*
 * Check that two vectors have the same number of dimensions.
 */
static inline void
vector_check_dims(const Vector *a, const Vector *b)
{
	if (a->dim != b->dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("different vector dimensions %d and %d", a->dim, b->dim)));
}

/*
 * Check that dimensions are valid.
 */
static inline void
vector_check_dim(int dim)
{
	if (dim < 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("vector must have at least 1 dimension")));
	if (dim > VECTOR_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("vector cannot have more than %d dimensions", VECTOR_MAX_DIM)));
}

/*
 * Check that a float value is finite (not NaN or Inf).
 */
static inline void
vector_check_value(float val)
{
	if (isnan(val))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("NaN not allowed in vector value")));
	if (isinf(val))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("infinite value not allowed in vector value")));
}

/* ----------------------------------------------------------------
 *		I/O functions
 * ----------------------------------------------------------------
 */

/*
 * vector_in - converts "[1.0, 2.0, 3.0]" text representation to internal form.
 */
Datum
vector_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
	int32		typmod = PG_GETARG_INT32(2);
	Vector	   *result;
	float	   *values;
	int			dim = 0;
	int			maxdim = 256;	/* initial allocation */
	char	   *ptr = str;
	char	   *end;

	/* Allocate temporary array for parsing */
	values = (float *) palloc(sizeof(float) * maxdim);

	/* Skip leading whitespace */
	while (isspace((unsigned char) *ptr))
		ptr++;

	/* Expect opening bracket */
	if (*ptr != '[')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("malformed vector literal: \"%s\"", str),
				 errdetail("Vector contents must start with \"[\".")));
	ptr++;

	/* Parse comma-separated float values */
	while (true)
	{
		float		val;

		/* Skip whitespace */
		while (isspace((unsigned char) *ptr))
			ptr++;

		/* Check for end of vector */
		if (*ptr == ']')
		{
			ptr++;
			break;
		}

		/* Need comma between values (except before first) */
		if (dim > 0)
		{
			if (*ptr != ',')
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("malformed vector literal: \"%s\"", str),
						 errdetail("Unexpected character \"%c\".", *ptr)));
			ptr++;
			while (isspace((unsigned char) *ptr))
				ptr++;
		}

		/* Parse the float value */
		errno = 0;
		val = strtof(ptr, &end);
		if (ptr == end)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("malformed vector literal: \"%s\"", str),
					 errdetail("Expected a numeric value.")));
		ptr = end;

		vector_check_value(val);

		/* Grow array if needed */
		if (dim >= maxdim)
		{
			maxdim *= 2;
			if (maxdim > VECTOR_MAX_DIM)
				maxdim = VECTOR_MAX_DIM + 1;	/* will trigger check below */
			values = (float *) repalloc(values, sizeof(float) * maxdim);
		}

		values[dim++] = val;
	}

	/* Skip trailing whitespace */
	while (isspace((unsigned char) *ptr))
		ptr++;

	/* Should be at end of string */
	if (*ptr != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("malformed vector literal: \"%s\"", str),
				 errdetail("Junk after closing bracket.")));

	vector_check_dim(dim);

	/* Check typmod if specified */
	if (typmod != -1 && typmod != dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("expected %d dimensions, not %d", typmod, dim)));

	/* Build result */
	result = InitVector(dim);
	memcpy(result->x, values, sizeof(float) * dim);

	pfree(values);

	PG_RETURN_VECTOR_P(result);
}

/*
 * vector_out - converts internal form to "[1, 2, 3]" text representation.
 */
Datum
vector_out(PG_FUNCTION_ARGS)
{
	Vector	   *v = PG_GETARG_VECTOR_P(0);
	StringInfoData buf;
	int			i;

	initStringInfo(&buf);
	appendStringInfoChar(&buf, '[');

	for (i = 0; i < v->dim; i++)
	{
		if (i > 0)
			appendStringInfoString(&buf, ",");
		appendStringInfo(&buf, "%g", v->x[i]);
	}

	appendStringInfoChar(&buf, ']');

	PG_RETURN_CSTRING(buf.data);
}

/*
 * vector_recv - converts external binary format to internal.
 */
Datum
vector_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	int32		typmod = PG_GETARG_INT32(2);
	Vector	   *result;
	int			dim;
	int			i;

	dim = pq_getmsgint(buf, 2);
	(void) pq_getmsgint(buf, 2);	/* unused field */

	vector_check_dim(dim);

	if (typmod != -1 && typmod != dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("expected %d dimensions, not %d", typmod, dim)));

	result = InitVector(dim);

	for (i = 0; i < dim; i++)
	{
		result->x[i] = pq_getmsgfloat4(buf);
		vector_check_value(result->x[i]);
	}

	PG_RETURN_VECTOR_P(result);
}

/*
 * vector_send - converts internal to external binary format.
 */
Datum
vector_send(PG_FUNCTION_ARGS)
{
	Vector	   *v = PG_GETARG_VECTOR_P(0);
	StringInfoData buf;
	int			i;

	pq_begintypsend(&buf);
	pq_sendint16(&buf, v->dim);
	pq_sendint16(&buf, v->unused);

	for (i = 0; i < v->dim; i++)
		pq_sendfloat4(&buf, v->x[i]);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/* ----------------------------------------------------------------
 *		Typmod functions
 * ----------------------------------------------------------------
 */

/*
 * vector_typmod_in - parse dimension from type modifier, e.g. vector(768)
 */
Datum
vector_typmod_in(PG_FUNCTION_ARGS)
{
	ArrayType  *ta = PG_GETARG_ARRAYTYPE_P(0);
	int32	   *tl;
	int			n;
	int32		typmod;

	tl = ArrayGetIntegerTypmods(ta, &n);

	if (n != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid type modifier"),
				 errdetail("vector type modifier must be a single integer (dimension count).")));

	typmod = tl[0];
	vector_check_dim(typmod);

	PG_RETURN_INT32(typmod);
}

/*
 * vector_typmod_out - format dimension as string
 */
Datum
vector_typmod_out(PG_FUNCTION_ARGS)
{
	int32		typmod = PG_GETARG_INT32(0);
	char	   *result;

	result = psprintf("(%d)", typmod);

	PG_RETURN_CSTRING(result);
}

/* ----------------------------------------------------------------
 *		Distance functions (with SIMD acceleration)
 * ----------------------------------------------------------------
 */

/*
 * Dot product of two float arrays.
 * This is the core primitive; L2, cosine, and IP all build on it.
 */
static inline double
dot_product_internal(const float *a, const float *b, int dim)
{
	double		result = 0.0;
	int			i = 0;

#ifdef USE_AVX2_VECTOR
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
#elif defined(USE_NEON_VECTOR)
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
 * L2 squared distance between two float arrays.
 */
static inline double
l2_squared_internal(const float *a, const float *b, int dim)
{
	double		result = 0.0;
	int			i = 0;

#ifdef USE_AVX2_VECTOR
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
#elif defined(USE_NEON_VECTOR)
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
 * Public distance functions used by index AMs and SQL operators.
 */

double
vector_l2_squared_distance(const Vector *a, const Vector *b)
{
	return l2_squared_internal(a->x, b->x, a->dim);
}

double
vector_l2_distance(const Vector *a, const Vector *b)
{
	return sqrt(l2_squared_internal(a->x, b->x, a->dim));
}

double
vector_dot_product(const Vector *a, const Vector *b)
{
	return dot_product_internal(a->x, b->x, a->dim);
}

double
vector_inner_product(const Vector *a, const Vector *b)
{
	/* Return negative so that ORDER BY ascending = most similar first */
	return -dot_product_internal(a->x, b->x, a->dim);
}

double
vector_cosine_distance(const Vector *a, const Vector *b)
{
	double		dot = dot_product_internal(a->x, b->x, a->dim);
	double		norma = dot_product_internal(a->x, a->x, a->dim);
	double		normb = dot_product_internal(b->x, b->x, b->dim);
	double		denom;

	denom = sqrt(norma) * sqrt(normb);

	if (denom < DBL_EPSILON)
		return 1.0;				/* zero vectors treated as maximally distant */

	/* Clamp to [0, 2] to handle floating-point rounding */
	double		similarity = dot / denom;

	if (similarity > 1.0)
		similarity = 1.0;
	if (similarity < -1.0)
		similarity = -1.0;

	return 1.0 - similarity;
}

double
vector_l1_distance(const Vector *a, const Vector *b)
{
	double		result = 0.0;
	int			i;

	for (i = 0; i < a->dim; i++)
		result += fabs((double) a->x[i] - (double) b->x[i]);

	return result;
}

double
vector_norm(const Vector *a)
{
	return sqrt(dot_product_internal(a->x, a->x, a->dim));
}

/* ----------------------------------------------------------------
 *		SQL-callable distance operator functions
 * ----------------------------------------------------------------
 */

/*
 * vector_l2_distance_op - implements <-> operator
 */
Datum
vector_l2_distance_op(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);

	vector_check_dims(a, b);

	PG_RETURN_FLOAT8(vector_l2_distance(a, b));
}

/*
 * vector_cosine_distance_op - implements <=> operator
 */
Datum
vector_cosine_distance_op(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);

	vector_check_dims(a, b);

	PG_RETURN_FLOAT8(vector_cosine_distance(a, b));
}

/*
 * vector_ip_distance_op - implements <#> operator (negative inner product)
 */
Datum
vector_ip_distance_op(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);

	vector_check_dims(a, b);

	PG_RETURN_FLOAT8(vector_inner_product(a, b));
}

/*
 * vector_l1_distance_op - implements <+> operator
 */
Datum
vector_l1_distance_op(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);

	vector_check_dims(a, b);

	PG_RETURN_FLOAT8(vector_l1_distance(a, b));
}

/* ----------------------------------------------------------------
 *		Utility functions
 * ----------------------------------------------------------------
 */

/*
 * vector_dims - returns the number of dimensions
 */
Datum
vector_dims_func(PG_FUNCTION_ARGS)
{
	Vector	   *v = PG_GETARG_VECTOR_P(0);

	PG_RETURN_INT32(v->dim);
}

/*
 * vector_norm_func - returns the L2 norm
 */
Datum
vector_norm_func(PG_FUNCTION_ARGS)
{
	Vector	   *v = PG_GETARG_VECTOR_P(0);

	PG_RETURN_FLOAT8(vector_norm(v));
}

/*
 * vector_add - element-wise addition
 */
Datum
vector_add(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	Vector	   *result;
	int			i;

	vector_check_dims(a, b);

	result = InitVector(a->dim);

	for (i = 0; i < a->dim; i++)
	{
		result->x[i] = a->x[i] + b->x[i];
		vector_check_value(result->x[i]);
	}

	PG_RETURN_VECTOR_P(result);
}

/*
 * vector_sub - element-wise subtraction
 */
Datum
vector_sub(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	Vector	   *result;
	int			i;

	vector_check_dims(a, b);

	result = InitVector(a->dim);

	for (i = 0; i < a->dim; i++)
	{
		result->x[i] = a->x[i] - b->x[i];
		vector_check_value(result->x[i]);
	}

	PG_RETURN_VECTOR_P(result);
}

/*
 * vector_mul - element-wise multiplication
 */
Datum
vector_mul(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	Vector	   *result;
	int			i;

	vector_check_dims(a, b);

	result = InitVector(a->dim);

	for (i = 0; i < a->dim; i++)
	{
		result->x[i] = a->x[i] * b->x[i];
		vector_check_value(result->x[i]);
	}

	PG_RETURN_VECTOR_P(result);
}

/*
 * vector_eq - equality comparison
 */
Datum
vector_eq(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	int			i;

	if (a->dim != b->dim)
		PG_RETURN_BOOL(false);

	for (i = 0; i < a->dim; i++)
	{
		if (a->x[i] != b->x[i])
			PG_RETURN_BOOL(false);
	}

	PG_RETURN_BOOL(true);
}

/*
 * vector_ne - inequality comparison
 */
Datum
vector_ne(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	int			i;

	if (a->dim != b->dim)
		PG_RETURN_BOOL(true);

	for (i = 0; i < a->dim; i++)
	{
		if (a->x[i] != b->x[i])
			PG_RETURN_BOOL(true);
	}

	PG_RETURN_BOOL(false);
}

/*
 * vector_lt / vector_le / vector_gt / vector_ge - lexicographic comparison
 * for btree indexing support (not for similarity search).
 */
static int
vector_cmp_internal(const Vector *a, const Vector *b)
{
	int			mindim = Min(a->dim, b->dim);
	int			i;

	for (i = 0; i < mindim; i++)
	{
		if (a->x[i] < b->x[i])
			return -1;
		if (a->x[i] > b->x[i])
			return 1;
	}

	if (a->dim < b->dim)
		return -1;
	if (a->dim > b->dim)
		return 1;

	return 0;
}

Datum
vector_lt(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);

	PG_RETURN_BOOL(vector_cmp_internal(a, b) < 0);
}

Datum
vector_le(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);

	PG_RETURN_BOOL(vector_cmp_internal(a, b) <= 0);
}

Datum
vector_gt(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);

	PG_RETURN_BOOL(vector_cmp_internal(a, b) > 0);
}

Datum
vector_ge(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);

	PG_RETURN_BOOL(vector_cmp_internal(a, b) >= 0);
}

Datum
vector_cmp(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);

	PG_RETURN_INT32(vector_cmp_internal(a, b));
}

/*
 * vector_normalize - returns a unit vector (L2 norm = 1)
 */
Datum
vector_normalize(PG_FUNCTION_ARGS)
{
	Vector	   *v = PG_GETARG_VECTOR_P(0);
	Vector	   *result;
	double		norm;
	int			i;

	norm = vector_norm(v);

	result = InitVector(v->dim);

	if (norm < DBL_EPSILON)
	{
		/* Zero vector stays zero */
		memset(result->x, 0, sizeof(float) * v->dim);
	}
	else
	{
		for (i = 0; i < v->dim; i++)
			result->x[i] = (float) ((double) v->x[i] / norm);
	}

	PG_RETURN_VECTOR_P(result);
}

/*
 * array_to_vector - convert float4 array to vector
 */
Datum
array_to_vector(PG_FUNCTION_ARGS)
{
	ArrayType  *arr = PG_GETARG_ARRAYTYPE_P(0);
	Vector	   *result;
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;
	int			i;

	deconstruct_array(arr, FLOAT4OID, sizeof(float4), true, TYPALIGN_INT,
					  &elems, &nulls, &nelems);

	vector_check_dim(nelems);

	result = InitVector(nelems);

	for (i = 0; i < nelems; i++)
	{
		if (nulls[i])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("NULL not allowed in vector value")));
		result->x[i] = DatumGetFloat4(elems[i]);
		vector_check_value(result->x[i]);
	}

	pfree(elems);
	pfree(nulls);

	PG_RETURN_VECTOR_P(result);
}

/*
 * vector_to_array - convert vector to float4 array
 */
Datum
vector_to_array(PG_FUNCTION_ARGS)
{
	Vector	   *v = PG_GETARG_VECTOR_P(0);
	Datum	   *elems;
	ArrayType  *result;
	int			i;

	elems = (Datum *) palloc(sizeof(Datum) * v->dim);

	for (i = 0; i < v->dim; i++)
		elems[i] = Float4GetDatum(v->x[i]);

	result = construct_array(elems, v->dim, FLOAT4OID, sizeof(float4), true, TYPALIGN_INT);

	pfree(elems);

	PG_RETURN_ARRAYTYPE_P(result);
}

/*
 * vector_scale - multiply vector by scalar
 */
Datum
vector_scale(PG_FUNCTION_ARGS)
{
	Vector	   *v = PG_GETARG_VECTOR_P(0);
	float8		scalar = PG_GETARG_FLOAT8(1);
	Vector	   *result;
	int			i;

	result = InitVector(v->dim);

	for (i = 0; i < v->dim; i++)
	{
		result->x[i] = (float) ((double) v->x[i] * scalar);
		vector_check_value(result->x[i]);
	}

	PG_RETURN_VECTOR_P(result);
}
