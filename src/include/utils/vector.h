/*-------------------------------------------------------------------------
 *
 * vector.h
 *	  Declarations for the vector data type (fixed-dimension float4 arrays
 *	  for embedding storage and similarity search).
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * src/include/utils/vector.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VECTOR_H
#define VECTOR_H

#include "fmgr.h"

/* Maximum number of dimensions for a vector */
#define VECTOR_MAX_DIM		16384

/* Size of a vector value with the given number of dimensions */
#define VECTOR_SIZE(dim)	(offsetof(Vector, x) + sizeof(float) * (dim))

/* Minimum size of a valid vector (header + at least 1 dimension) */
#define VECTOR_MIN_SIZE		VECTOR_SIZE(1)

/*
 * Vector type structure.
 *
 * Stored as a varlena with:
 *   - standard varlena header (4 bytes)
 *   - uint16 dim (number of dimensions)
 *   - uint16 unused (padding, must be 0)
 *   - float x[] (dimension values, float4/32-bit IEEE 754)
 */
typedef struct Vector
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	uint16		dim;			/* number of dimensions */
	uint16		unused;			/* reserved, must be 0 */
	float		x[FLEXIBLE_ARRAY_MEMBER];	/* dimension values */
} Vector;

/* fmgr interface macros */
#define DatumGetVector(x)		((Vector *) PG_DETOAST_DATUM(x))
#define PG_GETARG_VECTOR_P(x)	DatumGetVector(PG_GETARG_DATUM(x))
#define PG_RETURN_VECTOR_P(x)	PG_RETURN_POINTER(x)

/* Allocate a new vector with the given number of dimensions */
static inline Vector *
InitVector(int dim)
{
	Vector	   *result;
	int			size = VECTOR_SIZE(dim);

	result = (Vector *) palloc0(size);
	SET_VARSIZE(result, size);
	result->dim = dim;
	result->unused = 0;
	return result;
}

/*
 * Distance function declarations.
 *
 * These are the core distance metrics used for vector similarity search.
 * Each has both a standard C implementation and a SIMD-accelerated version
 * selected at runtime.
 */

/* L2 (Euclidean) squared distance - avoids sqrt for comparison */
extern double vector_l2_squared_distance(const Vector *a, const Vector *b);

/* L2 (Euclidean) distance */
extern double vector_l2_distance(const Vector *a, const Vector *b);

/* Cosine distance = 1 - cosine_similarity */
extern double vector_cosine_distance(const Vector *a, const Vector *b);

/* Inner (dot) product - returned as negative for ORDER BY ascending */
extern double vector_inner_product(const Vector *a, const Vector *b);

/* L1 (Manhattan/Taxicab) distance */
extern double vector_l1_distance(const Vector *a, const Vector *b);

/* Raw inner product (not negated) */
extern double vector_dot_product(const Vector *a, const Vector *b);

/* Vector norm (L2 norm) */
extern double vector_norm(const Vector *a);

/*
 * HNSW / IVFFlat index support strategy numbers.
 * These must match pg_amop.dat entries.
 */
#define VECTOR_L2_DISTANCE_STRATEGY		1	/* <-> operator */
#define VECTOR_COSINE_DISTANCE_STRATEGY	2	/* <=> operator */
#define VECTOR_IP_DISTANCE_STRATEGY		3	/* <#> operator */
#define VECTOR_L1_DISTANCE_STRATEGY		4	/* <+> operator */

/*
 * Support function numbers for vector operator classes.
 */
#define VECTOR_DISTANCE_PROC			1

#endif							/* VECTOR_H */
