#ifndef ALOHADB_VECTOR_H
#define ALOHADB_VECTOR_H

#include "postgres.h"
#include "fmgr.h"

/*
 * Variable-length vector type:
 *   [int32  vl_len_  ]  varlena header
 *   [int16  ndim     ]  number of dimensions
 *   [int16  _unused  ]  padding / reserved
 *   [float4 data[ndim]]
 */
typedef struct Vector
{
	int32		vl_len_;
	int16		ndim;
	int16		_unused;
	float4		data[FLEXIBLE_ARRAY_MEMBER];
} Vector;

#define VECTOR_SIZE(ndim)   (offsetof(Vector, data) + (ndim) * sizeof(float4))
#define DatumGetVector(d)   ((Vector *) PG_DETOAST_DATUM(d))
#define PG_GETARG_VECTOR(n) DatumGetVector(PG_GETARG_DATUM(n))
#define PG_RETURN_VECTOR(v) PG_RETURN_POINTER(v)

/* Distance functions (vector_ivfflat.c) */
extern float8 vector_l2_dist(Vector *a, Vector *b);
extern float8 vector_cosine_dist(Vector *a, Vector *b);
extern float8 vector_dot_product(Vector *a, Vector *b);

/* SQL-callable functions */
extern Datum vector_in(PG_FUNCTION_ARGS);
extern Datum vector_out(PG_FUNCTION_ARGS);
extern Datum vector_dims(PG_FUNCTION_ARGS);
extern Datum vector_l2_distance(PG_FUNCTION_ARGS);
extern Datum vector_cosine_distance(PG_FUNCTION_ARGS);
extern Datum vector_neg_dot_product(PG_FUNCTION_ARGS);
extern Datum vector_index_create(PG_FUNCTION_ARGS);
extern Datum vector_index_insert(PG_FUNCTION_ARGS);
extern Datum vector_index_search(PG_FUNCTION_ARGS);
extern Datum vector_index_rebuild(PG_FUNCTION_ARGS);
extern Datum vector_index_stats(PG_FUNCTION_ARGS);

#endif							/* ALOHADB_VECTOR_H */
