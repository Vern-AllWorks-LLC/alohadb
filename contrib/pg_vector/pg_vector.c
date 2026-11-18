/*-------------------------------------------------------------------------
 *
 * pg_vector.c
 *	  Vector type I/O and scalar distance functions.
 *
 *	  The vector type is a variable-length varlena:
 *	    [int32  vl_len_]  varlena header
 *	    [int16  ndim   ]  number of dimensions
 *	    [int16  _unused]  padding
 *	    [float4 data[ndim]]
 *
 *	  Text format:  [f1, f2, ..., fn]   (brackets required)
 *
 *	  Distance functions use standard mathematical formulae with
 *	  prior art pre-dating any patent window:
 *	    L2 (Euclidean):   Euclid, ~300 BCE
 *	    Cosine distance:  Salton & McGill, 1983
 *	    Dot product:      standard linear algebra, centuries of prior art
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_vector/pg_vector.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "varatt.h"

#include "pg_vector.h"

#include <math.h>
#include <ctype.h>

PG_MODULE_MAGIC_EXT(
					.name = "pg_vector",
					.version = "1.0"
);

PG_FUNCTION_INFO_V1(vector_in);
PG_FUNCTION_INFO_V1(vector_out);
PG_FUNCTION_INFO_V1(vector_dims);
PG_FUNCTION_INFO_V1(vector_l2_distance);
PG_FUNCTION_INFO_V1(vector_cosine_distance);
PG_FUNCTION_INFO_V1(vector_neg_dot_product);

/* ----------------------------------------------------------------
 * vector_in
 *
 * Parse "[f1, f2, ..., fn]" into a Vector.
 * ---------------------------------------------------------------- */
Datum
vector_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
	char	   *p = str;
	float4		vals[65536];	/* max 64k dims at parse time */
	int			ndim = 0;
	Vector	   *vec;

	/* Skip whitespace, require '[' */
	while (isspace((unsigned char) *p)) p++;
	if (*p != '[')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("vector must start with '['")));
	p++;

	/* Parse comma-separated floats */
	while (*p)
	{
		char   *end;
		double	v;

		while (isspace((unsigned char) *p)) p++;
		if (*p == ']') break;
		if (*p == ',') { p++; continue; }

		v = strtod(p, &end);
		if (end == p)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid float in vector at position %d", ndim)));
		if (ndim >= 65536)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("vector exceeds maximum of 65536 dimensions")));

		vals[ndim++] = (float4) v;
		p = end;
	}

	while (isspace((unsigned char) *p)) p++;
	if (*p != ']')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("vector must end with ']'")));

	/* Allocate and fill */
	vec = (Vector *) palloc(VECTOR_SIZE(ndim));
	SET_VARSIZE(vec, VECTOR_SIZE(ndim));
	vec->ndim    = (int16) ndim;
	vec->_unused = 0;
	memcpy(vec->data, vals, ndim * sizeof(float4));

	PG_RETURN_VECTOR(vec);
}

/* ----------------------------------------------------------------
 * vector_out
 *
 * Produce "[f1,f2,...,fn]" from a Vector.
 * ---------------------------------------------------------------- */
Datum
vector_out(PG_FUNCTION_ARGS)
{
	Vector		   *vec = PG_GETARG_VECTOR(0);
	StringInfoData	buf;
	int				i;

	initStringInfo(&buf);
	appendStringInfoChar(&buf, '[');

	for (i = 0; i < vec->ndim; i++)
	{
		if (i > 0)
			appendStringInfoChar(&buf, ',');
		appendStringInfo(&buf, "%g", (double) vec->data[i]);
	}

	appendStringInfoChar(&buf, ']');
	PG_RETURN_CSTRING(buf.data);
}

/* ----------------------------------------------------------------
 * vector_dims — return number of dimensions
 * ---------------------------------------------------------------- */
Datum
vector_dims(PG_FUNCTION_ARGS)
{
	Vector *vec = PG_GETARG_VECTOR(0);
	PG_RETURN_INT32((int32) vec->ndim);
}

/* ----------------------------------------------------------------
 * Distance functions (also exported for vector_ivfflat.c)
 * ---------------------------------------------------------------- */

float8
vector_l2_dist(Vector *a, Vector *b)
{
	float8	sum = 0.0;
	int		i;

	if (a->ndim != b->ndim)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("vectors must have the same number of dimensions")));

	for (i = 0; i < a->ndim; i++)
	{
		float8 d = (float8) a->data[i] - (float8) b->data[i];
		sum += d * d;
	}
	return sqrt(sum);
}

float8
vector_cosine_dist(Vector *a, Vector *b)
{
	float8	dot = 0.0, na = 0.0, nb = 0.0;
	int		i;

	if (a->ndim != b->ndim)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("vectors must have the same number of dimensions")));

	for (i = 0; i < a->ndim; i++)
	{
		dot += (float8) a->data[i] * (float8) b->data[i];
		na  += (float8) a->data[i] * (float8) a->data[i];
		nb  += (float8) b->data[i] * (float8) b->data[i];
	}

	if (na == 0.0 || nb == 0.0)
		return 1.0;		/* treat zero vectors as maximally distant */

	return 1.0 - dot / (sqrt(na) * sqrt(nb));
}

float8
vector_dot_product(Vector *a, Vector *b)
{
	float8	sum = 0.0;
	int		i;

	if (a->ndim != b->ndim)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("vectors must have the same number of dimensions")));

	for (i = 0; i < a->ndim; i++)
		sum += (float8) a->data[i] * (float8) b->data[i];

	return sum;
}

/* ----------------------------------------------------------------
 * SQL-callable wrappers
 * ---------------------------------------------------------------- */

Datum
vector_l2_distance(PG_FUNCTION_ARGS)
{
	Vector *a = PG_GETARG_VECTOR(0);
	Vector *b = PG_GETARG_VECTOR(1);
	PG_RETURN_FLOAT8(vector_l2_dist(a, b));
}

Datum
vector_cosine_distance(PG_FUNCTION_ARGS)
{
	Vector *a = PG_GETARG_VECTOR(0);
	Vector *b = PG_GETARG_VECTOR(1);
	PG_RETURN_FLOAT8(vector_cosine_dist(a, b));
}

Datum
vector_neg_dot_product(PG_FUNCTION_ARGS)
{
	Vector *a = PG_GETARG_VECTOR(0);
	Vector *b = PG_GETARG_VECTOR(1);
	PG_RETURN_FLOAT8(-vector_dot_product(a, b));
}
