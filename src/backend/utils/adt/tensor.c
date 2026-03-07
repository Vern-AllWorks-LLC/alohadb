/*-------------------------------------------------------------------------
 *
 * tensor.c
 *	  Functions for the built-in tensor data type.
 *
 *	  The tensor type stores multi-dimensional arrays of float4 values
 *	  in row-major order, intended for ML activations, feature maps,
 *	  and weight storage. Supports reshape, transpose, slice,
 *	  element-wise ops, matmul, and reduction operations.
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/tensor.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <float.h>
#include <math.h>

#include "catalog/pg_type.h"
#include "libpq/pqformat.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/tensor.h"

/*
 * Validation helpers
 */
static void
tensor_check_ndim(int ndim)
{
	if (ndim < 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("tensor must have at least 1 dimension")));
	if (ndim > TENSOR_MAX_NDIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("tensor cannot have more than %d dimensions", TENSOR_MAX_NDIM)));
}

static void
tensor_check_nelems(int64 nelems)
{
	if (nelems < 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("tensor must have at least 1 element")));
	if (nelems > TENSOR_MAX_ELEMENTS)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("tensor cannot have more than %d elements", TENSOR_MAX_ELEMENTS)));
}

static void
tensor_check_value(float val)
{
	if (isnan(val))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("NaN not allowed in tensor value")));
	if (isinf(val))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("infinite value not allowed in tensor value")));
}

/* ----------------------------------------------------------------
 *		I/O functions
 *
 *	Format: {{1,2,3},{4,5,6}} for a 2x3 tensor
 *	Or flat: [1,2,3,4,5,6] shape=(2,3)
 *	We use the flat format for simplicity: "shape=(2,3) data=[1,2,3,4,5,6]"
 * ----------------------------------------------------------------
 */

Datum
tensor_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
	Tensor	   *result;
	uint16		shape[TENSOR_MAX_NDIM];
	float	   *values;
	int			ndim = 0;
	int64		nelems = 1;
	int			nvalues = 0;
	int			maxvalues = 256;
	char	   *ptr = str;
	char	   *end;

	values = (float *) palloc(sizeof(float) * maxvalues);

	/* Skip whitespace */
	while (isspace((unsigned char) *ptr))
		ptr++;

	/* Parse shape: "shape=(d1,d2,...)" or "(d1,d2,...)" prefix */
	if (strncmp(ptr, "shape=", 6) == 0)
		ptr += 6;

	if (*ptr == '(')
	{
		ptr++;
		while (true)
		{
			long		dim;

			while (isspace((unsigned char) *ptr))
				ptr++;

			if (*ptr == ')')
			{
				ptr++;
				break;
			}

			if (ndim > 0)
			{
				if (*ptr != ',')
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
							 errmsg("malformed tensor literal: \"%s\"", str),
							 errdetail("Expected comma in shape.")));
				ptr++;
				while (isspace((unsigned char) *ptr))
					ptr++;
			}

			errno = 0;
			dim = strtol(ptr, &end, 10);
			if (ptr == end || dim < 1 || dim > 65535)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("malformed tensor literal: \"%s\"", str),
						 errdetail("Invalid dimension size.")));
			ptr = end;

			if (ndim >= TENSOR_MAX_NDIM)
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("tensor cannot have more than %d dimensions", TENSOR_MAX_NDIM)));

			shape[ndim++] = (uint16) dim;
			nelems *= dim;
		}
	}
	else
	{
		/* No shape prefix - treat as 1D */
		ndim = 0;				/* will be set from data count */
	}

	tensor_check_nelems(nelems);

	/* Skip whitespace and optional "data=" */
	while (isspace((unsigned char) *ptr))
		ptr++;
	if (strncmp(ptr, "data=", 5) == 0)
		ptr += 5;

	/* Parse data: [v1,v2,...] */
	if (*ptr != '[')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("malformed tensor literal: \"%s\"", str),
				 errdetail("Data must start with '['.")));
	ptr++;

	while (true)
	{
		float		val;

		while (isspace((unsigned char) *ptr))
			ptr++;

		if (*ptr == ']')
		{
			ptr++;
			break;
		}

		if (nvalues > 0)
		{
			if (*ptr != ',')
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("malformed tensor literal: \"%s\"", str)));
			ptr++;
			while (isspace((unsigned char) *ptr))
				ptr++;
		}

		errno = 0;
		val = strtof(ptr, &end);
		if (ptr == end)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("malformed tensor literal: \"%s\"", str)));
		ptr = end;
		tensor_check_value(val);

		if (nvalues >= maxvalues)
		{
			maxvalues *= 2;
			values = (float *) repalloc(values, sizeof(float) * maxvalues);
		}
		values[nvalues++] = val;
	}

	/* If no shape was given, treat as 1D */
	if (ndim == 0)
	{
		ndim = 1;
		shape[0] = nvalues;
		nelems = nvalues;
	}

	/* Verify element count matches shape */
	if (nvalues != nelems)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("tensor data has %d values but shape requires %lld",
						nvalues, (long long) nelems)));

	tensor_check_ndim(ndim);

	result = InitTensor(ndim, shape);
	memcpy(result->data, values, sizeof(float) * nvalues);

	pfree(values);

	PG_RETURN_TENSOR_P(result);
}

Datum
tensor_out(PG_FUNCTION_ARGS)
{
	Tensor	   *t = PG_GETARG_TENSOR_P(0);
	StringInfoData buf;
	int			i;
	int			nelems = tensor_nelems(t);

	initStringInfo(&buf);

	/* Shape */
	appendStringInfoString(&buf, "shape=(");
	for (i = 0; i < t->ndim; i++)
	{
		if (i > 0)
			appendStringInfoChar(&buf, ',');
		appendStringInfo(&buf, "%d", t->shape[i]);
	}
	appendStringInfoString(&buf, ") data=[");

	/* Data */
	for (i = 0; i < nelems; i++)
	{
		if (i > 0)
			appendStringInfoChar(&buf, ',');
		appendStringInfo(&buf, "%g", t->data[i]);
	}
	appendStringInfoChar(&buf, ']');

	PG_RETURN_CSTRING(buf.data);
}

Datum
tensor_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	Tensor	   *result;
	uint16		shape[TENSOR_MAX_NDIM];
	int			ndim;
	int64		nelems = 1;
	int			i;

	ndim = pq_getmsgint(buf, 1);
	(void) pq_getmsgint(buf, 1);	/* unused */

	tensor_check_ndim(ndim);

	for (i = 0; i < ndim; i++)
	{
		shape[i] = pq_getmsgint(buf, 2);
		nelems *= shape[i];
	}

	tensor_check_nelems(nelems);

	result = InitTensor(ndim, shape);
	for (i = 0; i < nelems; i++)
	{
		result->data[i] = pq_getmsgfloat4(buf);
		tensor_check_value(result->data[i]);
	}

	PG_RETURN_TENSOR_P(result);
}

Datum
tensor_send(PG_FUNCTION_ARGS)
{
	Tensor	   *t = PG_GETARG_TENSOR_P(0);
	StringInfoData buf;
	int			nelems = tensor_nelems(t);
	int			i;

	pq_begintypsend(&buf);
	pq_sendint8(&buf, t->ndim);
	pq_sendint8(&buf, 0);		/* unused */

	for (i = 0; i < t->ndim; i++)
		pq_sendint16(&buf, t->shape[i]);

	for (i = 0; i < nelems; i++)
		pq_sendfloat4(&buf, t->data[i]);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/* ----------------------------------------------------------------
 *		Utility functions
 * ----------------------------------------------------------------
 */

/*
 * tensor_ndims - returns the number of dimensions
 */
Datum
tensor_ndims(PG_FUNCTION_ARGS)
{
	Tensor	   *t = PG_GETARG_TENSOR_P(0);

	PG_RETURN_INT32(t->ndim);
}

/*
 * tensor_shape - returns the shape as an int4 array
 */
Datum
tensor_shape(PG_FUNCTION_ARGS)
{
	Tensor	   *t = PG_GETARG_TENSOR_P(0);
	Datum	   *elems;
	ArrayType  *result;
	int			i;

	elems = (Datum *) palloc(sizeof(Datum) * t->ndim);
	for (i = 0; i < t->ndim; i++)
		elems[i] = Int32GetDatum((int32) t->shape[i]);

	result = construct_array(elems, t->ndim, INT4OID, sizeof(int32), true, TYPALIGN_INT);

	pfree(elems);

	PG_RETURN_ARRAYTYPE_P(result);
}

/*
 * tensor_nelems_func - returns total element count
 */
Datum
tensor_nelems_func(PG_FUNCTION_ARGS)
{
	Tensor	   *t = PG_GETARG_TENSOR_P(0);

	PG_RETURN_INT64((int64) tensor_nelems(t));
}

/*
 * tensor_reshape - reshape tensor to new dimensions
 */
Datum
tensor_reshape(PG_FUNCTION_ARGS)
{
	Tensor	   *t = PG_GETARG_TENSOR_P(0);
	ArrayType  *new_shape_arr = PG_GETARG_ARRAYTYPE_P(1);
	Tensor	   *result;
	Datum	   *shape_elems;
	bool	   *shape_nulls;
	int			new_ndim;
	uint16		new_shape[TENSOR_MAX_NDIM];
	int64		new_nelems = 1;
	int			old_nelems;
	int			i;

	deconstruct_array(new_shape_arr, INT4OID, sizeof(int32), true, TYPALIGN_INT,
					  &shape_elems, &shape_nulls, &new_ndim);

	tensor_check_ndim(new_ndim);

	for (i = 0; i < new_ndim; i++)
	{
		if (shape_nulls[i])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("shape dimensions cannot be NULL")));
		new_shape[i] = (uint16) DatumGetInt32(shape_elems[i]);
		if (new_shape[i] < 1)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("shape dimensions must be positive")));
		new_nelems *= new_shape[i];
	}

	old_nelems = tensor_nelems(t);
	if (new_nelems != old_nelems)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("cannot reshape tensor of %d elements to %lld elements",
						old_nelems, (long long) new_nelems)));

	result = InitTensor(new_ndim, new_shape);
	memcpy(result->data, t->data, sizeof(float) * old_nelems);

	pfree(shape_elems);
	pfree(shape_nulls);

	PG_RETURN_TENSOR_P(result);
}

/*
 * tensor_add - element-wise addition
 */
Datum
tensor_add(PG_FUNCTION_ARGS)
{
	Tensor	   *a = PG_GETARG_TENSOR_P(0);
	Tensor	   *b = PG_GETARG_TENSOR_P(1);
	Tensor	   *result;
	int			nelems;
	int			i;

	if (a->ndim != b->ndim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("tensor dimensions mismatch")));
	for (i = 0; i < a->ndim; i++)
		if (a->shape[i] != b->shape[i])
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("tensor shape mismatch at dimension %d: %d vs %d",
							i, a->shape[i], b->shape[i])));

	nelems = tensor_nelems(a);
	result = InitTensor(a->ndim, a->shape);

	for (i = 0; i < nelems; i++)
	{
		result->data[i] = a->data[i] + b->data[i];
		tensor_check_value(result->data[i]);
	}

	PG_RETURN_TENSOR_P(result);
}

/*
 * tensor_sub - element-wise subtraction
 */
Datum
tensor_sub(PG_FUNCTION_ARGS)
{
	Tensor	   *a = PG_GETARG_TENSOR_P(0);
	Tensor	   *b = PG_GETARG_TENSOR_P(1);
	Tensor	   *result;
	int			nelems;
	int			i;

	if (a->ndim != b->ndim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("tensor dimensions mismatch")));
	for (i = 0; i < a->ndim; i++)
		if (a->shape[i] != b->shape[i])
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("tensor shape mismatch")));

	nelems = tensor_nelems(a);
	result = InitTensor(a->ndim, a->shape);

	for (i = 0; i < nelems; i++)
		result->data[i] = a->data[i] - b->data[i];

	PG_RETURN_TENSOR_P(result);
}

/*
 * tensor_mul_elem - element-wise multiplication (Hadamard product)
 */
Datum
tensor_mul_elem(PG_FUNCTION_ARGS)
{
	Tensor	   *a = PG_GETARG_TENSOR_P(0);
	Tensor	   *b = PG_GETARG_TENSOR_P(1);
	Tensor	   *result;
	int			nelems;
	int			i;

	if (a->ndim != b->ndim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("tensor dimensions mismatch")));
	for (i = 0; i < a->ndim; i++)
		if (a->shape[i] != b->shape[i])
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("tensor shape mismatch")));

	nelems = tensor_nelems(a);
	result = InitTensor(a->ndim, a->shape);

	for (i = 0; i < nelems; i++)
		result->data[i] = a->data[i] * b->data[i];

	PG_RETURN_TENSOR_P(result);
}

/*
 * tensor_matmul - matrix multiplication for 2D tensors
 */
Datum
tensor_matmul(PG_FUNCTION_ARGS)
{
	Tensor	   *a = PG_GETARG_TENSOR_P(0);
	Tensor	   *b = PG_GETARG_TENSOR_P(1);
	Tensor	   *result;
	uint16		result_shape[TENSOR_MAX_NDIM];
	int			M,
				K,
				N;
	int			i,
				j,
				k;

	if (a->ndim != 2 || b->ndim != 2)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("tensor_matmul requires 2D tensors")));

	M = a->shape[0];
	K = a->shape[1];
	if (b->shape[0] != K)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("tensor_matmul: inner dimensions mismatch (%d vs %d)",
						K, b->shape[0])));
	N = b->shape[1];

	tensor_check_nelems((int64) M * N);

	result_shape[0] = M;
	result_shape[1] = N;
	result = InitTensor(2, result_shape);

	/* Naive matrix multiply - adequate for in-DB use; GPU extension for large */
	for (i = 0; i < M; i++)
	{
		for (j = 0; j < N; j++)
		{
			double		sum = 0.0;

			for (k = 0; k < K; k++)
				sum += (double) a->data[i * K + k] * (double) b->data[k * N + j];

			result->data[i * N + j] = (float) sum;
		}
	}

	PG_RETURN_TENSOR_P(result);
}

/*
 * tensor_transpose - transpose a 2D tensor
 */
Datum
tensor_transpose(PG_FUNCTION_ARGS)
{
	Tensor	   *t = PG_GETARG_TENSOR_P(0);
	Tensor	   *result;
	uint16		result_shape[TENSOR_MAX_NDIM];
	int			rows,
				cols;
	int			i,
				j;

	if (t->ndim != 2)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("tensor_transpose requires a 2D tensor")));

	rows = t->shape[0];
	cols = t->shape[1];

	result_shape[0] = cols;
	result_shape[1] = rows;
	result = InitTensor(2, result_shape);

	for (i = 0; i < rows; i++)
		for (j = 0; j < cols; j++)
			result->data[j * rows + i] = t->data[i * cols + j];

	PG_RETURN_TENSOR_P(result);
}

/*
 * tensor_reduce_sum - sum along an axis
 */
Datum
tensor_reduce_sum(PG_FUNCTION_ARGS)
{
	Tensor	   *t = PG_GETARG_TENSOR_P(0);
	int32		axis = PG_GETARG_INT32(1);
	Tensor	   *result;
	uint16		result_shape[TENSOR_MAX_NDIM];
	int			result_ndim;
	int			nelems;
	int			i,
				d;
	int			outer_size,
				axis_size,
				inner_size;

	if (axis < 0 || axis >= t->ndim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("axis %d out of range for %d-dimensional tensor",
						axis, t->ndim)));

	/* Compute result shape (remove the axis dimension) */
	if (t->ndim == 1)
	{
		/* Reduce 1D tensor to scalar - return as 1D tensor with 1 element */
		result_ndim = 1;
		result_shape[0] = 1;
	}
	else
	{
		result_ndim = t->ndim - 1;
		d = 0;
		for (i = 0; i < t->ndim; i++)
		{
			if (i != axis)
				result_shape[d++] = t->shape[i];
		}
	}

	/* Compute strides for the reduction */
	outer_size = 1;
	for (i = 0; i < axis; i++)
		outer_size *= t->shape[i];

	axis_size = t->shape[axis];

	inner_size = 1;
	for (i = axis + 1; i < t->ndim; i++)
		inner_size *= t->shape[i];

	result = InitTensor(result_ndim, result_shape);
	nelems = tensor_nelems(result);
	memset(result->data, 0, sizeof(float) * nelems);

	/* Perform reduction */
	for (i = 0; i < outer_size; i++)
	{
		for (int a = 0; a < axis_size; a++)
		{
			for (int j = 0; j < inner_size; j++)
			{
				int			src_idx = (i * axis_size + a) * inner_size + j;
				int			dst_idx = i * inner_size + j;

				result->data[dst_idx] += t->data[src_idx];
			}
		}
	}

	PG_RETURN_TENSOR_P(result);
}

/*
 * tensor_reduce_mean - mean along an axis
 */
Datum
tensor_reduce_mean(PG_FUNCTION_ARGS)
{
	Tensor	   *t = PG_GETARG_TENSOR_P(0);
	int32		axis = PG_GETARG_INT32(1);
	Tensor	   *sum_result;
	Datum		sum_datum;
	int			nelems;
	int			axis_size;
	int			i;

	/* First compute sum */
	sum_datum = DirectFunctionCall2(tensor_reduce_sum,
								   PG_GETARG_DATUM(0),
								   Int32GetDatum(axis));
	sum_result = DatumGetTensor(sum_datum);

	axis_size = t->shape[axis];
	nelems = tensor_nelems(sum_result);

	for (i = 0; i < nelems; i++)
		sum_result->data[i] /= (float) axis_size;

	PG_RETURN_TENSOR_P(sum_result);
}

/*
 * tensor_reduce_max - max along an axis
 */
Datum
tensor_reduce_max(PG_FUNCTION_ARGS)
{
	Tensor	   *t = PG_GETARG_TENSOR_P(0);
	int32		axis = PG_GETARG_INT32(1);
	Tensor	   *result;
	uint16		result_shape[TENSOR_MAX_NDIM];
	int			result_ndim;
	int			nelems;
	int			i,
				d;
	int			outer_size,
				axis_size,
				inner_size;

	if (axis < 0 || axis >= t->ndim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("axis %d out of range", axis)));

	if (t->ndim == 1)
	{
		result_ndim = 1;
		result_shape[0] = 1;
	}
	else
	{
		result_ndim = t->ndim - 1;
		d = 0;
		for (i = 0; i < t->ndim; i++)
			if (i != axis)
				result_shape[d++] = t->shape[i];
	}

	outer_size = 1;
	for (i = 0; i < axis; i++)
		outer_size *= t->shape[i];
	axis_size = t->shape[axis];
	inner_size = 1;
	for (i = axis + 1; i < t->ndim; i++)
		inner_size *= t->shape[i];

	result = InitTensor(result_ndim, result_shape);
	nelems = tensor_nelems(result);

	/* Initialize with -infinity */
	for (i = 0; i < nelems; i++)
		result->data[i] = -FLT_MAX;

	for (i = 0; i < outer_size; i++)
	{
		for (int a = 0; a < axis_size; a++)
		{
			for (int j = 0; j < inner_size; j++)
			{
				int			src_idx = (i * axis_size + a) * inner_size + j;
				int			dst_idx = i * inner_size + j;

				if (t->data[src_idx] > result->data[dst_idx])
					result->data[dst_idx] = t->data[src_idx];
			}
		}
	}

	PG_RETURN_TENSOR_P(result);
}

/*
 * tensor_eq - equality comparison
 */
Datum
tensor_eq(PG_FUNCTION_ARGS)
{
	Tensor	   *a = PG_GETARG_TENSOR_P(0);
	Tensor	   *b = PG_GETARG_TENSOR_P(1);
	int			nelems;
	int			i;

	if (a->ndim != b->ndim)
		PG_RETURN_BOOL(false);
	for (i = 0; i < a->ndim; i++)
		if (a->shape[i] != b->shape[i])
			PG_RETURN_BOOL(false);

	nelems = tensor_nelems(a);
	for (i = 0; i < nelems; i++)
		if (a->data[i] != b->data[i])
			PG_RETURN_BOOL(false);

	PG_RETURN_BOOL(true);
}

Datum
tensor_ne(PG_FUNCTION_ARGS)
{
	bool		eq = DatumGetBool(DirectFunctionCall2(tensor_eq,
													  PG_GETARG_DATUM(0),
													  PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(!eq);
}
