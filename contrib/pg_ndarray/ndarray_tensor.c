/*-------------------------------------------------------------------------
 *
 * ndarray_tensor.c
 *	  Slicing helpers for the core tensor type: tensor_slice and
 *	  tensor_index.
 *
 *	  Indices are 0-based, stop bounds exclusive, steps positive (the
 *	  usual Python/NumPy basic-slicing convention).
 *
 * Copyright (c) 2026, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_ndarray/ndarray_tensor.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/tensor.h"

#include "pg_ndarray.h"

/*
 * Defensive check of a tensor value's header against its varlena size.
 */
void
nd_check_tensor(const Tensor *t)
{
	int64		nelems = 1;
	int			d;

	if (VARSIZE(t) < TENSOR_HEADER_SIZE || t->ndim < 1 ||
		t->ndim > TENSOR_MAX_NDIM)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid tensor value")));
	for (d = 0; d < t->ndim; d++)
	{
		if (t->shape[d] < 1)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("invalid tensor value")));
		nelems *= t->shape[d];
		if (nelems > TENSOR_MAX_ELEMENTS)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("invalid tensor value")));
	}
	if (VARSIZE(t) != TENSOR_SIZE(nelems))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid tensor value")));
}

PG_FUNCTION_INFO_V1(nd_tensor_slice);
Datum
nd_tensor_slice(PG_FUNCTION_ARGS)
{
	Tensor	   *t;
	Tensor	   *result;
	int64		start[TENSOR_MAX_NDIM];
	int64		stop[TENSOR_MAX_NDIM];
	int64		step[TENSOR_MAX_NDIM];
	int64		istride[TENSOR_MAX_NDIM];
	int64		ctr[TENSOR_MAX_NDIM];
	uint16		shape[TENSOR_MAX_NDIM];
	int64		nelems;
	int64		i;
	int			ndim;
	int			d;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		PG_RETURN_NULL();
	t = PG_GETARG_TENSOR_P(0);
	nd_check_tensor(t);
	ndim = t->ndim;

	nd_parse_index_vec(PG_GETARG_ARRAYTYPE_P(1), ndim, start, "start");
	nd_parse_index_vec(PG_GETARG_ARRAYTYPE_P(2), ndim, stop, "stop");
	if (PG_ARGISNULL(3))
		for (d = 0; d < ndim; d++)
			step[d] = 1;
	else
		nd_parse_index_vec(PG_GETARG_ARRAYTYPE_P(3), ndim, step, "step");

	for (d = 0; d < ndim; d++)
	{
		if (start[d] < 0 || start[d] >= t->shape[d])
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("start index " INT64_FORMAT " is out of bounds for dimension %d of size %d",
							start[d], d, t->shape[d])));
		if (stop[d] <= start[d] || stop[d] > t->shape[d])
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("stop index " INT64_FORMAT " is out of bounds for dimension %d",
							stop[d], d)));
		if (step[d] < 1)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("step must be a positive integer")));
		shape[d] = (uint16) ((stop[d] - start[d] - 1) / step[d] + 1);
	}

	istride[ndim - 1] = 1;
	for (d = ndim - 2; d >= 0; d--)
		istride[d] = istride[d + 1] * t->shape[d + 1];

	result = InitTensor(ndim, shape);
	nelems = tensor_nelems(result);
	memset(ctr, 0, sizeof(ctr));
	for (i = 0; i < nelems; i++)
	{
		int64		off = 0;

		for (d = 0; d < ndim; d++)
			off += (start[d] + ctr[d] * step[d]) * istride[d];
		result->data[i] = t->data[off];

		/* advance row-major counter */
		for (d = ndim - 1; d >= 0; d--)
		{
			if (++ctr[d] < shape[d])
				break;
			ctr[d] = 0;
		}
		if ((i & 0xFFFF) == 0)
			CHECK_FOR_INTERRUPTS();
	}

	PG_RETURN_TENSOR_P(result);
}

PG_FUNCTION_INFO_V1(nd_tensor_index);
Datum
nd_tensor_index(PG_FUNCTION_ARGS)
{
	Tensor	   *t = PG_GETARG_TENSOR_P(0);
	int64		idx[TENSOR_MAX_NDIM];
	int64		off = 0;
	int			d;

	nd_check_tensor(t);
	nd_parse_index_vec(PG_GETARG_ARRAYTYPE_P(1), t->ndim, idx, "idx");
	for (d = 0; d < t->ndim; d++)
	{
		if (idx[d] < 0 || idx[d] >= t->shape[d])
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("index " INT64_FORMAT " is out of bounds for dimension %d of size %d",
							idx[d], d, t->shape[d])));
		off = off * t->shape[d] + idx[d];
	}

	PG_RETURN_FLOAT4(t->data[off]);
}
