/*-------------------------------------------------------------------------
 *
 * tensor.h
 *	  Declarations for the tensor data type (multi-dimensional float4
 *	  arrays for ML activations, feature maps, and weight storage).
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * src/include/utils/tensor.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TENSOR_H
#define TENSOR_H

#include "fmgr.h"

/* Maximum number of dimensions for a tensor */
#define TENSOR_MAX_NDIM		8

/* Maximum total number of elements */
#define TENSOR_MAX_ELEMENTS	1048576		/* 1M */

/*
 * Tensor type structure.
 *
 * Stored as a varlena with:
 *   - standard varlena header (4 bytes)
 *   - uint8 ndim (number of dimensions)
 *   - uint8 unused (padding)
 *   - uint16 shape[TENSOR_MAX_NDIM] (dimension sizes, unused dims = 0)
 *   - float data[] (row-major element data)
 *
 * Total header before data: 4 + 1 + 1 + 16 = 22 bytes, padded to 24
 */
typedef struct Tensor
{
	int32		vl_len_;		/* varlena header */
	uint8		ndim;			/* number of dimensions */
	uint8		unused;			/* padding, must be 0 */
	uint16		shape[TENSOR_MAX_NDIM]; /* dimension sizes */
	float		data[FLEXIBLE_ARRAY_MEMBER]; /* row-major elements */
} Tensor;

/* Size computation */
#define TENSOR_HEADER_SIZE	offsetof(Tensor, data)
#define TENSOR_SIZE(nelems)	(TENSOR_HEADER_SIZE + sizeof(float) * (nelems))

/* fmgr interface macros */
#define DatumGetTensor(x)		((Tensor *) PG_DETOAST_DATUM(x))
#define PG_GETARG_TENSOR_P(x)	DatumGetTensor(PG_GETARG_DATUM(x))
#define PG_RETURN_TENSOR_P(x)	PG_RETURN_POINTER(x)

/* Compute total number of elements from shape */
static inline int
tensor_nelems(const Tensor *t)
{
	int			n = 1;
	int			i;

	for (i = 0; i < t->ndim; i++)
		n *= t->shape[i];
	return n;
}

/* Allocate a new tensor */
static inline Tensor *
InitTensor(int ndim, const uint16 *shape)
{
	Tensor	   *result;
	int			nelems = 1;
	int			size;
	int			i;

	for (i = 0; i < ndim; i++)
		nelems *= shape[i];

	size = TENSOR_SIZE(nelems);
	result = (Tensor *) palloc0(size);
	SET_VARSIZE(result, size);
	result->ndim = ndim;
	result->unused = 0;
	for (i = 0; i < ndim; i++)
		result->shape[i] = shape[i];
	for (i = ndim; i < TENSOR_MAX_NDIM; i++)
		result->shape[i] = 0;

	return result;
}

#endif							/* TENSOR_H */
