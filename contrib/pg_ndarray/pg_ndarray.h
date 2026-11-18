/*-------------------------------------------------------------------------
 *
 * pg_ndarray.h
 *	  Shared declarations for the pg_ndarray extension.
 *
 * Copyright (c) 2026, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_ndarray/pg_ndarray.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ALOHADB_NDARRAY_H
#define ALOHADB_NDARRAY_H

#include "utils/array.h"
#include "utils/tensor.h"

/* Maximum number of dimensions of an ndarray (same as the core tensor) */
#define ND_MAXDIM			TENSOR_MAX_NDIM

extern void nd_parse_index_vec(ArrayType *a, int n, int64 *out,
							   const char *what);
extern void nd_check_tensor(const Tensor *t);

#endif							/* ALOHADB_NDARRAY_H */
