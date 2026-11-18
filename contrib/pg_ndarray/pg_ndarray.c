/*-------------------------------------------------------------------------
 *
 * pg_ndarray.c
 *	  Chunked, compressed, sliceable N-dimensional arrays stored in
 *	  ordinary tables, with server-side slicing and math.
 *
 *	  An ndarray is a registry row (name, dtype, shape, chunk_shape, codec,
 *	  fill_value, attrs) plus zero or more chunk rows.  The array is cut
 *	  into a regular dense grid of chunks of chunk_shape elements; each
 *	  chunk is identified by the row-major linearisation of its grid
 *	  coordinates and stored as little-endian, row-major (C order) element
 *	  data for the full chunk_shape (edge chunks are padded with
 *	  fill_value), compressed with zstd unless codec = 'none'.  Chunks that
 *	  were never written are simply absent and read as fill_value.
 *
 *	  Reads visit only the chunks that intersect the requested region
 *	  (either by point lookups or by a single index range scan, whichever
 *	  touches fewer rows), decode them one at a time and copy the selected
 *	  (optionally strided) elements out.  Reductions stream chunk by chunk
 *	  and account for absent chunks arithmetically, so they never
 *	  materialise the array.  Writes lock the chunk rows they modify with
 *	  SELECT ... FOR UPDATE in ascending chunk order, so writers to
 *	  disjoint chunks proceed concurrently; everything else is plain
 *	  MVCC because the storage is ordinary heap tables.
 *
 *	  Indices are 0-based; "stop" bounds are exclusive (Python/NumPy
 *	  slicing convention).  Values are processed as float8 in memory and
 *	  converted to the array's dtype when a chunk is encoded (integer
 *	  dtypes truncate toward zero and reject NaN / out-of-range values).
 *
 * Prior art / patent notes:
 *	  Only the dense, regular chunked layout is implemented.  This design
 *	  is long-established prior art: HDF5 chunked storage (1998), netCDF-4
 *	  (2008, built on HDF5) and the Zarr v2 storage specification (2016,
 *	  MIT licensed) all store an N-d array as a regular grid of
 *	  independently compressed, row-major chunks keyed by chunk
 *	  coordinates, with missing chunks read as a fill value.  Chunk-wise
 *	  streaming reductions use textbook algorithms (running sums, and the
 *	  Welford / Chan et al. pairwise update for variance, 1962/1979).
 *	  Blocked matrix multiplication is likewise textbook.  Deliberately NOT
 *	  implemented: sparse fragment / tile-based layouts (TileDB style) and
 *	  overlapping-chunk schemes (SciDB "chunk overlap").
 *
 * Copyright (c) 2026, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_ndarray/pg_ndarray.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <float.h>
#include <math.h>
#include <zstd.h>

#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "common/int.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "port/pg_bswap.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/tensor.h"

#include "pg_ndarray.h"

PG_MODULE_MAGIC_EXT(
					.name = "pg_ndarray",
					.version = "1.0"
);

/* Largest single dimension (keeps index arithmetic far from overflow) */
#define ND_MAX_DIM_SIZE		(INT64CONST(1) << 62)
/* Largest chunk, in elements (64MB as float8 in memory) */
#define ND_MAX_CHUNK_ELEMS	(8 * 1024 * 1024)
/* Largest materialised result (float8[] of 256MB) */
#define ND_MAX_RESULT_ELEMS (32 * 1024 * 1024)
/* Largest reduce_axis output, in cells */
#define ND_MAX_AXIS_CELLS	(8 * 1024 * 1024)
/* Max chunks probed one by one before switching to a range scan */
#define ND_POINT_LOOKUP_MAX 1000000
/* Max chunk-grid cells per operand for ndarray_matmul */
#define ND_MATMUL_MAX_GRID	1000000
#define ND_FETCH_BATCH		32
#define ND_ZSTD_LEVEL		3

typedef enum NdDtype
{
	ND_FLOAT32,
	ND_FLOAT64,
	ND_INT32,
	ND_INT64
} NdDtype;

static const char *const nd_dtype_names[] = {"float32", "float64", "int32", "int64"};
static const int nd_dtype_size[] = {4, 8, 4, 8};

typedef struct NdMeta
{
	int64		id;
	NdDtype		dtype;
	bool		zstd;
	double		fill;
	int			ndim;
	int64		shape[ND_MAXDIM];
	int64		chunk[ND_MAXDIM];
	int64		grid[ND_MAXDIM];	/* chunks per dimension */
	int64		cstride[ND_MAXDIM]; /* element strides inside a chunk */
	int64		gstride[ND_MAXDIM]; /* strides of the chunk grid */
	int64		nelems;
	int64		nchunks;
	int64		chunk_elems;
	int64		chunk_bytes;	/* encoded size before compression */
} NdMeta;

/* A (strided) hyper-rectangular selection: start <= i < stop, by step */
typedef struct NdRegion
{
	int			ndim;
	int64		start[ND_MAXDIM];
	int64		stop[ND_MAXDIM];
	int64		step[ND_MAXDIM];
	int64		count[ND_MAXDIM];
	int64		nelems;
} NdRegion;

/* Intersection of a region with one chunk */
typedef struct NdIsect
{
	int64		cc[ND_MAXDIM];	/* chunk grid coordinates */
	int64		n[ND_MAXDIM];	/* selected elements per dimension */
	int64		o0[ND_MAXDIM];	/* first output index per dimension */
	int64		l0[ND_MAXDIM];	/* first chunk-local index per dimension */
	int64		nelems;
} NdIsect;

/*
 * Iterates the "rows" (innermost-dimension runs) of an intersection,
 * producing matching offsets into the chunk buffer and an output buffer.
 */
typedef struct NdIter
{
	int			ndim;
	int64		n[ND_MAXDIM];
	int64		cs[ND_MAXDIM];
	int64		os[ND_MAXDIM];
	int64		ctr[ND_MAXDIM];
	int64		coff;
	int64		ooff;
	bool		first;
} NdIter;

typedef struct NdCtx
{
	MemoryContext callerctx;
	MemoryContext tmpctx;
	bool		read_only;
	char	   *meta_tab;
	char	   *chunk_tab;
	SPIPlanPtr	fetch_plan;
	SPIPlanPtr	fetch_upd_plan;
	SPIPlanPtr	insert_plan;
	SPIPlanPtr	insert_nothing_plan;
	SPIPlanPtr	update_plan;
} NdCtx;

typedef enum NdStoreMode
{
	ND_STORE_INSERT,
	ND_STORE_INSERT_IF_ABSENT,
	ND_STORE_UPDATE
} NdStoreMode;

typedef void (*nd_visit_cb) (const NdMeta *m, const NdRegion *r,
							 const NdIsect *is, const double *buf, void *arg);

/* ----------------------------------------------------------------
 *		Small helpers
 * ----------------------------------------------------------------
 */

static NdDtype
nd_parse_dtype(const char *s)
{
	int			i;

	for (i = 0; i < lengthof(nd_dtype_names); i++)
	{
		if (strcmp(s, nd_dtype_names[i]) == 0)
			return (NdDtype) i;
	}
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("invalid ndarray dtype \"%s\"", s),
			 errhint("Valid dtypes are float32, float64, int32 and int64.")));
	return ND_FLOAT64;			/* keep compiler quiet */
}

static bool
nd_parse_codec(const char *s)
{
	if (strcmp(s, "zstd") == 0)
		return true;
	if (strcmp(s, "none") == 0)
		return false;
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("invalid ndarray codec \"%s\"", s),
			 errhint("Valid codecs are zstd and none.")));
	return false;				/* keep compiler quiet */
}

static inline bool
nd_dtype_is_float(NdDtype dt)
{
	return dt == ND_FLOAT32 || dt == ND_FLOAT64;
}

/*
 * Convert a float8 to the value that will actually be stored for dtype,
 * raising an error if it is not representable.
 */
static double
nd_convert(NdDtype dt, double v)
{
	switch (dt)
	{
		case ND_FLOAT64:
			return v;
		case ND_FLOAT32:
			if (!isnan(v) && !isinf(v) && fabs(v) > FLT_MAX)
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("value %g is out of range for dtype float32", v)));
			return (double) (float) v;
		case ND_INT32:
			if (isnan(v))
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("cannot store NaN in an int32 ndarray")));
			if (!(v > -2147483649.0 && v < 2147483648.0))
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("value %g is out of range for dtype int32", v)));
			return trunc(v);
		case ND_INT64:
			if (isnan(v))
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("cannot store NaN in an int64 ndarray")));
			if (!(v >= -9223372036854775808.0 && v < 9223372036854775808.0))
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("value %g is out of range for dtype int64", v)));
			return trunc(v);
	}
	return v;					/* keep compiler quiet */
}

/*
 * Parse a one-dimensional int4[] or int8[] of exactly n non-null elements.
 */
void
nd_parse_index_vec(ArrayType *a, int n, int64 *out, const char *what)
{
	int			nitems;
	int			i;

	if (ARR_NDIM(a) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("%s must be a one-dimensional array", what)));
	nitems = ArrayGetNItems(ARR_NDIM(a), ARR_DIMS(a));
	if (nitems != n)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("%s must have %d elements, not %d", what, n, nitems)));
	if (array_contains_nulls(a))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("%s must not contain nulls", what)));

	if (ARR_ELEMTYPE(a) == INT8OID)
	{
		const int64 *p = (const int64 *) ARR_DATA_PTR(a);

		for (i = 0; i < n; i++)
			out[i] = p[i];
	}
	else if (ARR_ELEMTYPE(a) == INT4OID)
	{
		const int32 *p = (const int32 *) ARR_DATA_PTR(a);

		for (i = 0; i < n; i++)
			out[i] = p[i];
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("%s must be an integer array", what)));
}

/*
 * Validate the user/catalog supplied geometry and compute derived fields.
 */
static void
nd_setup_meta(NdMeta *m)
{
	int64		nelems = 1;
	int64		nchunks = 1;
	int64		celems = 1;
	int			d;

	if (m->ndim < 1 || m->ndim > ND_MAXDIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("ndarray must have between 1 and %d dimensions", ND_MAXDIM)));

	for (d = 0; d < m->ndim; d++)
	{
		if (m->shape[d] < 1 || m->shape[d] > ND_MAX_DIM_SIZE)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("ndarray dimension %d has invalid size " INT64_FORMAT,
							d, m->shape[d])));
		if (m->chunk[d] < 1 || m->chunk[d] > PG_INT32_MAX)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("chunk dimension %d has invalid size " INT64_FORMAT,
							d, m->chunk[d])));
		m->grid[d] = (m->shape[d] - 1) / m->chunk[d] + 1;
		if (pg_mul_s64_overflow(nelems, m->shape[d], &nelems))
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("ndarray has too many elements")));
		if (pg_mul_s64_overflow(nchunks, m->grid[d], &nchunks))
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("ndarray has too many chunks")));
		if (pg_mul_s64_overflow(celems, m->chunk[d], &celems) ||
			celems > ND_MAX_CHUNK_ELEMS)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("chunk shape is too large"),
					 errdetail("A chunk may hold at most %d elements.",
							   ND_MAX_CHUNK_ELEMS)));
	}

	m->nelems = nelems;
	m->nchunks = nchunks;
	m->chunk_elems = celems;
	m->chunk_bytes = celems * nd_dtype_size[m->dtype];

	m->cstride[m->ndim - 1] = 1;
	m->gstride[m->ndim - 1] = 1;
	for (d = m->ndim - 2; d >= 0; d--)
	{
		m->cstride[d] = m->cstride[d + 1] * m->chunk[d + 1];
		m->gstride[d] = m->gstride[d + 1] * m->grid[d + 1];
	}
}

/* Advance an N-d odometer over lo..hi (inclusive). Returns false at end. */
static bool
nd_odometer_next(int ndim, int64 *cc, const int64 *lo, const int64 *hi)
{
	int			d;

	for (d = ndim - 1; d >= 0; d--)
	{
		if (cc[d] < hi[d])
		{
			cc[d]++;
			return true;
		}
		cc[d] = lo[d];
	}
	return false;
}

static int64
nd_chunk_index(const NdMeta *m, const int64 *cc)
{
	int64		idx = 0;
	int			d;

	for (d = 0; d < m->ndim; d++)
		idx += cc[d] * m->gstride[d];
	return idx;
}

static void
nd_chunk_coords(const NdMeta *m, int64 idx, int64 *cc)
{
	int			d;

	if (idx < 0 || idx >= m->nchunks)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("ndarray chunk index " INT64_FORMAT " is out of range", idx)));
	for (d = 0; d < m->ndim; d++)
	{
		cc[d] = idx / m->gstride[d];
		idx %= m->gstride[d];
	}
}

/* Row-major strides for the given extents */
static void
nd_row_major_strides(int ndim, const int64 *count, int64 *stride)
{
	int			d;

	stride[ndim - 1] = 1;
	for (d = ndim - 2; d >= 0; d--)
		stride[d] = stride[d + 1] * count[d + 1];
}

/*
 * Build a region from start/stop/step, checking bounds.
 */
static void
nd_make_region(const NdMeta *m, const int64 *start, const int64 *stop,
			   const int64 *step, NdRegion *r)
{
	int64		nelems = 1;
	int			d;

	r->ndim = m->ndim;
	for (d = 0; d < m->ndim; d++)
	{
		if (start[d] < 0 || start[d] >= m->shape[d])
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("start index " INT64_FORMAT " is out of bounds for dimension %d of size " INT64_FORMAT,
							start[d], d, m->shape[d])));
		if (stop[d] <= start[d] || stop[d] > m->shape[d])
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("stop index " INT64_FORMAT " is out of bounds for dimension %d",
							stop[d], d),
					 errdetail("stop must be greater than start (" INT64_FORMAT ") and at most the dimension size (" INT64_FORMAT ").",
							   start[d], m->shape[d])));
		if (step[d] < 1 || step[d] > PG_INT32_MAX)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("step must be a positive integer")));
		r->start[d] = start[d];
		r->stop[d] = stop[d];
		r->step[d] = step[d];
		r->count[d] = (stop[d] - start[d] - 1) / step[d] + 1;
		if (pg_mul_s64_overflow(nelems, r->count[d], &nelems))
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("region has too many elements")));
	}
	r->nelems = nelems;
}

static void
nd_full_region(const NdMeta *m, NdRegion *r)
{
	int64		start[ND_MAXDIM];
	int64		step[ND_MAXDIM];
	int			d;

	for (d = 0; d < m->ndim; d++)
	{
		start[d] = 0;
		step[d] = 1;
	}
	nd_make_region(m, start, m->shape, step, r);
}

/*
 * Compute the intersection of region r with chunk cc.  Returns false if
 * it is empty (possible with step > 1).
 */
static bool
nd_isect(const NdMeta *m, const NdRegion *r, const int64 *cc, NdIsect *is)
{
	int64		nelems = 1;
	int			d;

	for (d = 0; d < m->ndim; d++)
	{
		int64		lo = cc[d] * m->chunk[d];
		int64		hi = Min(lo + m->chunk[d], m->shape[d]);
		int64		bound = Min(hi, r->stop[d]);
		int64		first;

		if (r->start[d] >= lo)
			first = r->start[d];
		else
			first = r->start[d] +
				((lo - r->start[d] + r->step[d] - 1) / r->step[d]) * r->step[d];
		if (first >= bound)
			return false;
		is->cc[d] = cc[d];
		is->n[d] = (bound - first - 1) / r->step[d] + 1;
		is->o0[d] = (first - r->start[d]) / r->step[d];
		is->l0[d] = first - lo;
		nelems *= is->n[d];
	}
	is->nelems = nelems;
	return true;
}

static void
nd_iter_init(NdIter *it, const NdMeta *m, const NdRegion *r,
			 const NdIsect *is, const int64 *ostride)
{
	int			d;

	it->ndim = m->ndim;
	it->coff = 0;
	it->ooff = 0;
	for (d = 0; d < m->ndim; d++)
	{
		it->n[d] = is->n[d];
		it->cs[d] = m->cstride[d] * r->step[d];
		it->os[d] = ostride[d];
		it->ctr[d] = 0;
		it->coff += is->l0[d] * m->cstride[d];
		it->ooff += is->o0[d] * ostride[d];
	}
	it->first = true;
}

static bool
nd_iter_next(NdIter *it)
{
	int			d;

	if (it->first)
	{
		it->first = false;
		return true;
	}
	for (d = it->ndim - 2; d >= 0; d--)
	{
		it->ctr[d]++;
		it->coff += it->cs[d];
		it->ooff += it->os[d];
		if (it->ctr[d] < it->n[d])
			return true;
		it->coff -= it->cs[d] * it->n[d];
		it->ooff -= it->os[d] * it->n[d];
		it->ctr[d] = 0;
	}
	return false;
}

/* ----------------------------------------------------------------
 *		Chunk encoding
 * ----------------------------------------------------------------
 */

static inline uint32
nd_le32(uint32 v)
{
#ifdef WORDS_BIGENDIAN
	return pg_bswap32(v);
#else
	return v;
#endif
}

static inline uint64
nd_le64(uint64 v)
{
#ifdef WORDS_BIGENDIAN
	return pg_bswap64(v);
#else
	return v;
#endif
}

static void
nd_raw_to_doubles(const NdMeta *m, const char *raw, double *out)
{
	int64		n = m->chunk_elems;
	int64		i;

	switch (m->dtype)
	{
		case ND_FLOAT32:
			for (i = 0; i < n; i++)
			{
				uint32		u;
				float		f;

				memcpy(&u, raw + i * 4, 4);
				u = nd_le32(u);
				memcpy(&f, &u, 4);
				out[i] = f;
			}
			break;
		case ND_FLOAT64:
			for (i = 0; i < n; i++)
			{
				uint64		u;

				memcpy(&u, raw + i * 8, 8);
				u = nd_le64(u);
				memcpy(&out[i], &u, 8);
			}
			break;
		case ND_INT32:
			for (i = 0; i < n; i++)
			{
				uint32		u;
				int32		v;

				memcpy(&u, raw + i * 4, 4);
				u = nd_le32(u);
				memcpy(&v, &u, 4);
				out[i] = v;
			}
			break;
		case ND_INT64:
			for (i = 0; i < n; i++)
			{
				uint64		u;
				int64		v;

				memcpy(&u, raw + i * 8, 8);
				u = nd_le64(u);
				memcpy(&v, &u, 8);
				out[i] = (double) v;
			}
			break;
	}
}

static void
nd_doubles_to_raw(const NdMeta *m, const double *in, char *raw)
{
	int64		n = m->chunk_elems;
	int64		i;

	switch (m->dtype)
	{
		case ND_FLOAT32:
			for (i = 0; i < n; i++)
			{
				float		f = (float) nd_convert(ND_FLOAT32, in[i]);
				uint32		u;

				memcpy(&u, &f, 4);
				u = nd_le32(u);
				memcpy(raw + i * 4, &u, 4);
			}
			break;
		case ND_FLOAT64:
			for (i = 0; i < n; i++)
			{
				uint64		u;

				memcpy(&u, &in[i], 8);
				u = nd_le64(u);
				memcpy(raw + i * 8, &u, 8);
			}
			break;
		case ND_INT32:
			for (i = 0; i < n; i++)
			{
				int32		v = (int32) nd_convert(ND_INT32, in[i]);
				uint32		u;

				memcpy(&u, &v, 4);
				u = nd_le32(u);
				memcpy(raw + i * 4, &u, 4);
			}
			break;
		case ND_INT64:
			for (i = 0; i < n; i++)
			{
				int64		v = (int64) nd_convert(ND_INT64, in[i]);
				uint64		u;

				memcpy(&u, &v, 8);
				u = nd_le64(u);
				memcpy(raw + i * 8, &u, 8);
			}
			break;
	}
}

/* Encode a chunk of doubles into a (possibly compressed) bytea. */
static bytea *
nd_encode(const NdMeta *m, const double *buf)
{
	Size		raw_size = (Size) m->chunk_bytes;
	bytea	   *result;

	if (!m->zstd)
	{
		result = (bytea *) palloc(VARHDRSZ + raw_size);
		SET_VARSIZE(result, VARHDRSZ + raw_size);
		nd_doubles_to_raw(m, buf, VARDATA(result));
	}
	else
	{
		char	   *raw = palloc(raw_size);
		size_t		bound = ZSTD_compressBound(raw_size);
		size_t		csize;

		nd_doubles_to_raw(m, buf, raw);
		result = (bytea *) palloc(VARHDRSZ + bound);
		csize = ZSTD_compress(VARDATA(result), bound, raw, raw_size,
							  ND_ZSTD_LEVEL);
		if (ZSTD_isError(csize))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("zstd compression failed: %s",
							ZSTD_getErrorName(csize))));
		SET_VARSIZE(result, VARHDRSZ + csize);
		pfree(raw);
	}
	return result;
}

/* Decode a stored chunk (detoasting as needed) into doubles. */
static void
nd_decode_datum(const NdMeta *m, Datum d, double *out)
{
	bytea	   *b = DatumGetByteaPP(d);
	const char *src = VARDATA_ANY(b);
	Size		len = VARSIZE_ANY_EXHDR(b);
	Size		raw_size = (Size) m->chunk_bytes;

	if (m->zstd)
	{
		unsigned long long fsize;
		char	   *raw;
		size_t		dsize;

		if (len == 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("ndarray chunk is empty")));
		fsize = ZSTD_getFrameContentSize(src, len);
		if (fsize == ZSTD_CONTENTSIZE_ERROR ||
			fsize == ZSTD_CONTENTSIZE_UNKNOWN ||
			fsize != (unsigned long long) raw_size)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("ndarray chunk has an invalid zstd frame header"),
					 errdetail("Expected %zu decompressed bytes.", raw_size)));
		raw = palloc(raw_size);
		dsize = ZSTD_decompress(raw, raw_size, src, len);
		if (ZSTD_isError(dsize) || dsize != raw_size)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("ndarray chunk could not be decompressed")));
		nd_raw_to_doubles(m, raw, out);
	}
	else
	{
		if (len != raw_size)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("ndarray chunk has %zu bytes, expected %zu",
							len, raw_size)));
		nd_raw_to_doubles(m, src, out);
	}
}

static void
nd_fill_buf(double *buf, int64 n, double v)
{
	int64		i;

	for (i = 0; i < n; i++)
		buf[i] = v;
}

/* ----------------------------------------------------------------
 *		SPI / catalog access
 * ----------------------------------------------------------------
 */

static void
nd_begin(NdCtx *ctx, bool read_only)
{
	Oid			extoid;
	Oid			nspoid;
	char	   *nsp;

	memset(ctx, 0, sizeof(NdCtx));
	ctx->callerctx = CurrentMemoryContext;
	ctx->read_only = read_only;

	extoid = get_extension_oid("pg_ndarray", false);
	nspoid = get_extension_schema(extoid);
	nsp = OidIsValid(nspoid) ? get_namespace_name(nspoid) : NULL;
	if (nsp == NULL)
		elog(ERROR, "could not determine schema of extension pg_ndarray");
	ctx->meta_tab = psprintf("%s.%s", quote_identifier(nsp),
							 quote_identifier("ndarray"));
	ctx->chunk_tab = psprintf("%s.%s", quote_identifier(nsp),
							  quote_identifier("ndarray_chunk"));

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");
	ctx->tmpctx = AllocSetContextCreate(CurrentMemoryContext,
										"pg_ndarray chunk",
										ALLOCSET_DEFAULT_SIZES);
}

static void
nd_end(NdCtx *ctx)
{
	SPI_finish();
}

static SPIPlanPtr
nd_prepare(const char *sql, int nargs, Oid *argtypes)
{
	SPIPlanPtr	plan = SPI_prepare(sql, nargs, argtypes);

	if (plan == NULL)
		elog(ERROR, "SPI_prepare failed for \"%s\": %s",
			 sql, SPI_result_code_string(SPI_result));
	return plan;
}

static char *
nd_spi_text(HeapTuple tup, TupleDesc desc, int col)
{
	bool		isnull;
	Datum		d = SPI_getbinval(tup, desc, col, &isnull);

	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("ndarray catalog contains an unexpected null")));
	return TextDatumGetCString(d);
}

/*
 * Look up an array by name.  With lock, the registry row is locked FOR KEY
 * SHARE so that the array cannot be dropped underneath a writer.
 */
static void
nd_load_meta(NdCtx *ctx, const char *name, bool lock, NdMeta *m)
{
	char	   *sql;
	Oid			argtypes[1] = {TEXTOID};
	Datum		values[1];
	int			rc;
	HeapTuple	tup;
	TupleDesc	desc;
	bool		isnull;
	Datum		d;
	ArrayType  *arr;
	int64		vec[ND_MAXDIM];
	int			ndim;
	int			i;

	sql = psprintf("SELECT array_id, dtype, shape, chunk_shape, codec, fill_value "
				   "FROM %s WHERE name = $1%s",
				   ctx->meta_tab, lock ? " FOR KEY SHARE" : "");
	values[0] = CStringGetTextDatum(name);
	rc = SPI_execute_with_args(sql, 1, argtypes, values, NULL,
							   ctx->read_only, 2);
	if (rc != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute failed: %s", SPI_result_code_string(rc));
	if (SPI_processed == 0)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("ndarray \"%s\" does not exist", name)));

	memset(m, 0, sizeof(NdMeta));
	tup = SPI_tuptable->vals[0];
	desc = SPI_tuptable->tupdesc;

	d = SPI_getbinval(tup, desc, 1, &isnull);
	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("ndarray catalog contains an unexpected null")));
	m->id = DatumGetInt64(d);
	m->dtype = nd_parse_dtype(nd_spi_text(tup, desc, 2));

	d = SPI_getbinval(tup, desc, 3, &isnull);
	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("ndarray catalog contains an unexpected null")));
	arr = DatumGetArrayTypeP(d);
	ndim = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
	if (ndim < 1 || ndim > ND_MAXDIM)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("ndarray \"%s\" has an invalid shape", name)));
	nd_parse_index_vec(arr, ndim, vec, "shape");
	m->ndim = ndim;
	for (i = 0; i < ndim; i++)
		m->shape[i] = vec[i];

	d = SPI_getbinval(tup, desc, 4, &isnull);
	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("ndarray catalog contains an unexpected null")));
	arr = DatumGetArrayTypeP(d);
	nd_parse_index_vec(arr, ndim, vec, "chunk_shape");
	for (i = 0; i < ndim; i++)
		m->chunk[i] = vec[i];

	m->zstd = nd_parse_codec(nd_spi_text(tup, desc, 5));

	d = SPI_getbinval(tup, desc, 6, &isnull);
	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("ndarray catalog contains an unexpected null")));
	m->fill = DatumGetFloat8(d);

	SPI_freetuptable(SPI_tuptable);
	nd_setup_meta(m);
	/* catalog may have been edited by hand: make sure fill is storable */
	m->fill = nd_convert(m->dtype, m->fill);
}

/*
 * Insert a new registry row for m (shape/chunk/dtype/codec/fill already
 * validated) and set m->id.
 */
static void
nd_create_internal(NdCtx *ctx, const char *name, NdMeta *m, Datum attrs,
				   bool attrs_null)
{
	char	   *sql;
	Oid			argtypes[7] = {TEXTOID, TEXTOID, INT8ARRAYOID, INT4ARRAYOID,
	TEXTOID, FLOAT8OID, JSONBOID};
	Datum		values[7];
	char		nulls[7] = {' ', ' ', ' ', ' ', ' ', ' ', ' '};
	Datum		shp[ND_MAXDIM];
	Datum		chk[ND_MAXDIM];
	int			rc;
	bool		isnull;
	int			i;

	if (name[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ndarray name must not be empty")));

	sql = psprintf("SELECT 1 FROM %s WHERE name = $1", ctx->meta_tab);
	values[0] = CStringGetTextDatum(name);
	rc = SPI_execute_with_args(sql, 1, argtypes, values, NULL, false, 1);
	if (rc != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute failed: %s", SPI_result_code_string(rc));
	if (SPI_processed > 0)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("ndarray \"%s\" already exists", name)));
	SPI_freetuptable(SPI_tuptable);

	for (i = 0; i < m->ndim; i++)
	{
		shp[i] = Int64GetDatum(m->shape[i]);
		chk[i] = Int32GetDatum((int32) m->chunk[i]);
	}

	values[1] = CStringGetTextDatum(nd_dtype_names[m->dtype]);
	values[2] = PointerGetDatum(construct_array_builtin(shp, m->ndim, INT8OID));
	values[3] = PointerGetDatum(construct_array_builtin(chk, m->ndim, INT4OID));
	values[4] = CStringGetTextDatum(m->zstd ? "zstd" : "none");
	values[5] = Float8GetDatum(m->fill);
	values[6] = attrs;
	if (attrs_null)
		nulls[6] = 'n';

	sql = psprintf("INSERT INTO %s (name, dtype, shape, chunk_shape, codec, "
				   "fill_value, attrs) "
				   "VALUES ($1, $2, $3, $4, $5, $6, coalesce($7, '{}'::jsonb)) "
				   "RETURNING array_id", ctx->meta_tab);
	rc = SPI_execute_with_args(sql, 7, argtypes, values, nulls, false, 1);
	if (rc != SPI_OK_INSERT_RETURNING || SPI_processed != 1)
		elog(ERROR, "could not insert ndarray registry row: %s",
			 SPI_result_code_string(rc));
	m->id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
										SPI_tuptable->tupdesc, 1, &isnull));
	SPI_freetuptable(SPI_tuptable);
}

/*
 * Fetch and decode one chunk into out.  Returns false if it is not stored.
 */
static bool
nd_fetch_chunk(NdCtx *ctx, const NdMeta *m, int64 idx, bool for_update,
			   double *out)
{
	SPIPlanPtr	plan;
	Datum		values[2];
	int			rc;
	bool		isnull;
	Datum		d;
	MemoryContext oldcxt;

	plan = for_update ? ctx->fetch_upd_plan : ctx->fetch_plan;
	if (plan == NULL)
	{
		Oid			argtypes[2] = {INT8OID, INT8OID};
		char	   *sql;

		sql = psprintf("SELECT data FROM %s WHERE array_id = $1 AND chunk_idx = $2%s",
					   ctx->chunk_tab, for_update ? " FOR UPDATE" : "");
		plan = nd_prepare(sql, 2, argtypes);
		if (for_update)
			ctx->fetch_upd_plan = plan;
		else
			ctx->fetch_plan = plan;
	}

	values[0] = Int64GetDatum(m->id);
	values[1] = Int64GetDatum(idx);
	rc = SPI_execute_plan(plan, values, NULL, ctx->read_only, 1);
	if (rc != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute_plan failed: %s", SPI_result_code_string(rc));
	if (SPI_processed == 0)
	{
		SPI_freetuptable(SPI_tuptable);
		return false;
	}
	d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("ndarray chunk data is null")));
	oldcxt = MemoryContextSwitchTo(ctx->tmpctx);
	nd_decode_datum(m, d, out);
	MemoryContextSwitchTo(oldcxt);
	MemoryContextReset(ctx->tmpctx);
	SPI_freetuptable(SPI_tuptable);
	return true;
}

/*
 * Encode and store one chunk.  Returns false only for
 * ND_STORE_INSERT_IF_ABSENT when a concurrent insert won.
 */
static bool
nd_store_chunk(NdCtx *ctx, const NdMeta *m, int64 idx, const double *buf,
			   NdStoreMode mode)
{
	SPIPlanPtr *planp;
	Datum		values[4];
	int			rc;
	uint64		processed;
	MemoryContext oldcxt;

	switch (mode)
	{
		case ND_STORE_INSERT:
			planp = &ctx->insert_plan;
			break;
		case ND_STORE_INSERT_IF_ABSENT:
			planp = &ctx->insert_nothing_plan;
			break;
		default:
			planp = &ctx->update_plan;
			break;
	}
	if (*planp == NULL)
	{
		Oid			argtypes[4] = {INT8OID, INT8OID, BYTEAOID, INT4OID};
		char	   *sql;

		if (mode == ND_STORE_UPDATE)
			sql = psprintf("UPDATE %s SET data = $3, raw_bytes = $4 "
						   "WHERE array_id = $1 AND chunk_idx = $2",
						   ctx->chunk_tab);
		else
			sql = psprintf("INSERT INTO %s (array_id, chunk_idx, raw_bytes, data) "
						   "VALUES ($1, $2, $4, $3)%s",
						   ctx->chunk_tab,
						   mode == ND_STORE_INSERT_IF_ABSENT ?
						   " ON CONFLICT (array_id, chunk_idx) DO NOTHING" : "");
		*planp = nd_prepare(sql, 4, argtypes);
	}

	oldcxt = MemoryContextSwitchTo(ctx->tmpctx);
	values[0] = Int64GetDatum(m->id);
	values[1] = Int64GetDatum(idx);
	values[2] = PointerGetDatum(nd_encode(m, buf));
	values[3] = Int32GetDatum((int32) m->chunk_bytes);
	MemoryContextSwitchTo(oldcxt);

	rc = SPI_execute_plan(*planp, values, NULL, false, 0);
	processed = SPI_processed;
	MemoryContextReset(ctx->tmpctx);

	if (mode == ND_STORE_UPDATE)
	{
		if (rc != SPI_OK_UPDATE || processed != 1)
			elog(ERROR, "could not update ndarray chunk " INT64_FORMAT, idx);
		return true;
	}
	if (rc != SPI_OK_INSERT)
		elog(ERROR, "could not insert ndarray chunk " INT64_FORMAT, idx);
	return processed == 1;
}

/*
 * Call cb for every *stored* chunk intersecting region r.  Absent chunks
 * are skipped; callers account for them as fill_value.  Either probes each
 * chunk of the region by primary key, or does one index range scan over
 * [first chunk, last chunk] when the region is dense in chunk-index space.
 */
static void
nd_visit_stored(NdCtx *ctx, const NdMeta *m, const NdRegion *r, double *buf,
				nd_visit_cb cb, void *arg)
{
	int64		clo[ND_MAXDIM];
	int64		chi[ND_MAXDIM];
	int64		cc[ND_MAXDIM];
	int64		nreg = 1;
	bool		nreg_ovf = false;
	int64		idx_lo;
	int64		idx_hi;
	int64		span;
	NdIsect		is;
	int			d;

	for (d = 0; d < m->ndim; d++)
	{
		clo[d] = r->start[d] / m->chunk[d];
		chi[d] = (r->stop[d] - 1) / m->chunk[d];
		if (!nreg_ovf && pg_mul_s64_overflow(nreg, chi[d] - clo[d] + 1, &nreg))
			nreg_ovf = true;
	}
	idx_lo = nd_chunk_index(m, clo);
	idx_hi = nd_chunk_index(m, chi);
	span = idx_hi - idx_lo + 1;

	if (!nreg_ovf && nreg <= ND_POINT_LOOKUP_MAX && nreg < span / 2)
	{
		/* point lookups */
		memcpy(cc, clo, sizeof(int64) * m->ndim);
		do
		{
			CHECK_FOR_INTERRUPTS();
			if (!nd_isect(m, r, cc, &is))
				continue;
			if (nd_fetch_chunk(ctx, m, nd_chunk_index(m, cc), false, buf))
				cb(m, r, &is, buf, arg);
		} while (nd_odometer_next(m->ndim, cc, clo, chi));
	}
	else
	{
		/* index range scan */
		Oid			argtypes[3] = {INT8OID, INT8OID, INT8OID};
		Datum		values[3];
		SPIPlanPtr	plan;
		Portal		portal;
		char	   *sql;

		sql = psprintf("SELECT chunk_idx, data FROM %s WHERE array_id = $1 "
					   "AND chunk_idx BETWEEN $2 AND $3 ORDER BY chunk_idx",
					   ctx->chunk_tab);
		plan = nd_prepare(sql, 3, argtypes);
		values[0] = Int64GetDatum(m->id);
		values[1] = Int64GetDatum(idx_lo);
		values[2] = Int64GetDatum(idx_hi);
		portal = SPI_cursor_open(NULL, plan, values, NULL, ctx->read_only);

		for (;;)
		{
			SPITupleTable *tt;
			uint64		ntup;
			uint64		t;

			SPI_cursor_fetch(portal, true, ND_FETCH_BATCH);
			tt = SPI_tuptable;
			ntup = SPI_processed;
			if (ntup == 0)
			{
				SPI_freetuptable(tt);
				break;
			}
			for (t = 0; t < ntup; t++)
			{
				bool		isnull;
				Datum		dd;
				bool		inside = true;
				MemoryContext oldcxt;

				CHECK_FOR_INTERRUPTS();
				dd = SPI_getbinval(tt->vals[t], tt->tupdesc, 1, &isnull);
				if (isnull)
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("ndarray chunk index is null")));
				nd_chunk_coords(m, DatumGetInt64(dd), cc);
				for (d = 0; d < m->ndim; d++)
				{
					if (cc[d] < clo[d] || cc[d] > chi[d])
					{
						inside = false;
						break;
					}
				}
				if (!inside || !nd_isect(m, r, cc, &is))
					continue;
				dd = SPI_getbinval(tt->vals[t], tt->tupdesc, 2, &isnull);
				if (isnull)
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("ndarray chunk data is null")));
				oldcxt = MemoryContextSwitchTo(ctx->tmpctx);
				nd_decode_datum(m, dd, buf);
				MemoryContextSwitchTo(oldcxt);
				MemoryContextReset(ctx->tmpctx);
				cb(m, r, &is, buf, arg);
			}
			SPI_freetuptable(tt);
		}
		SPI_cursor_close(portal);
	}
}

/* ----------------------------------------------------------------
 *		Region read / write
 * ----------------------------------------------------------------
 */

typedef struct NdReadState
{
	double	   *out;
	int64		ostride[ND_MAXDIM];
} NdReadState;

static void
nd_read_cb(const NdMeta *m, const NdRegion *r, const NdIsect *is,
		   const double *buf, void *arg)
{
	NdReadState *st = (NdReadState *) arg;
	NdIter		it;
	int			last = m->ndim - 1;

	nd_iter_init(&it, m, r, is, st->ostride);
	while (nd_iter_next(&it))
	{
		const double *src = buf + it.coff;
		double	   *dst = st->out + it.ooff;
		int64		n = it.n[last];
		int64		cs = it.cs[last];
		int64		os = it.os[last];
		int64		i;

		for (i = 0; i < n; i++)
			dst[i * os] = src[i * cs];
	}
}

/* Read region r into out (r->nelems doubles, row-major). */
static void
nd_read_region(NdCtx *ctx, const NdMeta *m, const NdRegion *r, double *out)
{
	NdReadState st;
	double	   *buf;

	nd_fill_buf(out, r->nelems, m->fill);
	st.out = out;
	nd_row_major_strides(m->ndim, r->count, st.ostride);
	buf = palloc(sizeof(double) * m->chunk_elems);
	nd_visit_stored(ctx, m, r, buf, nd_read_cb, &st);
	pfree(buf);
}

/*
 * Write a dense block src (row-major, extents count) at start.  Chunks are
 * read-modify-written in ascending chunk order with row locks.
 */
static void
nd_write_region(NdCtx *ctx, const NdMeta *m, const int64 *start,
				const int64 *count, const double *src)
{
	NdRegion	r;
	int64		stop[ND_MAXDIM];
	int64		step[ND_MAXDIM];
	int64		sstride[ND_MAXDIM];
	int64		clo[ND_MAXDIM];
	int64		chi[ND_MAXDIM];
	int64		cc[ND_MAXDIM];
	double	   *buf;
	int			last = m->ndim - 1;
	int			d;

	for (d = 0; d < m->ndim; d++)
	{
		if (start[d] < 0 || start[d] >= m->shape[d])
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("start index " INT64_FORMAT " is out of bounds for dimension %d of size " INT64_FORMAT,
							start[d], d, m->shape[d])));
		if (count[d] < 1 || count[d] > m->shape[d] - start[d])
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("write region exceeds dimension %d of size " INT64_FORMAT,
							d, m->shape[d]),
					 errdetail("start " INT64_FORMAT " + extent " INT64_FORMAT " > " INT64_FORMAT ".",
							   start[d], count[d], m->shape[d])));
		stop[d] = start[d] + count[d];
		step[d] = 1;
		clo[d] = start[d] / m->chunk[d];
		chi[d] = (stop[d] - 1) / m->chunk[d];
	}
	nd_make_region(m, start, stop, step, &r);
	nd_row_major_strides(m->ndim, count, sstride);
	buf = palloc(sizeof(double) * m->chunk_elems);

	memcpy(cc, clo, sizeof(int64) * m->ndim);
	do
	{
		NdIsect		is;
		int64		idx = nd_chunk_index(m, cc);
		int			tries;

		CHECK_FOR_INTERRUPTS();
		if (!nd_isect(m, &r, cc, &is))
			continue;			/* cannot happen with step 1 */

		for (tries = 0;; tries++)
		{
			NdIter		it;
			bool		found;

			found = nd_fetch_chunk(ctx, m, idx, true, buf);
			if (!found)
				nd_fill_buf(buf, m->chunk_elems, m->fill);

			nd_iter_init(&it, m, &r, &is, sstride);
			while (nd_iter_next(&it))
			{
				double	   *dst = buf + it.coff;
				const double *s = src + it.ooff;
				int64		n = it.n[last];
				int64		i;

				for (i = 0; i < n; i++)
					dst[i] = s[i];
			}

			if (found)
			{
				nd_store_chunk(ctx, m, idx, buf, ND_STORE_UPDATE);
				break;
			}
			if (nd_store_chunk(ctx, m, idx, buf, ND_STORE_INSERT_IF_ABSENT))
				break;
			/* a concurrent writer created the chunk first: merge again */
			if (tries >= 3)
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("could not write ndarray chunk due to concurrent updates")));
		}
	} while (nd_odometer_next(m->ndim, cc, clo, chi));

	pfree(buf);
}

/* Build a float8[] directly, data copied by caller into ARR_DATA_PTR */
static ArrayType *
nd_alloc_float8_array(int ndim, const int64 *count, int64 nelems)
{
	int			dims[MAXDIM];
	int			lbs[MAXDIM];
	Size		nbytes;
	ArrayType  *result;
	int			d;

	if (ndim > MAXDIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("result would have %d dimensions, but float8[] supports at most %d",
						ndim, MAXDIM),
				 errhint("Use ndarray_read_tensor for arrays with more dimensions.")));
	if (nelems > ND_MAX_RESULT_ELEMS)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("result of " INT64_FORMAT " elements is too large", nelems),
				 errdetail("At most %d elements can be materialised at once.",
						   ND_MAX_RESULT_ELEMS)));
	for (d = 0; d < ndim; d++)
	{
		dims[d] = (int) count[d];
		lbs[d] = 1;
	}
	nbytes = ARR_OVERHEAD_NONULLS(ndim) + sizeof(float8) * (Size) nelems;
	result = (ArrayType *) palloc0(nbytes);
	SET_VARSIZE(result, nbytes);
	result->ndim = ndim;
	result->dataoffset = 0;
	result->elemtype = FLOAT8OID;
	memcpy(ARR_DIMS(result), dims, sizeof(int) * ndim);
	memcpy(ARR_LBOUND(result), lbs, sizeof(int) * ndim);
	return result;
}

/* Parse start/stop/step arguments (step may be absent) into a region */
static void
nd_region_from_args(const NdMeta *m, ArrayType *start, ArrayType *stop,
					ArrayType *step, NdRegion *r)
{
	int64		s0[ND_MAXDIM];
	int64		s1[ND_MAXDIM];
	int64		st[ND_MAXDIM];
	int			d;

	nd_parse_index_vec(start, m->ndim, s0, "start");
	nd_parse_index_vec(stop, m->ndim, s1, "stop");
	if (step)
		nd_parse_index_vec(step, m->ndim, st, "step");
	else
		for (d = 0; d < m->ndim; d++)
			st[d] = 1;
	nd_make_region(m, s0, s1, st, r);
}

/* ----------------------------------------------------------------
 *		SQL: create / write / read
 * ----------------------------------------------------------------
 */

PG_FUNCTION_INFO_V1(ndarray_create);
Datum
ndarray_create(PG_FUNCTION_ARGS)
{
	NdMeta		m;
	NdCtx		ctx;
	char	   *name;
	ArrayType  *shape;
	ArrayType  *chunk;
	int			ndim;
	int64		vec[ND_MAXDIM];
	int			d;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2) ||
		PG_ARGISNULL(3) || PG_ARGISNULL(4) || PG_ARGISNULL(5))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("name, dtype, shape, chunk_shape, codec and fill_value must not be null")));

	memset(&m, 0, sizeof(m));
	name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	m.dtype = nd_parse_dtype(text_to_cstring(PG_GETARG_TEXT_PP(1)));
	shape = PG_GETARG_ARRAYTYPE_P(2);
	chunk = PG_GETARG_ARRAYTYPE_P(3);
	m.zstd = nd_parse_codec(text_to_cstring(PG_GETARG_TEXT_PP(4)));

	if (ARR_NDIM(shape) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("shape must be a non-empty one-dimensional array")));
	ndim = ARR_DIMS(shape)[0];
	if (ndim < 1 || ndim > ND_MAXDIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("ndarray must have between 1 and %d dimensions", ND_MAXDIM)));
	m.ndim = ndim;
	nd_parse_index_vec(shape, ndim, vec, "shape");
	for (d = 0; d < ndim; d++)
		m.shape[d] = vec[d];
	nd_parse_index_vec(chunk, ndim, vec, "chunk_shape");
	for (d = 0; d < ndim; d++)
		m.chunk[d] = vec[d];
	nd_setup_meta(&m);
	m.fill = nd_convert(m.dtype, PG_GETARG_FLOAT8(5));

	nd_begin(&ctx, false);
	nd_create_internal(&ctx, name, &m,
					   PG_ARGISNULL(6) ? (Datum) 0 : PG_GETARG_DATUM(6),
					   PG_ARGISNULL(6));
	nd_end(&ctx);

	PG_RETURN_INT64(m.id);
}

PG_FUNCTION_INFO_V1(ndarray_write);
Datum
ndarray_write(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	ArrayType  *startarr = PG_GETARG_ARRAYTYPE_P(1);
	ArrayType  *vals = PG_GETARG_ARRAYTYPE_P(2);
	NdCtx		ctx;
	NdMeta		m;
	int64		start[ND_MAXDIM];
	int64		count[ND_MAXDIM];
	int64		nelems;
	int			d;

	if (array_contains_nulls(vals))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("values must not contain nulls")));

	nd_begin(&ctx, false);
	nd_load_meta(&ctx, name, true, &m);
	if (ARR_NDIM(vals) != m.ndim)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("values has %d dimensions but ndarray \"%s\" has %d",
						ARR_NDIM(vals), name, m.ndim)));
	nd_parse_index_vec(startarr, m.ndim, start, "start");
	nelems = 1;
	for (d = 0; d < m.ndim; d++)
	{
		count[d] = ARR_DIMS(vals)[d];
		nelems *= count[d];		/* bounded by MaxArraySize */
	}
	nd_write_region(&ctx, &m, start, count, (const double *) ARR_DATA_PTR(vals));
	nd_end(&ctx);

	PG_RETURN_INT64(nelems);
}

PG_FUNCTION_INFO_V1(ndarray_write_tensor);
Datum
ndarray_write_tensor(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	ArrayType  *startarr = PG_GETARG_ARRAYTYPE_P(1);
	Tensor	   *t = PG_GETARG_TENSOR_P(2);
	NdCtx		ctx;
	NdMeta		m;
	int64		start[ND_MAXDIM];
	int64		count[ND_MAXDIM];
	double	   *src;
	int			nelems;
	int			d;
	int			i;

	nd_check_tensor(t);
	nelems = tensor_nelems(t);
	src = palloc(sizeof(double) * nelems);
	for (i = 0; i < nelems; i++)
		src[i] = t->data[i];

	nd_begin(&ctx, false);
	nd_load_meta(&ctx, name, true, &m);
	if (t->ndim != m.ndim)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("tensor has %d dimensions but ndarray \"%s\" has %d",
						t->ndim, name, m.ndim)));
	nd_parse_index_vec(startarr, m.ndim, start, "start");
	for (d = 0; d < m.ndim; d++)
		count[d] = t->shape[d];
	nd_write_region(&ctx, &m, start, count, src);
	nd_end(&ctx);

	PG_RETURN_INT64(nelems);
}

PG_FUNCTION_INFO_V1(ndarray_read);
Datum
ndarray_read(PG_FUNCTION_ARGS)
{
	char	   *name;
	NdCtx		ctx;
	NdMeta		m;
	NdRegion	r;
	ArrayType  *result;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		PG_RETURN_NULL();
	name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	nd_begin(&ctx, true);
	nd_load_meta(&ctx, name, false, &m);
	nd_region_from_args(&m, PG_GETARG_ARRAYTYPE_P(1), PG_GETARG_ARRAYTYPE_P(2),
						PG_ARGISNULL(3) ? NULL : PG_GETARG_ARRAYTYPE_P(3), &r);
	{
		MemoryContext old = MemoryContextSwitchTo(ctx.callerctx);

		result = nd_alloc_float8_array(m.ndim, r.count, r.nelems);
		MemoryContextSwitchTo(old);
	}
	nd_read_region(&ctx, &m, &r, (double *) ARR_DATA_PTR(result));
	nd_end(&ctx);

	PG_RETURN_ARRAYTYPE_P(result);
}

PG_FUNCTION_INFO_V1(ndarray_read_tensor);
Datum
ndarray_read_tensor(PG_FUNCTION_ARGS)
{
	char	   *name;
	NdCtx		ctx;
	NdMeta		m;
	NdRegion	r;
	uint16		shape[TENSOR_MAX_NDIM];
	double	   *vals;
	Tensor	   *t;
	int64		i;
	int			d;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		PG_RETURN_NULL();
	name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	nd_begin(&ctx, true);
	nd_load_meta(&ctx, name, false, &m);
	nd_region_from_args(&m, PG_GETARG_ARRAYTYPE_P(1), PG_GETARG_ARRAYTYPE_P(2),
						PG_ARGISNULL(3) ? NULL : PG_GETARG_ARRAYTYPE_P(3), &r);
	for (d = 0; d < m.ndim; d++)
	{
		if (r.count[d] > PG_UINT16_MAX)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("tensor dimensions are limited to %d elements",
							PG_UINT16_MAX)));
		shape[d] = (uint16) r.count[d];
	}
	if (r.nelems > TENSOR_MAX_ELEMENTS)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("tensor cannot have more than %d elements",
						TENSOR_MAX_ELEMENTS)));
	vals = palloc(sizeof(double) * r.nelems);
	nd_read_region(&ctx, &m, &r, vals);

	{
		MemoryContext old = MemoryContextSwitchTo(ctx.callerctx);

		t = InitTensor(m.ndim, shape);
		MemoryContextSwitchTo(old);
	}
	for (i = 0; i < r.nelems; i++)
	{
		double		v = vals[i];

		if (isnan(v) || isinf(v) || fabs(v) > FLT_MAX)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("value %g cannot be represented in a tensor", v)));
		t->data[i] = (float) v;
	}
	nd_end(&ctx);

	PG_RETURN_TENSOR_P(t);
}

PG_FUNCTION_INFO_V1(ndarray_get);
Datum
ndarray_get(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	ArrayType  *idxarr = PG_GETARG_ARRAYTYPE_P(1);
	NdCtx		ctx;
	NdMeta		m;
	int64		idx[ND_MAXDIM];
	int64		cc[ND_MAXDIM];
	int64		off = 0;
	double	   *buf;
	double		result;
	int			d;

	nd_begin(&ctx, true);
	nd_load_meta(&ctx, name, false, &m);
	nd_parse_index_vec(idxarr, m.ndim, idx, "idx");
	for (d = 0; d < m.ndim; d++)
	{
		if (idx[d] < 0 || idx[d] >= m.shape[d])
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("index " INT64_FORMAT " is out of bounds for dimension %d of size " INT64_FORMAT,
							idx[d], d, m.shape[d])));
		cc[d] = idx[d] / m.chunk[d];
		off += (idx[d] % m.chunk[d]) * m.cstride[d];
	}
	buf = palloc(sizeof(double) * m.chunk_elems);
	if (nd_fetch_chunk(&ctx, &m, nd_chunk_index(&m, cc), false, buf))
		result = buf[off];
	else
		result = m.fill;
	nd_end(&ctx);

	PG_RETURN_FLOAT8(result);
}

/* ----------------------------------------------------------------
 *		SQL: reductions
 * ----------------------------------------------------------------
 */

typedef enum NdReduceOp
{
	RED_SUM,
	RED_MEAN,
	RED_MIN,
	RED_MAX,
	RED_STD,
	RED_NNZ
} NdReduceOp;

static NdReduceOp
nd_parse_reduce_op(const char *s)
{
	if (strcmp(s, "sum") == 0)
		return RED_SUM;
	if (strcmp(s, "mean") == 0)
		return RED_MEAN;
	if (strcmp(s, "min") == 0)
		return RED_MIN;
	if (strcmp(s, "max") == 0)
		return RED_MAX;
	if (strcmp(s, "std") == 0)
		return RED_STD;
	if (strcmp(s, "count_nonzero") == 0)
		return RED_NNZ;
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("invalid reduce op \"%s\"", s),
			 errhint("Valid ops are sum, mean, min, max, std and count_nonzero.")));
	return RED_SUM;				/* keep compiler quiet */
}

/* Streaming accumulator (Welford/Chan for std) */
typedef struct NdAcc
{
	double		n;
	double		sum;
	double		mean;
	double		m2;
	double		min;
	double		max;
	double		nnz;
} NdAcc;

static inline double
nd_min2(double a, double b)
{
	return (a < b || isnan(a)) ? a : b;
}

static inline double
nd_max2(double a, double b)
{
	return (a > b || isnan(a)) ? a : b;
}

static void
nd_acc_init(NdAcc *a)
{
	memset(a, 0, sizeof(NdAcc));
	a->min = INFINITY;
	a->max = -INFINITY;
}

static inline void
nd_acc_add(NdAcc *a, NdReduceOp op, double x)
{
	a->n += 1;
	switch (op)
	{
		case RED_SUM:
		case RED_MEAN:
			a->sum += x;
			break;
		case RED_MIN:
			a->min = nd_min2(a->min, x);
			break;
		case RED_MAX:
			a->max = nd_max2(a->max, x);
			break;
		case RED_STD:
			{
				double		delta = x - a->mean;

				a->mean += delta / a->n;
				a->m2 += delta * (x - a->mean);
			}
			break;
		case RED_NNZ:
			if (x != 0)
				a->nnz += 1;
			break;
	}
}

/* Merge cnt copies of value f (absent chunks) */
static void
nd_acc_add_fill(NdAcc *a, NdReduceOp op, double f, double cnt)
{
	double		n;

	if (cnt <= 0)
		return;
	n = a->n + cnt;
	switch (op)
	{
		case RED_SUM:
		case RED_MEAN:
			a->sum += f * cnt;
			break;
		case RED_MIN:
			a->min = nd_min2(a->min, f);
			break;
		case RED_MAX:
			a->max = nd_max2(a->max, f);
			break;
		case RED_STD:
			{
				double		delta = f - a->mean;

				a->m2 += delta * delta * a->n * cnt / n;
				a->mean += delta * cnt / n;
			}
			break;
		case RED_NNZ:
			if (f != 0)
				a->nnz += cnt;
			break;
	}
	a->n = n;
}

static double
nd_acc_result(const NdAcc *a, NdReduceOp op)
{
	switch (op)
	{
		case RED_SUM:
			return a->sum;
		case RED_MEAN:
			return a->sum / a->n;
		case RED_MIN:
			return a->min;
		case RED_MAX:
			return a->max;
		case RED_STD:
			return sqrt(a->m2 / a->n);
		case RED_NNZ:
			return a->nnz;
	}
	return 0;					/* keep compiler quiet */
}

typedef struct NdReduceState
{
	NdReduceOp	op;
	NdAcc		acc;
	int64		visited;
} NdReduceState;

static void
nd_reduce_cb(const NdMeta *m, const NdRegion *r, const NdIsect *is,
			 const double *buf, void *arg)
{
	NdReduceState *st = (NdReduceState *) arg;
	int64		zero[ND_MAXDIM] = {0};
	NdIter		it;
	int			last = m->ndim - 1;

	nd_iter_init(&it, m, r, is, zero);
	while (nd_iter_next(&it))
	{
		const double *src = buf + it.coff;
		int64		n = it.n[last];
		int64		cs = it.cs[last];
		int64		i;

		for (i = 0; i < n; i++)
			nd_acc_add(&st->acc, st->op, src[i * cs]);
	}
	st->visited += is->nelems;
}

PG_FUNCTION_INFO_V1(ndarray_reduce);
Datum
ndarray_reduce(PG_FUNCTION_ARGS)
{
	char	   *name;
	NdCtx		ctx;
	NdMeta		m;
	NdRegion	r;
	NdReduceState st;
	double	   *buf;
	double		result;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();
	name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	st.op = nd_parse_reduce_op(text_to_cstring(PG_GETARG_TEXT_PP(1)));
	nd_acc_init(&st.acc);
	st.visited = 0;

	nd_begin(&ctx, true);
	nd_load_meta(&ctx, name, false, &m);
	{
		int64		s0[ND_MAXDIM];
		int64		s1[ND_MAXDIM];
		int64		step[ND_MAXDIM];
		int			d;

		for (d = 0; d < m.ndim; d++)
		{
			s0[d] = 0;
			s1[d] = m.shape[d];
			step[d] = 1;
		}
		if (!PG_ARGISNULL(2))
			nd_parse_index_vec(PG_GETARG_ARRAYTYPE_P(2), m.ndim, s0, "start");
		if (!PG_ARGISNULL(3))
			nd_parse_index_vec(PG_GETARG_ARRAYTYPE_P(3), m.ndim, s1, "stop");
		nd_make_region(&m, s0, s1, step, &r);
	}

	buf = palloc(sizeof(double) * m.chunk_elems);
	nd_visit_stored(&ctx, &m, &r, buf, nd_reduce_cb, &st);
	nd_acc_add_fill(&st.acc, st.op, m.fill, (double) (r.nelems - st.visited));
	result = nd_acc_result(&st.acc, st.op);
	nd_end(&ctx);

	PG_RETURN_FLOAT8(result);
}

typedef struct NdAxisState
{
	NdReduceOp	op;
	int64		ostride[ND_MAXDIM];
	double	   *cnt;
	double	   *v1;				/* sum / min / max / nnz / mean */
	double	   *v2;				/* m2 (std only) */
} NdAxisState;

static void
nd_axis_cb(const NdMeta *m, const NdRegion *r, const NdIsect *is,
		   const double *buf, void *arg)
{
	NdAxisState *st = (NdAxisState *) arg;
	NdIter		it;
	int			last = m->ndim - 1;

	nd_iter_init(&it, m, r, is, st->ostride);
	while (nd_iter_next(&it))
	{
		const double *src = buf + it.coff;
		int64		n = it.n[last];
		int64		cs = it.cs[last];
		int64		os = it.os[last];
		int64		i;

		for (i = 0; i < n; i++)
		{
			int64		cell = it.ooff + i * os;
			double		x = src[i * cs];

			st->cnt[cell] += 1;
			switch (st->op)
			{
				case RED_SUM:
				case RED_MEAN:
					st->v1[cell] += x;
					break;
				case RED_MIN:
					st->v1[cell] = nd_min2(st->v1[cell], x);
					break;
				case RED_MAX:
					st->v1[cell] = nd_max2(st->v1[cell], x);
					break;
				case RED_NNZ:
					if (x != 0)
						st->v1[cell] += 1;
					break;
				case RED_STD:
					{
						double		delta = x - st->v1[cell];

						st->v1[cell] += delta / st->cnt[cell];
						st->v2[cell] += delta * (x - st->v1[cell]);
					}
					break;
			}
		}
	}
}

PG_FUNCTION_INFO_V1(ndarray_reduce_axis);
Datum
ndarray_reduce_axis(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	NdReduceOp	op = nd_parse_reduce_op(text_to_cstring(PG_GETARG_TEXT_PP(1)));
	int32		axis = PG_GETARG_INT32(2);
	NdCtx		ctx;
	NdMeta		m;
	NdRegion	r;
	NdAxisState st;
	int64		ocount[ND_MAXDIM];
	int64		ostride_c[ND_MAXDIM];
	int			ondim;
	int64		ncells = 1;
	double	   *buf;
	double	   *out;
	ArrayType  *result;
	int64		c;
	int			d;
	int			k;

	nd_begin(&ctx, true);
	nd_load_meta(&ctx, name, false, &m);
	if (axis < 0 || axis >= m.ndim)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("axis %d is out of range for ndarray with %d dimensions",
						axis, m.ndim)));

	/* output geometry: shape with the axis removed (a scalar is {x}) */
	ondim = 0;
	for (d = 0; d < m.ndim; d++)
	{
		if (d == axis)
			continue;
		ocount[ondim++] = m.shape[d];
		if (pg_mul_s64_overflow(ncells, m.shape[d], &ncells) ||
			ncells > ND_MAX_AXIS_CELLS)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("reduce_axis result is too large"),
					 errdetail("At most %d output cells are supported.",
							   ND_MAX_AXIS_CELLS)));
	}
	if (ondim == 0)
		ocount[ondim++] = 1;
	nd_row_major_strides(ondim, ocount, ostride_c);
	for (d = 0, k = 0; d < m.ndim; d++)
	{
		if (d == axis)
			st.ostride[d] = 0;
		else
			st.ostride[d] = ostride_c[k++];
	}

	{
		MemoryContext old = MemoryContextSwitchTo(ctx.callerctx);

		result = nd_alloc_float8_array(ondim, ocount, ncells);
		MemoryContextSwitchTo(old);
	}
	out = (double *) ARR_DATA_PTR(result);

	st.op = op;
	st.cnt = palloc0(sizeof(double) * ncells);
	st.v1 = palloc0(sizeof(double) * ncells);
	st.v2 = (op == RED_STD) ? palloc0(sizeof(double) * ncells) : NULL;
	if (op == RED_MIN)
		nd_fill_buf(st.v1, ncells, INFINITY);
	else if (op == RED_MAX)
		nd_fill_buf(st.v1, ncells, -INFINITY);

	nd_full_region(&m, &r);
	buf = palloc(sizeof(double) * m.chunk_elems);
	nd_visit_stored(&ctx, &m, &r, buf, nd_axis_cb, &st);

	for (c = 0; c < ncells; c++)
	{
		NdAcc		a;

		if ((c & 0xFFFF) == 0)
			CHECK_FOR_INTERRUPTS();
		nd_acc_init(&a);
		a.n = st.cnt[c];
		switch (op)
		{
			case RED_SUM:
			case RED_MEAN:
				a.sum = st.v1[c];
				break;
			case RED_MIN:
				a.min = st.v1[c];
				break;
			case RED_MAX:
				a.max = st.v1[c];
				break;
			case RED_NNZ:
				a.nnz = st.v1[c];
				break;
			case RED_STD:
				a.mean = st.v1[c];
				a.m2 = st.v2[c];
				break;
		}
		nd_acc_add_fill(&a, op, m.fill, (double) m.shape[axis] - st.cnt[c]);
		out[c] = nd_acc_result(&a, op);
	}
	nd_end(&ctx);

	PG_RETURN_ARRAYTYPE_P(result);
}

/* ----------------------------------------------------------------
 *		SQL: chunkwise elementwise ops into a new array
 * ----------------------------------------------------------------
 */

typedef enum NdMapOp
{
	MAP_ADD,
	MAP_SUB,
	MAP_MUL,
	MAP_DIV,
	MAP_POW,
	MAP_ABS,
	MAP_SQRT,
	MAP_LOG,
	MAP_EXP,
	MAP_CLIP,
	MAP_MIN,					/* binary only */
	MAP_MAX						/* binary only */
} NdMapOp;

static NdMapOp
nd_parse_map_op(const char *s, bool binary)
{
	static const char *const names[] = {"add", "sub", "mul", "div", "pow",
	"abs", "sqrt", "log", "exp", "clip", "min", "max"};
	int			i;

	for (i = 0; i < lengthof(names); i++)
	{
		NdMapOp		op = (NdMapOp) i;

		if (strcmp(s, names[i]) != 0)
			continue;
		if (binary && (op == MAP_ADD || op == MAP_SUB || op == MAP_MUL ||
					   op == MAP_DIV || op == MAP_POW || op == MAP_MIN ||
					   op == MAP_MAX))
			return op;
		if (!binary && op <= MAP_CLIP)
			return op;
	}
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("invalid %s op \"%s\"", binary ? "binary" : "map", s),
			 binary ?
			 errhint("Valid ops are add, sub, mul, div, pow, min and max.") :
			 errhint("Valid ops are add, sub, mul, div, pow, abs, sqrt, log, exp and clip.")));
	return MAP_ADD;				/* keep compiler quiet */
}

static inline double
nd_apply(NdMapOp op, double x, double s, double hi)
{
	switch (op)
	{
		case MAP_ADD:
			return x + s;
		case MAP_SUB:
			return x - s;
		case MAP_MUL:
			return x * s;
		case MAP_DIV:
			return x / s;
		case MAP_POW:
			return pow(x, s);
		case MAP_ABS:
			return fabs(x);
		case MAP_SQRT:
			return sqrt(x);
		case MAP_LOG:
			return log(x);
		case MAP_EXP:
			return exp(x);
		case MAP_CLIP:
			/* s = low bound, hi = high bound; NaN stays NaN */
			if (x < s)
				return s;
			if (x > hi)
				return hi;
			return x;
		case MAP_MIN:
			return nd_min2(x, s);
		case MAP_MAX:
			return nd_max2(x, s);
	}
	return x;					/* keep compiler quiet */
}

static NdDtype
nd_result_dtype(FunctionCallInfo fcinfo, int argno, NdDtype a, NdDtype b)
{
	if (!PG_ARGISNULL(argno))
		return nd_parse_dtype(text_to_cstring(PG_GETARG_TEXT_PP(argno)));
	if (a == b && nd_dtype_is_float(a))
		return a;
	return ND_FLOAT64;
}

/* Prepare a cursor over all stored chunks of an array (by chunk index) */
static Portal
nd_open_all_chunks(NdCtx *ctx, int64 id)
{
	Oid			argtypes[1] = {INT8OID};
	Datum		values[1];
	char	   *sql;

	sql = psprintf("SELECT chunk_idx, data FROM %s WHERE array_id = $1 "
				   "ORDER BY chunk_idx", ctx->chunk_tab);
	values[0] = Int64GetDatum(id);
	return SPI_cursor_open(NULL, nd_prepare(sql, 1, argtypes), values, NULL,
						   ctx->read_only);
}

static int64
nd_tuple_chunk_idx(const NdMeta *m, HeapTuple tup, TupleDesc desc, int col)
{
	bool		isnull;
	Datum		d = SPI_getbinval(tup, desc, col, &isnull);
	int64		idx;

	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("ndarray chunk index is null")));
	idx = DatumGetInt64(d);
	if (idx < 0 || idx >= m->nchunks)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("ndarray chunk index " INT64_FORMAT " is out of range", idx)));
	return idx;
}

PG_FUNCTION_INFO_V1(ndarray_map);
Datum
ndarray_map(PG_FUNCTION_ARGS)
{
	char	   *src;
	char	   *dstname;
	NdMapOp		op;
	double		s = 0;
	double		hi = 0;
	NdCtx		ctx;
	NdMeta		sm;
	NdMeta		dm;
	double	   *buf;
	Portal		portal;
	int64		nwritten = 0;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("src, dst and op must not be null")));
	src = text_to_cstring(PG_GETARG_TEXT_PP(0));
	dstname = text_to_cstring(PG_GETARG_TEXT_PP(1));
	op = nd_parse_map_op(text_to_cstring(PG_GETARG_TEXT_PP(2)), false);

	switch (op)
	{
		case MAP_ADD:
		case MAP_SUB:
		case MAP_MUL:
		case MAP_DIV:
		case MAP_POW:
			if (PG_ARGISNULL(3) || !PG_ARGISNULL(4))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("map op requires exactly one scalar argument")));
			s = PG_GETARG_FLOAT8(3);
			break;
		case MAP_CLIP:
			if (PG_ARGISNULL(3) && PG_ARGISNULL(4))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("clip requires a lower bound (scalar), an upper bound (scalar2), or both")));
			s = PG_ARGISNULL(3) ? -INFINITY : PG_GETARG_FLOAT8(3);
			hi = PG_ARGISNULL(4) ? INFINITY : PG_GETARG_FLOAT8(4);
			if (isnan(s) || isnan(hi) || s > hi)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid clip bounds")));
			break;
		default:
			if (!PG_ARGISNULL(3) || !PG_ARGISNULL(4))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("map op does not take a scalar argument")));
			break;
	}

	nd_begin(&ctx, false);
	nd_load_meta(&ctx, src, true, &sm);
	dm = sm;
	dm.dtype = nd_result_dtype(fcinfo, 5, sm.dtype, sm.dtype);
	nd_setup_meta(&dm);
	dm.fill = nd_convert(dm.dtype, nd_apply(op, sm.fill, s, hi));
	nd_create_internal(&ctx, dstname, &dm, (Datum) 0, true);

	buf = palloc(sizeof(double) * sm.chunk_elems);
	portal = nd_open_all_chunks(&ctx, sm.id);
	for (;;)
	{
		SPITupleTable *tt;
		uint64		ntup;
		uint64		t;

		SPI_cursor_fetch(portal, true, ND_FETCH_BATCH);
		tt = SPI_tuptable;
		ntup = SPI_processed;
		if (ntup == 0)
		{
			SPI_freetuptable(tt);
			break;
		}
		for (t = 0; t < ntup; t++)
		{
			int64		idx = nd_tuple_chunk_idx(&sm, tt->vals[t], tt->tupdesc, 1);
			bool		isnull;
			Datum		d = SPI_getbinval(tt->vals[t], tt->tupdesc, 2, &isnull);
			MemoryContext oldcxt;
			int64		i;

			CHECK_FOR_INTERRUPTS();
			if (isnull)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("ndarray chunk data is null")));
			oldcxt = MemoryContextSwitchTo(ctx.tmpctx);
			nd_decode_datum(&sm, d, buf);
			MemoryContextSwitchTo(oldcxt);
			MemoryContextReset(ctx.tmpctx);
			for (i = 0; i < sm.chunk_elems; i++)
				buf[i] = nd_apply(op, buf[i], s, hi);
			nd_store_chunk(&ctx, &dm, idx, buf, ND_STORE_INSERT);
			nwritten++;
		}
		SPI_freetuptable(tt);
	}
	SPI_cursor_close(portal);
	nd_end(&ctx);

	PG_RETURN_INT64(nwritten);
}

PG_FUNCTION_INFO_V1(ndarray_binary);
Datum
ndarray_binary(PG_FUNCTION_ARGS)
{
	char	   *aname;
	char	   *bname;
	char	   *dstname;
	NdMapOp		op;
	NdCtx		ctx;
	NdMeta		am;
	NdMeta		bm;
	NdMeta		dm;
	double	   *abuf;
	double	   *bbuf;
	Portal		portal;
	int64		nwritten = 0;
	int			d;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2) || PG_ARGISNULL(3))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("a, b, dst and op must not be null")));
	aname = text_to_cstring(PG_GETARG_TEXT_PP(0));
	bname = text_to_cstring(PG_GETARG_TEXT_PP(1));
	dstname = text_to_cstring(PG_GETARG_TEXT_PP(2));
	op = nd_parse_map_op(text_to_cstring(PG_GETARG_TEXT_PP(3)), true);

	nd_begin(&ctx, false);
	nd_load_meta(&ctx, aname, true, &am);
	nd_load_meta(&ctx, bname, true, &bm);
	if (am.ndim != bm.ndim)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ndarrays \"%s\" and \"%s\" must have the same shape and chunk shape",
						aname, bname)));
	for (d = 0; d < am.ndim; d++)
	{
		if (am.shape[d] != bm.shape[d] || am.chunk[d] != bm.chunk[d])
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("ndarrays \"%s\" and \"%s\" must have the same shape and chunk shape",
							aname, bname)));
	}

	dm = am;
	dm.dtype = nd_result_dtype(fcinfo, 4, am.dtype, bm.dtype);
	nd_setup_meta(&dm);
	dm.fill = nd_convert(dm.dtype, nd_apply(op, am.fill, bm.fill, 0));
	nd_create_internal(&ctx, dstname, &dm, (Datum) 0, true);

	abuf = palloc(sizeof(double) * am.chunk_elems);
	bbuf = palloc(sizeof(double) * am.chunk_elems);
	{
		Oid			argtypes[2] = {INT8OID, INT8OID};
		Datum		values[2];
		char	   *sql;

		sql = psprintf("SELECT coalesce(x.chunk_idx, y.chunk_idx), x.data, y.data "
					   "FROM (SELECT chunk_idx, data FROM %s WHERE array_id = $1) x "
					   "FULL JOIN (SELECT chunk_idx, data FROM %s WHERE array_id = $2) y "
					   "ON x.chunk_idx = y.chunk_idx ORDER BY 1",
					   ctx.chunk_tab, ctx.chunk_tab);
		values[0] = Int64GetDatum(am.id);
		values[1] = Int64GetDatum(bm.id);
		portal = SPI_cursor_open(NULL, nd_prepare(sql, 2, argtypes), values,
								 NULL, false);
	}
	for (;;)
	{
		SPITupleTable *tt;
		uint64		ntup;
		uint64		t;

		SPI_cursor_fetch(portal, true, ND_FETCH_BATCH);
		tt = SPI_tuptable;
		ntup = SPI_processed;
		if (ntup == 0)
		{
			SPI_freetuptable(tt);
			break;
		}
		for (t = 0; t < ntup; t++)
		{
			int64		idx = nd_tuple_chunk_idx(&am, tt->vals[t], tt->tupdesc, 1);
			bool		anull;
			bool		bnull;
			Datum		ad = SPI_getbinval(tt->vals[t], tt->tupdesc, 2, &anull);
			Datum		bd = SPI_getbinval(tt->vals[t], tt->tupdesc, 3, &bnull);
			MemoryContext oldcxt;
			int64		i;

			CHECK_FOR_INTERRUPTS();
			oldcxt = MemoryContextSwitchTo(ctx.tmpctx);
			if (anull)
				nd_fill_buf(abuf, am.chunk_elems, am.fill);
			else
				nd_decode_datum(&am, ad, abuf);
			if (bnull)
				nd_fill_buf(bbuf, bm.chunk_elems, bm.fill);
			else
				nd_decode_datum(&bm, bd, bbuf);
			MemoryContextSwitchTo(oldcxt);
			MemoryContextReset(ctx.tmpctx);
			for (i = 0; i < am.chunk_elems; i++)
				abuf[i] = nd_apply(op, abuf[i], bbuf[i], 0);
			nd_store_chunk(&ctx, &dm, idx, abuf, ND_STORE_INSERT);
			nwritten++;
		}
		SPI_freetuptable(tt);
	}
	SPI_cursor_close(portal);
	nd_end(&ctx);

	PG_RETURN_INT64(nwritten);
}

/* Mark which chunks of an array are stored (grid must be small) */
static bool *
nd_stored_bitmap(NdCtx *ctx, const NdMeta *m)
{
	bool	   *bits = palloc0(sizeof(bool) * m->nchunks);
	Oid			argtypes[1] = {INT8OID};
	Datum		values[1];
	char	   *sql;
	Portal		portal;

	sql = psprintf("SELECT chunk_idx FROM %s WHERE array_id = $1", ctx->chunk_tab);
	values[0] = Int64GetDatum(m->id);
	portal = SPI_cursor_open(NULL, nd_prepare(sql, 1, argtypes), values, NULL,
							 ctx->read_only);
	for (;;)
	{
		uint64		t;

		SPI_cursor_fetch(portal, true, 1000);
		if (SPI_processed == 0)
			break;
		for (t = 0; t < SPI_processed; t++)
			bits[nd_tuple_chunk_idx(m, SPI_tuptable->vals[t],
									SPI_tuptable->tupdesc, 1)] = true;
		SPI_freetuptable(SPI_tuptable);
		CHECK_FOR_INTERRUPTS();
	}
	SPI_freetuptable(SPI_tuptable);
	SPI_cursor_close(portal);
	return bits;
}

PG_FUNCTION_INFO_V1(ndarray_matmul);
Datum
ndarray_matmul(PG_FUNCTION_ARGS)
{
	char	   *aname = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *bname = text_to_cstring(PG_GETARG_TEXT_PP(1));
	char	   *dstname = text_to_cstring(PG_GETARG_TEXT_PP(2));
	NdCtx		ctx;
	NdMeta		am;
	NdMeta		bm;
	NdMeta		dm;
	bool	   *aex;
	bool	   *bex;
	double	   *abuf;
	double	   *bbuf;
	double	   *acc;
	int64		gI,
				gK,
				gJ;
	int64		M,
				K,
				N;
	int64		rc,
				kc,
				cc;
	int64		i,
				j,
				k;
	int64		nwritten = 0;

	nd_begin(&ctx, false);
	nd_load_meta(&ctx, aname, true, &am);
	nd_load_meta(&ctx, bname, true, &bm);
	if (am.ndim != 2 || bm.ndim != 2)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ndarray_matmul requires two 2-D arrays")));
	if (am.shape[1] != bm.shape[0])
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("matmul shape mismatch: (" INT64_FORMAT ", " INT64_FORMAT ") x (" INT64_FORMAT ", " INT64_FORMAT ")",
						am.shape[0], am.shape[1], bm.shape[0], bm.shape[1])));
	if (am.chunk[1] != bm.chunk[0])
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("matmul requires a's column chunk size to equal b's row chunk size")));
	if (am.nchunks > ND_MATMUL_MAX_GRID || bm.nchunks > ND_MATMUL_MAX_GRID)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("ndarray_matmul supports at most %d chunks per operand",
						ND_MATMUL_MAX_GRID)));

	M = am.shape[0];
	K = am.shape[1];
	N = bm.shape[1];
	rc = am.chunk[0];
	kc = am.chunk[1];
	cc = bm.chunk[1];
	gI = am.grid[0];
	gK = am.grid[1];
	gJ = bm.grid[1];

	memset(&dm, 0, sizeof(dm));
	dm.dtype = ND_FLOAT64;
	dm.zstd = am.zstd;
	dm.ndim = 2;
	dm.shape[0] = M;
	dm.shape[1] = N;
	dm.chunk[0] = rc;
	dm.chunk[1] = cc;
	nd_setup_meta(&dm);
	/* value of an output element whose inputs are all absent */
	dm.fill = (double) K * am.fill * bm.fill;
	nd_create_internal(&ctx, dstname, &dm, (Datum) 0, true);

	aex = nd_stored_bitmap(&ctx, &am);
	bex = nd_stored_bitmap(&ctx, &bm);
	abuf = palloc(sizeof(double) * am.chunk_elems);
	bbuf = palloc(sizeof(double) * bm.chunk_elems);
	acc = palloc(sizeof(double) * dm.chunk_elems);

	for (i = 0; i < gI; i++)
	{
		int64		rows = Min(rc, M - i * rc);

		for (j = 0; j < gJ; j++)
		{
			int64		cols = Min(cc, N - j * cc);
			bool		any = false;
			int64		p,
						q,
						s;

			CHECK_FOR_INTERRUPTS();
			for (k = 0; k < gK && !any; k++)
				any = aex[i * gK + k] || bex[k * gJ + j];
			if (!any)
				continue;		/* every element equals dst fill */

			nd_fill_buf(acc, dm.chunk_elems, 0.0);
			for (k = 0; k < gK; k++)
			{
				int64		kw = Min(kc, K - k * kc);
				bool		ah = aex[i * gK + k];
				bool		bh = bex[k * gJ + j];

				if (!ah && !bh)
				{
					double		v = (double) kw * am.fill * bm.fill;

					for (p = 0; p < rows; p++)
						for (s = 0; s < cols; s++)
							acc[p * cc + s] += v;
					continue;
				}
				if (!ah || !nd_fetch_chunk(&ctx, &am, i * gK + k, false, abuf))
					nd_fill_buf(abuf, am.chunk_elems, am.fill);
				if (!bh || !nd_fetch_chunk(&ctx, &bm, k * gJ + j, false, bbuf))
					nd_fill_buf(bbuf, bm.chunk_elems, bm.fill);

				for (p = 0; p < rows; p++)
				{
					double	   *arow = abuf + p * kc;
					double	   *crow = acc + p * cc;

					CHECK_FOR_INTERRUPTS();
					for (q = 0; q < kw; q++)
					{
						double		av = arow[q];
						double	   *brow = bbuf + q * cc;

						for (s = 0; s < cols; s++)
							crow[s] += av * brow[s];
					}
				}
			}
			/* pad edge cells deterministically */
			for (p = 0; p < rc; p++)
				for (s = 0; s < cc; s++)
					if (p >= rows || s >= cols)
						acc[p * cc + s] = dm.fill;

			nd_store_chunk(&ctx, &dm, i * gJ + j, acc, ND_STORE_INSERT);
			nwritten++;
		}
	}
	nd_end(&ctx);

	PG_RETURN_INT64(nwritten);
}
