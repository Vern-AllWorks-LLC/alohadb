/*-------------------------------------------------------------------------
 *
 * cms.c
 *	  Count-Min Sketch data type and functions.
 *
 *	  Provides a probabilistic frequency estimation data structure.
 *	  The sketch uses depth independent hash functions (based on PG's
 *	  hash_any with different seeds per row) to increment counters.
 *	  Point queries return the minimum counter value across all rows,
 *	  providing an upper-bound estimate of true frequency.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_approx/cms.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "common/hashfn.h"
#include "fmgr.h"
#include "varatt.h"
#include "alohadb_approx.h"
#include "utils/builtins.h"

PG_FUNCTION_INFO_V1(cms_in);
PG_FUNCTION_INFO_V1(cms_out);
PG_FUNCTION_INFO_V1(cms_create);
PG_FUNCTION_INFO_V1(cms_add);
PG_FUNCTION_INFO_V1(cms_estimate);
PG_FUNCTION_INFO_V1(cms_merge);

/*
 * cms_hash - Compute the hash for a given row of the sketch.
 *
 * We use PostgreSQL's hash_bytes_extended() with a different seed per row
 * to produce independent hash functions.  The seed is derived by XOR-ing
 * the row index with a constant.
 */
static uint32
cms_hash(const char *data, int len, uint32 row)
{
	uint64		seed = (uint64) row ^ UINT64CONST(0xA5A5A5A5A5A5A5A5);
	uint64		h;

	h = DatumGetUInt64(hash_any_extended((const unsigned char *) data,
										 len, seed));

	/* Fold 64-bit hash to 32 bits */
	return (uint32) (h ^ (h >> 32));
}

/*
 * cms_in - text input function for cms type.
 *
 * The cms type is not intended to be created from text; this function
 * exists only to satisfy the type system.  It always raises an error.
 */
Datum
cms_in(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot input a value of type %s", "cms"),
			 errhint("Use cms_create() to construct a Count-Min Sketch.")));

	PG_RETURN_NULL();			/* keep compiler quiet */
}

/*
 * cms_out - text output function for cms type.
 *
 * Returns a human-readable summary string.
 */
Datum
cms_out(PG_FUNCTION_ARGS)
{
	Cms			sketch = DatumGetCms(PG_GETARG_DATUM(0));
	char	   *result;

	result = psprintf("cms(width=%u, depth=%u)", sketch->width, sketch->depth);

	PG_RETURN_CSTRING(result);
}

/*
 * cms_create - Create an empty Count-Min Sketch with given dimensions.
 *
 * width: number of counters per row (controls accuracy)
 * depth: number of rows / hash functions (controls confidence)
 */
Datum
cms_create(PG_FUNCTION_ARGS)
{
	int32		width = PG_GETARG_INT32(0);
	int32		depth = PG_GETARG_INT32(1);
	Cms			sketch;
	Size		size;

	if (width <= 0 || depth <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("width and depth must be positive integers")));

	if (width > 1000000 || depth > 20)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("width must be <= 1000000 and depth must be <= 20")));

	size = CMS_SIZE((uint32) width, (uint32) depth);
	sketch = (Cms) palloc0(size);
	SET_VARSIZE(sketch, size);
	sketch->width = (uint32) width;
	sketch->depth = (uint32) depth;
	/* counters are already zeroed by palloc0 */

	PG_RETURN_CMS(sketch);
}

/*
 * cms_add - Add an element to the sketch, incrementing its counters.
 *
 * For each of the depth hash functions, compute the bucket index and
 * increment the corresponding counter.  Returns a new (modified) sketch.
 */
Datum
cms_add(PG_FUNCTION_ARGS)
{
	Cms			sketch = PG_GETARG_CMS_COPY(0);
	text	   *item = PG_GETARG_TEXT_PP(1);
	char	   *data = VARDATA_ANY(item);
	int			len = VARSIZE_ANY_EXHDR(item);
	uint32		row;

	for (row = 0; row < sketch->depth; row++)
	{
		uint32		h = cms_hash(data, len, row);
		uint32		idx = h % sketch->width;

		sketch->counters[row * sketch->width + idx]++;
	}

	PG_RETURN_CMS(sketch);
}

/*
 * cms_estimate - Estimate the frequency of an element.
 *
 * Returns the minimum counter value across all rows for the given item.
 * This is an upper-bound estimate of the true frequency.
 */
Datum
cms_estimate(PG_FUNCTION_ARGS)
{
	Cms			sketch = PG_GETARG_CMS(0);
	text	   *item = PG_GETARG_TEXT_PP(1);
	char	   *data = VARDATA_ANY(item);
	int			len = VARSIZE_ANY_EXHDR(item);
	uint32		row;
	uint32		min_count = UINT32_MAX;

	for (row = 0; row < sketch->depth; row++)
	{
		uint32		h = cms_hash(data, len, row);
		uint32		idx = h % sketch->width;
		uint32		count = sketch->counters[row * sketch->width + idx];

		if (count < min_count)
			min_count = count;
	}

	PG_RETURN_INT64((int64) min_count);
}

/*
 * cms_merge - Merge two Count-Min Sketches by summing their counters.
 *
 * Both sketches must have the same dimensions (width and depth).
 */
Datum
cms_merge(PG_FUNCTION_ARGS)
{
	Cms			a = PG_GETARG_CMS(0);
	Cms			b = PG_GETARG_CMS(1);
	Cms			result;
	Size		size;
	uint32		total;
	uint32		i;

	if (a->width != b->width || a->depth != b->depth)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot merge sketches with different dimensions"),
				 errdetail("Sketch a has width=%u, depth=%u; sketch b has width=%u, depth=%u.",
						   a->width, a->depth, b->width, b->depth)));

	size = CMS_SIZE(a->width, a->depth);
	result = (Cms) palloc(size);
	SET_VARSIZE(result, size);
	result->width = a->width;
	result->depth = a->depth;

	total = a->width * a->depth;
	for (i = 0; i < total; i++)
		result->counters[i] = a->counters[i] + b->counters[i];

	PG_RETURN_CMS(result);
}
