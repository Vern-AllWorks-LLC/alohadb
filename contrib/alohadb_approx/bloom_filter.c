/*-------------------------------------------------------------------------
 *
 * bloom_filter.c
 *	  Bloom filter probabilistic data structure.
 *
 *	  A Bloom filter is a space-efficient probabilistic data structure that
 *	  tests whether an element is a member of a set.  False positive matches
 *	  are possible, but false negatives are not: a query returns either
 *	  "possibly in set" or "definitely not in set".
 *
 *	  The filter is stored as a varlena with a bit array.  Optimal parameters
 *	  (number of bits m and hash functions k) are computed from the expected
 *	  number of items n and the desired false positive rate p:
 *	    m = ceil(-n * ln(p) / (ln(2)^2))
 *	    k = round(m/n * ln(2))
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_approx/bloom_filter.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <math.h>

#include "common/hashfn.h"
#include "fmgr.h"
#include "funcapi.h"
#include "varatt.h"
#include "alohadb_approx.h"
#include "utils/builtins.h"

PG_FUNCTION_INFO_V1(bloom_in);
PG_FUNCTION_INFO_V1(bloom_out);
PG_FUNCTION_INFO_V1(bloom_create);
PG_FUNCTION_INFO_V1(bloom_add);
PG_FUNCTION_INFO_V1(bloom_contains);
PG_FUNCTION_INFO_V1(bloom_merge);
PG_FUNCTION_INFO_V1(bloom_stats);
PG_FUNCTION_INFO_V1(bloom_agg_transfn);
PG_FUNCTION_INFO_V1(bloom_agg_finalfn);

/*
 * bloom_hash - Compute a hash for the given seed (hash function index).
 *
 * Uses hash_any_extended() with different seeds to produce k independent
 * hash functions.  Returns a value in [0, m).
 */
static uint32
bloom_hash(const char *data, int len, uint32 seed, uint32 m)
{
	uint64		h;

	h = DatumGetUInt64(hash_any_extended((const unsigned char *) data,
										 len, (uint64) seed));

	return (uint32) (h % m);
}

/*
 * bloom_allocate - Allocate and initialize an empty Bloom filter.
 */
static Bloom
bloom_allocate(uint32 m, uint32 k)
{
	Size		nbytes;
	Size		size;
	Bloom		bf;

	nbytes = (m + 7) / 8;
	size = BLOOM_HEADER_SIZE + nbytes;
	bf = (Bloom) palloc0(size);
	SET_VARSIZE(bf, size);
	bf->m = m;
	bf->k = k;
	bf->n_added = 0;
	/* bits are already zeroed by palloc0 */

	return bf;
}

/*
 * bloom_in - text input function for bloom_filter type.
 *
 * The bloom_filter type is not intended to be created from text; this
 * function exists only to satisfy the type system.
 */
Datum
bloom_in(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot input a value of type %s", "bloom_filter"),
			 errhint("Use bloom_create() to construct a Bloom filter.")));

	PG_RETURN_NULL();			/* keep compiler quiet */
}

/*
 * bloom_out - text output function for bloom_filter type.
 *
 * Returns a human-readable summary string.
 */
Datum
bloom_out(PG_FUNCTION_ARGS)
{
	Bloom		bf = DatumGetBloom(PG_GETARG_DATUM(0));
	char	   *result;

	result = psprintf("bloom_filter(bits=%u, hash_functions=%u, items_added=%u)",
					  bf->m, bf->k, bf->n_added);

	PG_RETURN_CSTRING(result);
}

/*
 * bloom_create - Create an empty Bloom filter.
 *
 * Arguments:
 *   expected_items (int4) - expected number of items to insert
 *   fpr (float8) - desired false positive rate (default 0.01)
 *
 * Computes optimal m and k from the given parameters.
 */
Datum
bloom_create(PG_FUNCTION_ARGS)
{
	int32		expected_items = PG_GETARG_INT32(0);
	float8		fpr = PG_GETARG_FLOAT8(1);
	uint32		m;
	uint32		k;
	Bloom		bf;

	if (expected_items <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("expected_items must be a positive integer")));

	if (fpr <= 0.0 || fpr >= 1.0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("false positive rate must be between 0 and 1 exclusive")));

	/* m = ceil(-n * ln(p) / (ln(2)^2)) */
	m = (uint32) ceil(-((double) expected_items) * log(fpr) / (log(2.0) * log(2.0)));
	if (m == 0)
		m = 1;

	/* Limit to a reasonable maximum (128 MB of bits = ~1 billion bits) */
	if (m > 1000000000U)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("computed filter size exceeds maximum allowed")));

	/* k = round(m/n * ln(2)) */
	k = (uint32) round(((double) m / (double) expected_items) * log(2.0));
	if (k == 0)
		k = 1;
	if (k > 30)
		k = 30;

	bf = bloom_allocate(m, k);

	PG_RETURN_BLOOM(bf);
}

/*
 * bloom_add - Add a text item to the Bloom filter.
 *
 * Sets the k hash-designated bits.  Returns the modified filter.
 */
Datum
bloom_add(PG_FUNCTION_ARGS)
{
	Bloom		bf = PG_GETARG_BLOOM_COPY(0);
	text	   *item = PG_GETARG_TEXT_PP(1);
	char	   *data = VARDATA_ANY(item);
	int			len = VARSIZE_ANY_EXHDR(item);
	uint32		i;

	for (i = 0; i < bf->k; i++)
	{
		uint32		bit = bloom_hash(data, len, i, bf->m);

		bf->bits[bit / 8] |= (1 << (bit % 8));
	}

	bf->n_added++;

	PG_RETURN_BLOOM(bf);
}

/*
 * bloom_contains - Test whether an item might be in the Bloom filter.
 *
 * Returns true if all k hash-designated bits are set (possibly in set),
 * false if any bit is not set (definitely not in set).
 */
Datum
bloom_contains(PG_FUNCTION_ARGS)
{
	Bloom		bf = DatumGetBloom(PG_GETARG_DATUM(0));
	text	   *item = PG_GETARG_TEXT_PP(1);
	char	   *data = VARDATA_ANY(item);
	int			len = VARSIZE_ANY_EXHDR(item);
	uint32		i;

	for (i = 0; i < bf->k; i++)
	{
		uint32		bit = bloom_hash(data, len, i, bf->m);

		if (!(bf->bits[bit / 8] & (1 << (bit % 8))))
			PG_RETURN_BOOL(false);
	}

	PG_RETURN_BOOL(true);
}

/*
 * bloom_merge - Merge two Bloom filters by OR-ing their bit arrays.
 *
 * Both filters must have the same parameters (m and k).
 */
Datum
bloom_merge(PG_FUNCTION_ARGS)
{
	Bloom		a = DatumGetBloom(PG_GETARG_DATUM(0));
	Bloom		b = DatumGetBloom(PG_GETARG_DATUM(1));
	Bloom		result;
	uint32		nbytes;
	uint32		i;

	if (a->m != b->m || a->k != b->k)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot merge Bloom filters with different parameters"),
				 errdetail("Filter a has m=%u, k=%u; filter b has m=%u, k=%u.",
						   a->m, a->k, b->m, b->k)));

	result = bloom_allocate(a->m, a->k);
	result->n_added = a->n_added + b->n_added;

	nbytes = (a->m + 7) / 8;
	for (i = 0; i < nbytes; i++)
		result->bits[i] = a->bits[i] | b->bits[i];

	PG_RETURN_BLOOM(result);
}

/*
 * bloom_stats - Return statistics about a Bloom filter.
 *
 * Returns a single row with columns:
 *   bits (int4), hash_functions (int4), items_added (int4), est_fpr (float8)
 *
 * The estimated false positive rate is computed as:
 *   (1 - e^(-k*n/m))^k
 */
Datum
bloom_stats(PG_FUNCTION_ARGS)
{
	Bloom		bf = DatumGetBloom(PG_GETARG_DATUM(0));
	TupleDesc	tupdesc;
	Datum		values[4];
	bool		nulls[4] = {false, false, false, false};
	HeapTuple	tuple;
	float8		est_fpr;

	/* Build the tuple descriptor */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context that cannot accept type record")));

	tupdesc = BlessTupleDesc(tupdesc);

	/* Compute estimated FPR: (1 - e^(-k*n/m))^k */
	if (bf->n_added == 0)
		est_fpr = 0.0;
	else
		est_fpr = pow(1.0 - exp(-((double) bf->k * (double) bf->n_added) / (double) bf->m),
					  (double) bf->k);

	values[0] = Int32GetDatum((int32) bf->m);
	values[1] = Int32GetDatum((int32) bf->k);
	values[2] = Int32GetDatum((int32) bf->n_added);
	values[3] = Float8GetDatum(est_fpr);

	tuple = heap_form_tuple(tupdesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/*
 * Aggregate transition state for bloom_agg.
 *
 * We store the bloom filter parameters (from the first call) and the
 * filter itself in the aggregate memory context.
 */
typedef struct BloomAggState
{
	Bloom		bf;
} BloomAggState;

/*
 * bloom_agg_transfn - Transition function for the bloom_agg aggregate.
 *
 * Arguments: (internal state, text item, int4 expected_items, float8 fpr)
 *
 * On the first call, creates a new Bloom filter with the given parameters.
 * On subsequent calls, adds the item to the filter.
 */
Datum
bloom_agg_transfn(PG_FUNCTION_ARGS)
{
	MemoryContext aggctx;
	MemoryContext oldctx;
	BloomAggState *state;

	if (!AggCheckCallContext(fcinfo, &aggctx))
		elog(ERROR, "bloom_agg_transfn called in non-aggregate context");

	/* Skip NULL inputs */
	if (PG_ARGISNULL(1))
	{
		if (PG_ARGISNULL(0))
			PG_RETURN_NULL();
		PG_RETURN_POINTER(PG_GETARG_POINTER(0));
	}

	if (PG_ARGISNULL(0))
	{
		int32		expected_items;
		float8		fpr;
		uint32		m;
		uint32		k;

		/* Get parameters */
		if (PG_ARGISNULL(2))
			elog(ERROR, "expected_items parameter must not be NULL");
		expected_items = PG_GETARG_INT32(2);

		fpr = PG_ARGISNULL(3) ? 0.01 : PG_GETARG_FLOAT8(3);

		if (expected_items <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("expected_items must be a positive integer")));

		if (fpr <= 0.0 || fpr >= 1.0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("false positive rate must be between 0 and 1 exclusive")));

		/* Compute optimal parameters */
		m = (uint32) ceil(-((double) expected_items) * log(fpr) / (log(2.0) * log(2.0)));
		if (m == 0)
			m = 1;
		if (m > 1000000000U)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("computed filter size exceeds maximum allowed")));

		k = (uint32) round(((double) m / (double) expected_items) * log(2.0));
		if (k == 0)
			k = 1;
		if (k > 30)
			k = 30;

		/* Allocate state in aggregate context */
		oldctx = MemoryContextSwitchTo(aggctx);
		state = (BloomAggState *) palloc(sizeof(BloomAggState));
		state->bf = bloom_allocate(m, k);
		MemoryContextSwitchTo(oldctx);
	}
	else
	{
		state = (BloomAggState *) PG_GETARG_POINTER(0);
	}

	/* Add the item */
	{
		text	   *item = PG_GETARG_TEXT_PP(1);
		char	   *data = VARDATA_ANY(item);
		int			len = VARSIZE_ANY_EXHDR(item);
		uint32		i;

		for (i = 0; i < state->bf->k; i++)
		{
			uint32		bit = bloom_hash(data, len, i, state->bf->m);

			state->bf->bits[bit / 8] |= (1 << (bit % 8));
		}
		state->bf->n_added++;
	}

	PG_RETURN_POINTER(state);
}

/*
 * bloom_agg_finalfn - Final function for the bloom_agg aggregate.
 *
 * Returns the Bloom filter as a bloom_filter datum.
 */
Datum
bloom_agg_finalfn(PG_FUNCTION_ARGS)
{
	BloomAggState *state;
	Bloom		result;
	Size		size;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (BloomAggState *) PG_GETARG_POINTER(0);

	/* Copy the filter to the caller's memory context */
	size = VARSIZE(state->bf);
	result = (Bloom) palloc(size);
	memcpy(result, state->bf, size);

	PG_RETURN_BLOOM(result);
}
