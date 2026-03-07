/*-------------------------------------------------------------------------
 *
 * tdigest.c
 *	  T-Digest approximate quantile data structure.
 *
 *	  A T-Digest is a data structure for accurate estimation of quantiles
 *	  and cumulative distribution functions over a stream of values.  It
 *	  maintains a set of weighted centroids and uses a compression parameter
 *	  to control the trade-off between accuracy and space.
 *
 *	  The implementation follows the merging digest approach from:
 *	  Ted Dunning, "Computing Extremely Accurate Quantiles Using t-Digests"
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_approx/tdigest.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <math.h>

#include "fmgr.h"
#include "varatt.h"
#include "alohadb_approx.h"
#include "utils/builtins.h"

PG_FUNCTION_INFO_V1(tdigest_in);
PG_FUNCTION_INFO_V1(tdigest_out);
PG_FUNCTION_INFO_V1(tdigest_create);
PG_FUNCTION_INFO_V1(tdigest_add);
PG_FUNCTION_INFO_V1(tdigest_quantile);
PG_FUNCTION_INFO_V1(tdigest_cdf);
PG_FUNCTION_INFO_V1(tdigest_merge);
PG_FUNCTION_INFO_V1(approx_percentile_transfn);
PG_FUNCTION_INFO_V1(approx_percentile_finalfn);

/*
 * Comparison function for sorting centroids by mean.
 */
static int
centroid_cmp(const void *a, const void *b)
{
	const Centroid *ca = (const Centroid *) a;
	const Centroid *cb = (const Centroid *) b;

	if (ca->mean < cb->mean)
		return -1;
	if (ca->mean > cb->mean)
		return 1;
	return 0;
}

/*
 * tdigest_allocate - Allocate and initialize an empty T-Digest.
 */
static TDigest
tdigest_allocate_internal(float8 compression)
{
	int32		max_centroids;
	Size		size;
	TDigest		td;

	max_centroids = (int32) ceil(compression * 10.0);
	size = TDIGEST_SIZE(max_centroids);

	td = (TDigest) palloc0(size);
	SET_VARSIZE(td, size);
	td->compression = compression;
	td->num_centroids = 0;
	td->max_centroids = max_centroids;
	td->total_weight = 0.0;
	td->min_val = INFINITY;
	td->max_val = -INFINITY;

	return td;
}

/*
 * tdigest_compress - Compress the T-Digest by merging centroids.
 *
 * Sorts centroids by mean, then merges adjacent centroids while respecting
 * the size bound: the merged centroid's weight must satisfy:
 *   q + w_merged/total <= compression * 4 * q * (1-q)
 * where q is the quantile at the centroid's position.
 */
static void
tdigest_compress(TDigest td)
{
	Centroid   *centroids;
	Centroid   *merged;
	int			n;
	int			num_merged;
	float8		total_weight;
	float8		weight_so_far;
	int			i;

	if (td->num_centroids <= 1)
		return;

	centroids = td->centroids;
	n = td->num_centroids;
	total_weight = td->total_weight;

	/* Sort centroids by mean */
	qsort(centroids, n, sizeof(Centroid), centroid_cmp);

	/* Merge pass */
	merged = (Centroid *) palloc(sizeof(Centroid) * n);
	merged[0] = centroids[0];
	num_merged = 1;
	weight_so_far = centroids[0].weight;

	for (i = 1; i < n; i++)
	{
		float8		q;
		float8		k_limit;
		float8		proposed_weight;

		proposed_weight = merged[num_merged - 1].weight + centroids[i].weight;
		q = (weight_so_far - merged[num_merged - 1].weight / 2.0) / total_weight;

		/* Clamp q to [0, 1] */
		if (q < 0.0)
			q = 0.0;
		if (q > 1.0)
			q = 1.0;

		k_limit = td->compression * 4.0 * q * (1.0 - q);

		/* Ensure minimum k_limit so that tails can still merge */
		if (k_limit < 1.0)
			k_limit = 1.0;

		if (proposed_weight <= k_limit)
		{
			/* Merge: update mean as weighted average */
			float8		old_weight = merged[num_merged - 1].weight;
			float8		new_weight = proposed_weight;

			merged[num_merged - 1].mean =
				(merged[num_merged - 1].mean * old_weight +
				 centroids[i].mean * centroids[i].weight) / new_weight;
			merged[num_merged - 1].weight = new_weight;
		}
		else
		{
			/* Start a new centroid */
			weight_so_far += merged[num_merged - 1].weight;
			merged[num_merged] = centroids[i];
			num_merged++;
		}
	}

	/* Copy merged centroids back */
	memcpy(td->centroids, merged, sizeof(Centroid) * num_merged);
	td->num_centroids = num_merged;

	pfree(merged);
}

/*
 * tdigest_add_value - Add a single value to the T-Digest.
 *
 * If the digest is full, compress first to make room.
 */
static void
tdigest_add_value(TDigest td, float8 value, float8 weight)
{
	/* Compress if we're at capacity */
	if (td->num_centroids >= td->max_centroids)
		tdigest_compress(td);

	/* If still full after compression (shouldn't normally happen), compress again */
	if (td->num_centroids >= td->max_centroids)
		tdigest_compress(td);

	/* Add new centroid */
	td->centroids[td->num_centroids].mean = value;
	td->centroids[td->num_centroids].weight = weight;
	td->num_centroids++;
	td->total_weight += weight;

	/* Update min/max */
	if (value < td->min_val)
		td->min_val = value;
	if (value > td->max_val)
		td->max_val = value;
}

/*
 * tdigest_in - text input function for tdigest type.
 *
 * The tdigest type is not intended to be created from text; this function
 * exists only to satisfy the type system.
 */
Datum
tdigest_in(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot input a value of type %s", "tdigest"),
			 errhint("Use tdigest_create() to construct a T-Digest.")));

	PG_RETURN_NULL();			/* keep compiler quiet */
}

/*
 * tdigest_out - text output function for tdigest type.
 *
 * Returns a human-readable summary string.
 */
Datum
tdigest_out(PG_FUNCTION_ARGS)
{
	TDigest		td = DatumGetTDigest(PG_GETARG_DATUM(0));
	char	   *result;

	result = psprintf("tdigest(compression=%.0f, centroids=%d, total_weight=%.0f)",
					  td->compression, td->num_centroids, td->total_weight);

	PG_RETURN_CSTRING(result);
}

/*
 * tdigest_create - Create an empty T-Digest.
 *
 * Arguments:
 *   compression (float8) - compression parameter (default 100)
 */
Datum
tdigest_create(PG_FUNCTION_ARGS)
{
	float8		compression = PG_GETARG_FLOAT8(0);
	TDigest		td;

	if (compression <= 0.0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("compression must be a positive number")));

	if (compression > 10000.0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("compression must be <= 10000")));

	td = tdigest_allocate_internal(compression);

	PG_RETURN_TDIGEST(td);
}

/*
 * tdigest_add - Add a value to the T-Digest.
 *
 * Returns the modified T-Digest.
 */
Datum
tdigest_add(PG_FUNCTION_ARGS)
{
	TDigest		td = PG_GETARG_TDIGEST_COPY(0);
	float8		value = PG_GETARG_FLOAT8(1);

	if (isnan(value))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot add NaN to a T-Digest")));

	tdigest_add_value(td, value, 1.0);

	PG_RETURN_TDIGEST(td);
}

/*
 * tdigest_quantile - Estimate the value at a given quantile.
 *
 * Walks the sorted centroids, interpolating at the target cumulative weight.
 * The quantile parameter must be in [0, 1].
 */
Datum
tdigest_quantile(PG_FUNCTION_ARGS)
{
	TDigest		td = DatumGetTDigest(PG_GETARG_DATUM(0));
	float8		quantile = PG_GETARG_FLOAT8(1);
	float8		target_weight;
	float8		cumulative;
	int			i;

	if (quantile < 0.0 || quantile > 1.0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("quantile must be between 0 and 1")));

	if (td->num_centroids == 0)
		PG_RETURN_NULL();

	/* Ensure centroids are sorted */
	if (td->num_centroids > 1)
		qsort(td->centroids, td->num_centroids, sizeof(Centroid), centroid_cmp);

	/* Edge cases */
	if (quantile == 0.0)
		PG_RETURN_FLOAT8(td->min_val);
	if (quantile == 1.0)
		PG_RETURN_FLOAT8(td->max_val);

	/* Single centroid case */
	if (td->num_centroids == 1)
		PG_RETURN_FLOAT8(td->centroids[0].mean);

	target_weight = quantile * td->total_weight;
	cumulative = 0.0;

	for (i = 0; i < td->num_centroids; i++)
	{
		float8		centroid_mid = cumulative + td->centroids[i].weight / 2.0;

		if (centroid_mid >= target_weight)
		{
			/*
			 * Interpolate between the previous centroid and this one,
			 * or use min_val if this is the first centroid.
			 */
			if (i == 0)
			{
				/* Interpolate between min_val and first centroid */
				float8		inner = target_weight / (td->centroids[0].weight / 2.0);

				if (inner > 1.0)
					inner = 1.0;
				PG_RETURN_FLOAT8(td->min_val + inner * (td->centroids[0].mean - td->min_val));
			}
			else
			{
				/* Interpolate between centroids[i-1] and centroids[i] */
				float8		prev_mid = cumulative - td->centroids[i - 1].weight / 2.0
					+ td->centroids[i - 1].weight;
				float8		delta;
				float8		frac;

				/* prev_mid is the right edge of centroid i-1 */
				prev_mid = cumulative;

				delta = centroid_mid - prev_mid;
				if (delta <= 0.0)
					PG_RETURN_FLOAT8(td->centroids[i].mean);

				frac = (target_weight - prev_mid) / delta;
				PG_RETURN_FLOAT8(td->centroids[i - 1].mean +
								 frac * (td->centroids[i].mean - td->centroids[i - 1].mean));
			}
		}

		cumulative += td->centroids[i].weight;
	}

	/* Should not reach here, but return max_val as fallback */
	PG_RETURN_FLOAT8(td->max_val);
}

/*
 * tdigest_cdf - Estimate the CDF value (quantile) for a given value.
 *
 * Returns the fraction of values in the digest that are less than or equal
 * to the given value.
 */
Datum
tdigest_cdf(PG_FUNCTION_ARGS)
{
	TDigest		td = DatumGetTDigest(PG_GETARG_DATUM(0));
	float8		value = PG_GETARG_FLOAT8(1);
	float8		cumulative;
	int			i;

	if (td->num_centroids == 0)
		PG_RETURN_FLOAT8(0.0);

	/* Ensure centroids are sorted */
	if (td->num_centroids > 1)
		qsort(td->centroids, td->num_centroids, sizeof(Centroid), centroid_cmp);

	/* Below minimum */
	if (value <= td->min_val)
		PG_RETURN_FLOAT8(0.0);

	/* Above maximum */
	if (value >= td->max_val)
		PG_RETURN_FLOAT8(1.0);

	/* Single centroid case */
	if (td->num_centroids == 1)
	{
		if (td->min_val == td->max_val)
			PG_RETURN_FLOAT8(0.5);

		PG_RETURN_FLOAT8((value - td->min_val) / (td->max_val - td->min_val));
	}

	cumulative = 0.0;

	for (i = 0; i < td->num_centroids; i++)
	{
		float8		centroid_mid = cumulative + td->centroids[i].weight / 2.0;

		if (td->centroids[i].mean >= value)
		{
			/*
			 * Interpolate between previous centroid (or min_val) and this one.
			 */
			if (i == 0)
			{
				/* Interpolate between min_val and first centroid */
				float8		delta = td->centroids[0].mean - td->min_val;

				if (delta <= 0.0)
					PG_RETURN_FLOAT8(cumulative / td->total_weight);

				PG_RETURN_FLOAT8(((value - td->min_val) / delta) *
								 (td->centroids[0].weight / 2.0) / td->total_weight);
			}
			else
			{
				float8		prev_mid = cumulative - td->centroids[i - 1].weight / 2.0
					+ td->centroids[i - 1].weight;
				float8		delta;
				float8		frac;

				prev_mid = cumulative;

				delta = td->centroids[i].mean - td->centroids[i - 1].mean;
				if (delta <= 0.0)
					PG_RETURN_FLOAT8(cumulative / td->total_weight);

				frac = (value - td->centroids[i - 1].mean) / delta;
				PG_RETURN_FLOAT8((prev_mid + frac * (centroid_mid - prev_mid)) / td->total_weight);
			}
		}

		cumulative += td->centroids[i].weight;
	}

	/* Value is beyond the last centroid mean but below max_val */
	PG_RETURN_FLOAT8(cumulative / td->total_weight);
}

/*
 * tdigest_merge - Merge two T-Digests.
 *
 * Creates a new T-Digest with the combined centroids, then compresses.
 * Uses the larger compression parameter of the two inputs.
 */
Datum
tdigest_merge(PG_FUNCTION_ARGS)
{
	TDigest		a = DatumGetTDigest(PG_GETARG_DATUM(0));
	TDigest		b = DatumGetTDigest(PG_GETARG_DATUM(1));
	TDigest		result;
	float8		compression;
	int			i;

	compression = (a->compression >= b->compression) ? a->compression : b->compression;

	result = tdigest_allocate_internal(compression);

	/* Add all centroids from both digests */
	for (i = 0; i < a->num_centroids; i++)
		tdigest_add_value(result, a->centroids[i].mean, a->centroids[i].weight);

	for (i = 0; i < b->num_centroids; i++)
		tdigest_add_value(result, b->centroids[i].mean, b->centroids[i].weight);

	/* Update min/max */
	if (a->min_val < result->min_val)
		result->min_val = a->min_val;
	if (b->min_val < result->min_val)
		result->min_val = b->min_val;
	if (a->max_val > result->max_val)
		result->max_val = a->max_val;
	if (b->max_val > result->max_val)
		result->max_val = b->max_val;

	/* Final compression */
	tdigest_compress(result);

	PG_RETURN_TDIGEST(result);
}

/*
 * Aggregate transition state for approx_percentile.
 */
typedef struct ApproxPercentileState
{
	TDigest		td;
	float8		percentile;		/* stored from first call */
} ApproxPercentileState;

/*
 * approx_percentile_transfn - Transition function for approx_percentile.
 *
 * Arguments: (internal state, float8 value, float8 percentile,
 *             float8 compression DEFAULT 100)
 */
Datum
approx_percentile_transfn(PG_FUNCTION_ARGS)
{
	MemoryContext aggctx;
	MemoryContext oldctx;
	ApproxPercentileState *state;

	if (!AggCheckCallContext(fcinfo, &aggctx))
		elog(ERROR, "approx_percentile_transfn called in non-aggregate context");

	/* Skip NULL value inputs */
	if (PG_ARGISNULL(1))
	{
		if (PG_ARGISNULL(0))
			PG_RETURN_NULL();
		PG_RETURN_POINTER(PG_GETARG_POINTER(0));
	}

	if (PG_ARGISNULL(0))
	{
		float8		percentile;
		float8		compression;

		/* percentile parameter must not be NULL */
		if (PG_ARGISNULL(2))
			elog(ERROR, "percentile parameter must not be NULL");

		percentile = PG_GETARG_FLOAT8(2);
		if (percentile < 0.0 || percentile > 1.0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("percentile must be between 0 and 1")));

		compression = PG_ARGISNULL(3) ? 100.0 : PG_GETARG_FLOAT8(3);
		if (compression <= 0.0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("compression must be a positive number")));

		/* First call: allocate state in aggregate context */
		oldctx = MemoryContextSwitchTo(aggctx);
		state = (ApproxPercentileState *) palloc(sizeof(ApproxPercentileState));
		state->td = tdigest_allocate_internal(compression);
		state->percentile = percentile;
		MemoryContextSwitchTo(oldctx);
	}
	else
	{
		state = (ApproxPercentileState *) PG_GETARG_POINTER(0);
	}

	/* Add the value */
	{
		float8		value = PG_GETARG_FLOAT8(1);

		if (isnan(value))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("cannot add NaN to a T-Digest")));

		tdigest_add_value(state->td, value, 1.0);
	}

	PG_RETURN_POINTER(state);
}

/*
 * approx_percentile_finalfn - Final function for approx_percentile.
 *
 * Compresses the T-Digest and computes the quantile at the stored percentile.
 */
Datum
approx_percentile_finalfn(PG_FUNCTION_ARGS)
{
	ApproxPercentileState *state;
	TDigest		td;
	float8		target_weight;
	float8		cumulative;
	int			i;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (ApproxPercentileState *) PG_GETARG_POINTER(0);
	td = state->td;

	if (td->num_centroids == 0)
		PG_RETURN_NULL();

	/* Compress before computing */
	tdigest_compress(td);

	/* Sort centroids */
	if (td->num_centroids > 1)
		qsort(td->centroids, td->num_centroids, sizeof(Centroid), centroid_cmp);

	/* Edge cases */
	if (state->percentile == 0.0)
		PG_RETURN_FLOAT8(td->min_val);
	if (state->percentile == 1.0)
		PG_RETURN_FLOAT8(td->max_val);

	/* Single centroid */
	if (td->num_centroids == 1)
		PG_RETURN_FLOAT8(td->centroids[0].mean);

	target_weight = state->percentile * td->total_weight;
	cumulative = 0.0;

	for (i = 0; i < td->num_centroids; i++)
	{
		float8		centroid_mid = cumulative + td->centroids[i].weight / 2.0;

		if (centroid_mid >= target_weight)
		{
			if (i == 0)
			{
				float8		inner = target_weight / (td->centroids[0].weight / 2.0);

				if (inner > 1.0)
					inner = 1.0;
				PG_RETURN_FLOAT8(td->min_val + inner * (td->centroids[0].mean - td->min_val));
			}
			else
			{
				float8		prev_mid = cumulative;
				float8		delta = centroid_mid - prev_mid;
				float8		frac;

				if (delta <= 0.0)
					PG_RETURN_FLOAT8(td->centroids[i].mean);

				frac = (target_weight - prev_mid) / delta;
				PG_RETURN_FLOAT8(td->centroids[i - 1].mean +
								 frac * (td->centroids[i].mean - td->centroids[i - 1].mean));
			}
		}

		cumulative += td->centroids[i].weight;
	}

	PG_RETURN_FLOAT8(td->max_val);
}
