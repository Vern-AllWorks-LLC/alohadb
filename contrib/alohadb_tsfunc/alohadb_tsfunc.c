/*-------------------------------------------------------------------------
 *
 * alohadb_tsfunc.c
 *	  Scientific time-series functions for AlohaDB.
 *
 *	  - ts_lttb():               Largest-Triangle-Three-Buckets downsampling
 *	  - ts_minmax_downsample():  min and max point per equal-width time bucket
 *	  - ts_resample():           irregular samples -> regular grid
 *	                             (linear / locf / nearest / mean, max_gap)
 *	  - ts_sort_dedup():         sort an out-of-order series, drop duplicates
 *	  - ts_asof_join():          O(n+m) merge-based as-of alignment of two
 *	                             timestamp arrays
 *	  The value codecs (ts_encode_* / ts_decode_*) live in tscodec.c.
 *
 *	  Prior art / patent notes: LTTB is from Sveinn Steinarsson, "Downsampling
 *	  Time Series for Visual Representation" (MSc thesis, University of
 *	  Iceland, 2013; MIT-licensed reference code).  Min/max-per-bucket
 *	  downsampling is a long-standing charting technique (cf. M4, Jugel et
 *	  al., VLDB 2014).  As-of joins are long established (kdb+ "aj",
 *	  pandas merge_asof).  The codecs in tscodec.c implement the openly
 *	  published Gorilla scheme (Pelkonen et al., VLDB 2015), which is also
 *	  implemented in Prometheus, InfluxDB and M3.  To stay clear of
 *	  Timescale's US patents 10,936,562 / 11,138,175 (moving batches of rows
 *	  into a compressed second table and routing queries between the two),
 *	  10,073,903 / 10,509,785 (insert-time chunk creation and chunk
 *	  policies), 11,030,189 (continuous aggregates via invalidation regions)
 *	  and 11,995,084 (tiering chunks to object storage), this extension
 *	  provides ONLY pure value-level functions: it never creates, moves,
 *	  compresses or deletes rows of any table, runs no background policy,
 *	  performs no query routing, creates no partitions and tracks no
 *	  invalidations.
 *
 * Copyright (c) 2026, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_tsfunc/alohadb_tsfunc.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <math.h>

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "common/int.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/float.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"

PG_MODULE_MAGIC_EXT(.name = "alohadb_tsfunc", .version = "1.0");

/* One observation; seq preserves input order for stable tie-breaking */
typedef struct TsPt
{
	int64		ts;
	double		v;
	bool		isnull;
	int32		seq;
} TsPt;

/* A growable list of observations */
typedef struct TsPtList
{
	TsPt	   *pts;
	int32		n;
	int32		cap;
} TsPtList;

typedef enum ResampleMethod
{
	RS_LINEAR,
	RS_LOCF,
	RS_NEAREST,
	RS_MEAN
} ResampleMethod;

/* Aggregate state for every aggregate in this file */
typedef struct TsAggState
{
	TsPtList	list;
	/* ts_lttb / ts_minmax_downsample */
	bool		have_param;
	int32		param;
	/* ts_resample */
	bool		have_step;
	int64		step;
	ResampleMethod method;
	bool		have_origin;
	int64		origin;
	bool		have_max_gap;
	int64		max_gap;
} TsAggState;

/* ----------------------------------------------------------------
 * Helpers
 * ----------------------------------------------------------------
 */

static void
ptlist_add(TsPtList *list, int64 ts, double v, bool isnull)
{
	if (list->n >= list->cap)
	{
		int32		newcap;

		if (list->cap >= PG_INT32_MAX / 2)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("too many points in time series")));
		newcap = list->cap == 0 ? 64 : list->cap * 2;
		if (list->pts == NULL)
			list->pts = palloc_extended(newcap * sizeof(TsPt), MCXT_ALLOC_HUGE);
		else
			list->pts = repalloc_huge(list->pts, newcap * sizeof(TsPt));
		list->cap = newcap;
	}
	list->pts[list->n].ts = ts;
	list->pts[list->n].v = v;
	list->pts[list->n].isnull = isnull;
	list->pts[list->n].seq = list->n;
	list->n++;
}

static int
tspt_cmp(const void *a, const void *b)
{
	const TsPt *pa = (const TsPt *) a;
	const TsPt *pb = (const TsPt *) b;

	if (pa->ts != pb->ts)
		return pa->ts < pb->ts ? -1 : 1;
	if (pa->seq != pb->seq)
		return pa->seq < pb->seq ? -1 : 1;
	return 0;
}

/* Sort by (ts, input order).  Total order, so repeated sorting is harmless. */
static void
ptlist_sort(TsPtList *list)
{
	if (list->n > 1)
		qsort(list->pts, list->n, sizeof(TsPt), tspt_cmp);
}

/*
 * Convert an interval to microseconds; month components have no fixed
 * length and are rejected.
 */
static int64
interval_to_usecs(const Interval *iv, const char *what)
{
	int64		result;

	if (INTERVAL_NOT_FINITE(iv))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s must be finite", what)));
	if (iv->month != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s must not contain months or years", what)));
	if (pg_mul_s64_overflow((int64) iv->day, USECS_PER_DAY, &result) ||
		pg_add_s64_overflow(result, iv->time, &result))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("%s is out of range", what)));
	return result;
}

/* Build a ts_point[] of the function's declared return type */
static Datum
build_point_array(FunctionCallInfo fcinfo, const TsPt *pts, int n)
{
	Oid			arrtype = get_func_rettype(fcinfo->flinfo->fn_oid);
	Oid			elemtype = get_element_type(arrtype);
	TupleDesc	tupdesc;
	Datum	   *elems;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	ArrayType  *arr;

	if (!OidIsValid(elemtype))
		elog(ERROR, "function return type is not an array");

	if (n == 0)
		PG_RETURN_ARRAYTYPE_P(construct_empty_array(elemtype));

	tupdesc = lookup_rowtype_tupdesc_copy(elemtype, -1);
	if (tupdesc->natts != 2)
		elog(ERROR, "unexpected ts_point definition");
	tupdesc = BlessTupleDesc(tupdesc);

	elems = palloc_extended(sizeof(Datum) * n, MCXT_ALLOC_HUGE);
	for (int i = 0; i < n; i++)
	{
		Datum		values[2];
		bool		nulls[2];
		HeapTuple	tup;

		values[0] = TimestampTzGetDatum(pts[i].ts);
		values[1] = Float8GetDatum(pts[i].v);
		nulls[0] = false;
		nulls[1] = pts[i].isnull;
		tup = heap_form_tuple(tupdesc, values, nulls);
		elems[i] = HeapTupleGetDatum(tup);
	}
	get_typlenbyvalalign(elemtype, &typlen, &typbyval, &typalign);
	arr = construct_array(elems, n, elemtype, typlen, typbyval, typalign);
	PG_RETURN_ARRAYTYPE_P(arr);
}

/*
 * Load paired (timestamptz[], float8[]) arrays into a list.  Pairs whose
 * timestamp is NULL are dropped; pairs whose value is NULL are dropped too
 * unless keep_null_values.
 */
static void
load_pair_arrays(ArrayType *tsarr, ArrayType *varr, TsPtList *list,
				 bool keep_null_values)
{
	Datum	   *tsd,
			   *vd;
	bool	   *tsn,
			   *vn;
	int			nts,
				nv;

	if (ARR_NDIM(tsarr) > 1 || ARR_NDIM(varr) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("only one-dimensional arrays are supported")));
	deconstruct_array(tsarr, TIMESTAMPTZOID, sizeof(int64), FLOAT8PASSBYVAL,
					  TYPALIGN_DOUBLE, &tsd, &tsn, &nts);
	deconstruct_array_builtin(varr, FLOAT8OID, &vd, &vn, &nv);
	if (nts != nv)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("timestamp and value arrays must have the same length (%d vs %d)",
						nts, nv)));
	for (int i = 0; i < nts; i++)
	{
		if (tsn[i])
			continue;
		if (vn[i] && !keep_null_values)
			continue;
		ptlist_add(list, DatumGetTimestampTz(tsd[i]),
				   vn[i] ? 0.0 : DatumGetFloat8(vd[i]), vn[i]);
	}
}

static TsAggState *
get_agg_state(FunctionCallInfo fcinfo, const char *fname)
{
	MemoryContext aggcontext;
	TsAggState *state;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "%s called in non-aggregate context", fname);

	if (!PG_ARGISNULL(0))
		return (TsAggState *) PG_GETARG_POINTER(0);

	state = (TsAggState *) MemoryContextAllocZero(aggcontext, sizeof(TsAggState));
	return state;
}

/*
 * Adding points must allocate in the aggregate context.
 */
static void
agg_add_point(FunctionCallInfo fcinfo, TsAggState *state, int64 ts, double v)
{
	MemoryContext aggcontext;
	MemoryContext old;

	(void) AggCheckCallContext(fcinfo, &aggcontext);
	old = MemoryContextSwitchTo(aggcontext);
	ptlist_add(&state->list, ts, v, false);
	MemoryContextSwitchTo(old);
}

/* ----------------------------------------------------------------
 * LTTB (Steinarsson 2013)
 * ----------------------------------------------------------------
 */

/*
 * Downsample sorted points src[0..n-1] to "threshold" points, writing them
 * to dst (which must have room for min(n, threshold) points).  Returns the
 * number of points written.
 */
static int
lttb_core(const TsPt *src, int n, int threshold, TsPt *dst)
{
	double		every;
	int			a = 0;
	int			out = 0;
	int64		t0;

	if (threshold >= n || n <= 2)
	{
		memcpy(dst, src, sizeof(TsPt) * n);
		return n;
	}

	/* x coordinates: seconds relative to the first point */
	t0 = src[0].ts;
#define X(i) (((double) src[i].ts - (double) t0) / 1000000.0)

	every = (double) (n - 2) / (double) (threshold - 2);
	dst[out++] = src[0];

	for (int i = 0; i < threshold - 2; i++)
	{
		int			avg_start = (int) floor((i + 1) * every) + 1;
		int			avg_end = (int) floor((i + 2) * every) + 1;
		int			range_offs = (int) floor(i * every) + 1;
		int			range_to = (int) floor((i + 1) * every) + 1;
		double		avg_x = 0.0;
		double		avg_y = 0.0;
		double		ax,
					ay;
		double		max_area = -1.0;
		int			next_a = range_offs;

		CHECK_FOR_INTERRUPTS();

		if (avg_end > n)
			avg_end = n;
		if (avg_start >= avg_end)	/* cannot happen, but be safe */
			avg_start = avg_end - 1;
		for (int j = avg_start; j < avg_end; j++)
		{
			avg_x += X(j);
			avg_y += src[j].v;
		}
		avg_x /= (avg_end - avg_start);
		avg_y /= (avg_end - avg_start);

		ax = X(a);
		ay = src[a].v;
		if (range_to > n - 1)
			range_to = n - 1;
		for (int j = range_offs; j < range_to; j++)
		{
			double		area = fabs((ax - avg_x) * (src[j].v - ay) -
									(ax - X(j)) * (avg_y - ay)) * 0.5;

			if (area > max_area)
			{
				max_area = area;
				next_a = j;
			}
		}
		dst[out++] = src[next_a];
		a = next_a;
	}
#undef X
	dst[out++] = src[n - 1];
	return out;
}

static Datum
lttb_finish(FunctionCallInfo fcinfo, TsPtList *list, int threshold)
{
	TsPt	   *dst;
	int			nout;

	if (threshold < 3)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("LTTB threshold must be at least 3, got %d", threshold)));
	ptlist_sort(list);
	dst = palloc_extended(sizeof(TsPt) * Max(list->n, 1), MCXT_ALLOC_HUGE);
	nout = lttb_core(list->pts, list->n, threshold, dst);
	return build_point_array(fcinfo, dst, nout);
}

/*
 * Transition function shared by ts_lttb and ts_minmax_downsample:
 * (state, ts, value, int parameter).  Rows with a NULL ts or value are
 * ignored.
 */
PG_FUNCTION_INFO_V1(ts_points_int_trans);
Datum
ts_points_int_trans(PG_FUNCTION_ARGS)
{
	TsAggState *state = get_agg_state(fcinfo, "ts_points_int_trans");

	if (!PG_ARGISNULL(3))
	{
		state->param = PG_GETARG_INT32(3);
		state->have_param = true;
	}
	if (!PG_ARGISNULL(1) && !PG_ARGISNULL(2))
		agg_add_point(fcinfo, state, PG_GETARG_TIMESTAMPTZ(1),
					  PG_GETARG_FLOAT8(2));
	PG_RETURN_POINTER(state);
}

PG_FUNCTION_INFO_V1(ts_lttb_final);
Datum
ts_lttb_final(PG_FUNCTION_ARGS)
{
	TsAggState *state;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	state = (TsAggState *) PG_GETARG_POINTER(0);
	if (!state->have_param)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("LTTB threshold must not be NULL")));
	return lttb_finish(fcinfo, &state->list, state->param);
}

PG_FUNCTION_INFO_V1(ts_lttb_arrays);
Datum
ts_lttb_arrays(PG_FUNCTION_ARGS)
{
	TsPtList	list = {0};

	load_pair_arrays(PG_GETARG_ARRAYTYPE_P(0), PG_GETARG_ARRAYTYPE_P(1),
					 &list, false);
	return lttb_finish(fcinfo, &list, PG_GETARG_INT32(2));
}

/* ----------------------------------------------------------------
 * Min/max per equal-width time bucket
 * ----------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(ts_minmax_final);
Datum
ts_minmax_final(PG_FUNCTION_ARGS)
{
	TsAggState *state;
	TsPtList   *list;
	int			nbuckets;
	int		   *minidx,
			   *maxidx;
	TsPt	   *dst;
	int			nout = 0;
	int64		tmin,
				tmax;
	double		range;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	state = (TsAggState *) PG_GETARG_POINTER(0);
	if (!state->have_param)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("number of buckets must not be NULL")));
	nbuckets = state->param;
	if (nbuckets < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("number of buckets must be at least 1, got %d", nbuckets)));
	list = &state->list;
	ptlist_sort(list);
	if (list->n == 0)
		return build_point_array(fcinfo, NULL, 0);

	/* there are never more non-empty buckets than points */
	if (nbuckets > list->n)
		nbuckets = list->n;
	minidx = palloc_array(int, nbuckets);
	maxidx = palloc_array(int, nbuckets);
	for (int b = 0; b < nbuckets; b++)
		minidx[b] = maxidx[b] = -1;

	tmin = list->pts[0].ts;
	tmax = list->pts[list->n - 1].ts;
	range = (double) ((uint64) tmax - (uint64) tmin);

	for (int i = 0; i < list->n; i++)
	{
		int			b = 0;
		double		v = list->pts[i].v;

		if (range > 0)
		{
			double		pos = (double) ((uint64) list->pts[i].ts - (uint64) tmin) /
				range * nbuckets;

			b = (int) Min(pos, (double) (nbuckets - 1));
			if (b < 0)
				b = 0;
		}
		if (minidx[b] < 0)
			minidx[b] = maxidx[b] = i;
		else
		{
			if (float8_cmp_internal(v, list->pts[minidx[b]].v) < 0)
				minidx[b] = i;
			if (float8_cmp_internal(v, list->pts[maxidx[b]].v) > 0)
				maxidx[b] = i;
		}
	}

	dst = palloc_array(TsPt, nbuckets * 2);
	for (int b = 0; b < nbuckets; b++)
	{
		int			lo,
					hi;

		if (minidx[b] < 0)
			continue;
		lo = Min(minidx[b], maxidx[b]);
		hi = Max(minidx[b], maxidx[b]);
		dst[nout++] = list->pts[lo];
		if (hi != lo)
			dst[nout++] = list->pts[hi];
	}
	return build_point_array(fcinfo, dst, nout);
}

/* ----------------------------------------------------------------
 * Resampling an irregular series onto a regular grid
 * ----------------------------------------------------------------
 */

static ResampleMethod
parse_method(text *t)
{
	char	   *s = text_to_cstring(t);

	if (pg_strcasecmp(s, "linear") == 0)
		return RS_LINEAR;
	if (pg_strcasecmp(s, "locf") == 0)
		return RS_LOCF;
	if (pg_strcasecmp(s, "nearest") == 0)
		return RS_NEAREST;
	if (pg_strcasecmp(s, "mean") == 0)
		return RS_MEAN;
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("unknown resample method \"%s\"", s),
			 errhint("Valid methods are linear, locf, nearest and mean.")));
	return RS_LINEAR;			/* keep compiler quiet */
}

/*
 * (state, ts, value, step [, method [, origin [, max_gap]]])
 * Parameters are taken from each row (they are expected to be constant).
 */
PG_FUNCTION_INFO_V1(ts_resample_trans);
Datum
ts_resample_trans(PG_FUNCTION_ARGS)
{
	TsAggState *state = get_agg_state(fcinfo, "ts_resample_trans");
	int			nargs = PG_NARGS();

	/* parse parameters once, on the first call */
	if (!state->have_step)
	{
		int64		step;

		if (PG_ARGISNULL(3))
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("resample step must not be NULL")));
		step = interval_to_usecs(PG_GETARG_INTERVAL_P(3), "resample step");
		if (step <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("resample step must be positive")));
		state->step = step;
		state->have_step = true;

		state->method = RS_LINEAR;
		if (nargs > 4 && !PG_ARGISNULL(4))
			state->method = parse_method(PG_GETARG_TEXT_PP(4));

		if (nargs > 5 && !PG_ARGISNULL(5))
		{
			state->origin = PG_GETARG_TIMESTAMPTZ(5);
			if (TIMESTAMP_NOT_FINITE(state->origin))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("resample origin must be finite")));
			state->have_origin = true;
		}

		if (nargs > 6 && !PG_ARGISNULL(6))
		{
			int64		gap = interval_to_usecs(PG_GETARG_INTERVAL_P(6),
												"max_gap");

			if (gap < 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("max_gap must not be negative")));
			state->max_gap = gap;
			state->have_max_gap = true;
		}
	}

	if (!PG_ARGISNULL(1) && !PG_ARGISNULL(2))
	{
		TimestampTz ts = PG_GETARG_TIMESTAMPTZ(1);

		if (TIMESTAMP_NOT_FINITE(ts))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("cannot resample infinite timestamps")));
		agg_add_point(fcinfo, state, ts, PG_GETARG_FLOAT8(2));
	}
	PG_RETURN_POINTER(state);
}

/* floor(a / b) for b > 0 */
static int64
floor_div(int64 a, int64 b)
{
	int64		q = a / b;

	if ((a % b) != 0 && a < 0)
		q--;
	return q;
}

static int64
grid_point(int64 origin, int64 k, int64 step)
{
	int64		off,
				g;

	if (pg_mul_s64_overflow(k, step, &off) ||
		pg_add_s64_overflow(origin, off, &g) ||
		TIMESTAMP_NOT_FINITE(g))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("resample grid is out of range")));
	return g;
}

PG_FUNCTION_INFO_V1(ts_resample_final);
Datum
ts_resample_final(PG_FUNCTION_ARGS)
{
	TsAggState *state;
	TsPtList   *list;
	TsPt	   *obs;
	int			m;
	int64		step,
				origin,
				tmin,
				tmax,
				kfirst,
				klast,
				d;
	int64		npoints;
	TsPt	   *out;
	int			j;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	state = (TsAggState *) PG_GETARG_POINTER(0);
	list = &state->list;
	if (list->n == 0)
		PG_RETURN_NULL();

	ptlist_sort(list);
	step = state->step;
	tmin = list->pts[0].ts;
	tmax = list->pts[list->n - 1].ts;
	origin = state->have_origin ? state->origin : tmin;

	/* grid index range */
	if (pg_sub_s64_overflow(tmin, origin, &d))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("resample origin is too far from the data")));
	if (state->method == RS_MEAN)
		kfirst = floor_div(d, step);
	else
		kfirst = -floor_div(-d, step);	/* ceil */
	if (pg_sub_s64_overflow(tmax, origin, &d))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("resample origin is too far from the data")));
	klast = floor_div(d, step);

	npoints = klast - kfirst + 1;
	if (npoints <= 0)
		return build_point_array(fcinfo, NULL, 0);
	if (npoints > (int64) MaxArraySize)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("resample grid would have " INT64_FORMAT " points, more than the maximum of %d",
						npoints, (int) MaxArraySize),
				 errhint("Use a larger step.")));

	out = palloc_extended(sizeof(TsPt) * npoints, MCXT_ALLOC_HUGE);

	if (state->method == RS_MEAN)
	{
		/* bucket average over [g, g + step); every sample counts */
		j = 0;
		for (int64 k = 0; k < npoints; k++)
		{
			int64		g = grid_point(origin, kfirst + k, step);
			int64		gend = grid_point(origin, kfirst + k + 1, step);
			double		sum = 0.0;
			int64		cnt = 0;

			CHECK_FOR_INTERRUPTS();
			while (j < list->n && list->pts[j].ts < gend)
			{
				sum += list->pts[j].v;
				cnt++;
				j++;
			}
			out[k].ts = g;
			out[k].isnull = (cnt == 0);
			out[k].v = cnt ? sum / cnt : 0.0;
		}
		return build_point_array(fcinfo, out, (int) npoints);
	}

	/* de-duplicate equal timestamps, keeping the last input row */
	obs = palloc_extended(sizeof(TsPt) * list->n, MCXT_ALLOC_HUGE);
	m = 0;
	for (int i = 0; i < list->n; i++)
	{
		if (m > 0 && obs[m - 1].ts == list->pts[i].ts)
			obs[m - 1] = list->pts[i];
		else
			obs[m++] = list->pts[i];
	}

	j = 0;						/* obs[j] = last observation with ts <= g */
	for (int64 k = 0; k < npoints; k++)
	{
		int64		g = grid_point(origin, kfirst + k, step);
		const TsPt *p,
				   *q;

		CHECK_FOR_INTERRUPTS();
		while (j + 1 < m && obs[j + 1].ts <= g)
			j++;
		p = &obs[j];
		out[k].ts = g;
		out[k].isnull = false;

		if (p->ts == g)
		{
			out[k].v = p->v;
			continue;
		}
		/* tmin <= g <= tmax and p->ts < g, so a next observation exists */
		Assert(j + 1 < m);
		q = &obs[j + 1];

		switch (state->method)
		{
			case RS_LINEAR:
				if (state->have_max_gap && q->ts - p->ts > state->max_gap)
					out[k].isnull = true;
				else
				{
					double		frac = (double) (g - p->ts) /
						(double) (q->ts - p->ts);

					out[k].v = p->v + (q->v - p->v) * frac;
				}
				break;
			case RS_LOCF:
				if (state->have_max_gap && g - p->ts > state->max_gap)
					out[k].isnull = true;
				else
					out[k].v = p->v;
				break;
			case RS_NEAREST:
				{
					int64		dp = g - p->ts;
					int64		dq = q->ts - g;
					const TsPt *best = (dp <= dq) ? p : q;

					if (state->have_max_gap && Min(dp, dq) > state->max_gap)
						out[k].isnull = true;
					else
						out[k].v = best->v;
				}
				break;
			case RS_MEAN:
				Assert(false);
				break;
		}
		if (out[k].isnull)
			out[k].v = 0.0;
	}
	return build_point_array(fcinfo, out, (int) npoints);
}

/* ----------------------------------------------------------------
 * ts_sort_dedup
 * ----------------------------------------------------------------
 */
typedef enum DedupKeep
{
	KEEP_FIRST,
	KEEP_LAST,
	KEEP_MIN,
	KEEP_MAX,
	KEEP_AVG
} DedupKeep;

PG_FUNCTION_INFO_V1(ts_sort_dedup);
Datum
ts_sort_dedup(PG_FUNCTION_ARGS)
{
	TsPtList	list = {0};
	char	   *keeps = text_to_cstring(PG_GETARG_TEXT_PP(2));
	DedupKeep	keep;
	TsPt	   *out;
	int			nout = 0;
	int			i;

	if (pg_strcasecmp(keeps, "first") == 0)
		keep = KEEP_FIRST;
	else if (pg_strcasecmp(keeps, "last") == 0)
		keep = KEEP_LAST;
	else if (pg_strcasecmp(keeps, "min") == 0)
		keep = KEEP_MIN;
	else if (pg_strcasecmp(keeps, "max") == 0)
		keep = KEEP_MAX;
	else if (pg_strcasecmp(keeps, "avg") == 0 || pg_strcasecmp(keeps, "mean") == 0)
		keep = KEEP_AVG;
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unknown keep option \"%s\"", keeps),
				 errhint("Valid options are first, last, min, max and avg.")));

	load_pair_arrays(PG_GETARG_ARRAYTYPE_P(0), PG_GETARG_ARRAYTYPE_P(1),
					 &list, true);
	ptlist_sort(&list);
	out = palloc_extended(sizeof(TsPt) * Max(list.n, 1), MCXT_ALLOC_HUGE);

	i = 0;
	while (i < list.n)
	{
		int			e = i;
		TsPt		r;

		while (e < list.n && list.pts[e].ts == list.pts[i].ts)
			e++;
		/* group is [i, e) in input order */
		switch (keep)
		{
			case KEEP_FIRST:
				r = list.pts[i];
				break;
			case KEEP_LAST:
				r = list.pts[e - 1];
				break;
			default:
				{
					/* aggregate over non-NULL values; NULL if none */
					double		acc = 0.0;
					int			cnt = 0;

					for (int k = i; k < e; k++)
					{
						double		v = list.pts[k].v;

						if (list.pts[k].isnull)
							continue;
						if (cnt == 0)
							acc = v;
						else if (keep == KEEP_MIN)
						{
							if (float8_cmp_internal(v, acc) < 0)
								acc = v;
						}
						else if (keep == KEEP_MAX)
						{
							if (float8_cmp_internal(v, acc) > 0)
								acc = v;
						}
						else
							acc += v;
						cnt++;
					}
					r = list.pts[i];
					r.isnull = (cnt == 0);
					r.v = cnt == 0 ? 0.0 : (keep == KEEP_AVG ? acc / cnt : acc);
				}
				break;
		}
		out[nout++] = r;
		i = e;
	}
	return build_point_array(fcinfo, out, nout);
}

/* ----------------------------------------------------------------
 * ts_asof_join: merge-based alignment of two timestamp arrays
 * ----------------------------------------------------------------
 */
typedef struct IdxTs
{
	int64		ts;
	int32		idx;			/* 0-based original position */
} IdxTs;

static int
idxts_cmp(const void *a, const void *b)
{
	const IdxTs *pa = (const IdxTs *) a;
	const IdxTs *pb = (const IdxTs *) b;

	if (pa->ts != pb->ts)
		return pa->ts < pb->ts ? -1 : 1;
	if (pa->idx != pb->idx)
		return pa->idx < pb->idx ? -1 : 1;
	return 0;
}

/* Collect the non-NULL elements of a timestamptz[] with their positions */
static IdxTs *
load_idxts(ArrayType *arr, int *nall, int *nvalid)
{
	Datum	   *d;
	bool	   *nulls;
	int			n,
				m = 0;
	IdxTs	   *res;

	if (ARR_NDIM(arr) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("only one-dimensional arrays are supported")));
	deconstruct_array(arr, TIMESTAMPTZOID, sizeof(int64), FLOAT8PASSBYVAL,
					  TYPALIGN_DOUBLE, &d, &nulls, &n);
	res = palloc_extended(sizeof(IdxTs) * Max(n, 1), MCXT_ALLOC_HUGE);
	for (int i = 0; i < n; i++)
	{
		if (nulls[i])
			continue;
		res[m].ts = DatumGetTimestampTz(d[i]);
		res[m].idx = i;
		m++;
	}
	if (m > 1)
		qsort(res, m, sizeof(IdxTs), idxts_cmp);
	*nall = n;
	*nvalid = m;
	return res;
}

PG_FUNCTION_INFO_V1(ts_asof_join);
Datum
ts_asof_join(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	IdxTs	   *L,
			   *R;
	int			nl_all,
				nl,
				nr_all,
				nr;
	bool		have_tol = false;
	uint64		tol = 0;
	int			dir = 0;		/* 0 backward, 1 forward, 2 nearest */
	int32	   *match;
	int			p = 0,
				q = 0;

	InitMaterializedSRF(fcinfo, 0);

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		return (Datum) 0;

	if (!PG_ARGISNULL(2))
	{
		int64		t = interval_to_usecs(PG_GETARG_INTERVAL_P(2), "tolerance");

		if (t < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("tolerance must not be negative")));
		tol = (uint64) t;
		have_tol = true;
	}
	if (!PG_ARGISNULL(3))
	{
		char	   *s = text_to_cstring(PG_GETARG_TEXT_PP(3));

		if (pg_strcasecmp(s, "backward") == 0)
			dir = 0;
		else if (pg_strcasecmp(s, "forward") == 0)
			dir = 1;
		else if (pg_strcasecmp(s, "nearest") == 0)
			dir = 2;
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("direction must be backward, forward or nearest, not \"%s\"", s)));
	}

	L = load_idxts(PG_GETARG_ARRAYTYPE_P(0), &nl_all, &nl);
	R = load_idxts(PG_GETARG_ARRAYTYPE_P(1), &nr_all, &nr);

	match = palloc_extended(sizeof(int32) * Max(nl_all, 1), MCXT_ALLOC_HUGE);
	for (int i = 0; i < nl_all; i++)
		match[i] = -1;

	/*
	 * Single merge pass.  p ends past the last right element with ts <= l
	 * (so p-1 is the backward candidate, the last of any equal-ts run); q
	 * is the first right element with ts >= l (the forward candidate, the
	 * first of any equal-ts run).  Distances are computed in uint64, which
	 * is exact because the operands are ordered.
	 */
	for (int i = 0; i < nl; i++)
	{
		int64		lt = L[i].ts;
		int			best = -1;
		bool		bok = false,
					fok = false;
		uint64		bd = 0,
					fd = 0;

		while (q < nr && R[q].ts < lt)
			q++;
		if (p < q)
			p = q;
		while (p < nr && R[p].ts <= lt)
			p++;

		if (dir != 1 && p > 0)
		{
			bd = (uint64) lt - (uint64) R[p - 1].ts;
			bok = !have_tol || bd <= tol;
		}
		if (dir != 0 && q < nr)
		{
			fd = (uint64) R[q].ts - (uint64) lt;
			fok = !have_tol || fd <= tol;
		}
		if (bok && (!fok || bd <= fd))
			best = R[p - 1].idx;
		else if (fok)
			best = R[q].idx;
		match[L[i].idx] = best;
	}

	for (int i = 0; i < nl_all; i++)
	{
		Datum		values[2];
		bool		nulls[2];

		values[0] = Int32GetDatum(i + 1);
		nulls[0] = false;
		values[1] = Int32GetDatum(match[i] + 1);
		nulls[1] = (match[i] < 0);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
	return (Datum) 0;
}
