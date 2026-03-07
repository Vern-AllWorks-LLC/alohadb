#include "postgres.h"
#include "fmgr.h"
#include "windowapi.h"
#include "funcapi.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "utils/datetime.h"
#include "utils/snapmgr.h"
#include "lib/stringinfo.h"
#include "alohadb_analytics.h"

PG_FUNCTION_INFO_V1(analytics_interpolate);
PG_FUNCTION_INFO_V1(analytics_moving_avg);
PG_FUNCTION_INFO_V1(analytics_moving_sum);
PG_FUNCTION_INFO_V1(analytics_gap_fill);
PG_FUNCTION_INFO_V1(analytics_first_transfn);
PG_FUNCTION_INFO_V1(analytics_first_finalfn);
PG_FUNCTION_INFO_V1(analytics_last_transfn);
PG_FUNCTION_INFO_V1(analytics_last_finalfn);
PG_FUNCTION_INFO_V1(analytics_delta);
PG_FUNCTION_INFO_V1(analytics_rate);

/*
 * State for first/last aggregates.
 * Stores (value_datum, time_datum, is_set, type info).
 */
typedef struct FirstLastState
{
	Datum		value;
	Datum		time;
	bool		is_set;
	Oid			val_type;
	Oid			time_type;
	int16		val_typlen;
	bool		val_typbyval;
	int16		time_typlen;
	bool		time_typbyval;
} FirstLastState;

/*
 * analytics_interpolate(time timestamptz, value float8, method text DEFAULT 'linear')
 * WINDOW FUNCTION -- linear interpolation between surrounding non-NULL values.
 *
 * If current row's value is not NULL, returns it as-is.
 * If NULL, finds the nearest non-NULL before and after, interpolates linearly.
 */
Datum
analytics_interpolate(PG_FUNCTION_ARGS)
{
	WindowObject winobj = PG_WINDOW_OBJECT();
	int64		curpos = WinGetCurrentPosition(winobj);
	int64		totalrows = WinGetPartitionRowCount(winobj);
	Datum		cur_val;
	bool		isnull;
	float8		result;

	/* Get current value */
	cur_val = WinGetFuncArgInPartition(winobj, 1, curpos, WINDOW_SEEK_HEAD, false, &isnull, NULL);
	if (!isnull)
		PG_RETURN_FLOAT8(DatumGetFloat8(cur_val));

	/* Find previous non-null */
	{
		int64		prev_pos = -1;
		float8		prev_val = 0;
		int64		next_pos = -1;
		float8		next_val = 0;
		int64		i;

		for (i = curpos - 1; i >= 0; i--)
		{
			Datum		d = WinGetFuncArgInPartition(winobj, 1, i, WINDOW_SEEK_HEAD, false, &isnull, NULL);

			if (!isnull)
			{
				prev_pos = i;
				prev_val = DatumGetFloat8(d);
				break;
			}
		}

		for (i = curpos + 1; i < totalrows; i++)
		{
			Datum		d = WinGetFuncArgInPartition(winobj, 1, i, WINDOW_SEEK_HEAD, false, &isnull, NULL);

			if (!isnull)
			{
				next_pos = i;
				next_val = DatumGetFloat8(d);
				break;
			}
		}

		if (prev_pos >= 0 && next_pos >= 0)
		{
			/* Linear interpolation */
			float8		frac = (float8) (curpos - prev_pos) / (float8) (next_pos - prev_pos);

			result = prev_val + frac * (next_val - prev_val);
		}
		else if (prev_pos >= 0)
			result = prev_val;
		else if (next_pos >= 0)
			result = next_val;
		else
			PG_RETURN_NULL();

		PG_RETURN_FLOAT8(result);
	}
}

/*
 * analytics_moving_avg(value float8, window_size int)
 * WINDOW FUNCTION -- moving average over the last window_size rows.
 */
Datum
analytics_moving_avg(PG_FUNCTION_ARGS)
{
	WindowObject winobj = PG_WINDOW_OBJECT();
	int64		curpos = WinGetCurrentPosition(winobj);
	float8		sum = 0;
	int			count = 0;
	int64		start;
	int64		i;
	bool		isnull;
	int32		window_size;

	window_size = DatumGetInt32(WinGetFuncArgCurrent(winobj, 1, &isnull));
	if (isnull)
		PG_RETURN_NULL();

	start = curpos - window_size + 1;
	if (start < 0)
		start = 0;

	for (i = start; i <= curpos; i++)
	{
		Datum		d = WinGetFuncArgInPartition(winobj, 0, i, WINDOW_SEEK_HEAD, false, &isnull, NULL);

		if (!isnull)
		{
			sum += DatumGetFloat8(d);
			count++;
		}
	}

	if (count == 0)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8(sum / count);
}

/*
 * analytics_moving_sum(value float8, window_size int)
 * WINDOW FUNCTION
 */
Datum
analytics_moving_sum(PG_FUNCTION_ARGS)
{
	WindowObject winobj = PG_WINDOW_OBJECT();
	int64		curpos = WinGetCurrentPosition(winobj);
	float8		sum = 0;
	int			count = 0;
	int64		start;
	int64		i;
	bool		isnull;
	int32		window_size;

	window_size = DatumGetInt32(WinGetFuncArgCurrent(winobj, 1, &isnull));
	if (isnull)
		PG_RETURN_NULL();

	start = curpos - window_size + 1;
	if (start < 0)
		start = 0;

	for (i = start; i <= curpos; i++)
	{
		Datum		d = WinGetFuncArgInPartition(winobj, 0, i, WINDOW_SEEK_HEAD, false, &isnull, NULL);

		if (!isnull)
		{
			sum += DatumGetFloat8(d);
			count++;
		}
	}

	if (count == 0)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8(sum);
}

/*
 * analytics_gap_fill(time_col timestamptz, bucket_width interval,
 *                    range_start timestamptz, range_end timestamptz)
 *
 * Returns a set of timestamptz values filling gaps in the time range.
 */

typedef struct GapFillState
{
	TimestampTz current;
	TimestampTz end_ts;
	Interval	bucket;
} GapFillState;

Datum
analytics_gap_fill(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcxt;
		GapFillState *state;
		TimestampTz start_ts;
		TimestampTz end_ts;
		Interval   *bucket;
		int64		bucket_usec;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcxt = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		bucket = PG_GETARG_INTERVAL_P(1);
		start_ts = PG_GETARG_TIMESTAMPTZ(2);
		end_ts = PG_GETARG_TIMESTAMPTZ(3);

		state = palloc(sizeof(GapFillState));
		state->current = start_ts;
		state->end_ts = end_ts;
		memcpy(&state->bucket, bucket, sizeof(Interval));

		funcctx->user_fctx = state;

		/* Estimate max calls */
		bucket_usec = bucket->time + (int64) bucket->day * USECS_PER_DAY +
					  (int64) bucket->month * 30 * USECS_PER_DAY;
		if (bucket_usec > 0)
			funcctx->max_calls = (end_ts - start_ts) / bucket_usec + 1;
		else
			funcctx->max_calls = 0;

		MemoryContextSwitchTo(oldcxt);
	}

	funcctx = SRF_PERCALL_SETUP();

	{
		GapFillState *state = (GapFillState *) funcctx->user_fctx;

		if (state->current <= state->end_ts)
		{
			TimestampTz result = state->current;

			/* Advance for next call */
			state->current = DatumGetTimestampTz(
				DirectFunctionCall2(timestamptz_pl_interval,
									TimestampTzGetDatum(state->current),
									IntervalPGetDatum(&state->bucket)));
			SRF_RETURN_NEXT(funcctx, TimestampTzGetDatum(result));
		}
		else
			SRF_RETURN_DONE(funcctx);
	}
}

/*
 * analytics_first_transfn(internal, anyelement value, timestamptz time)
 * Keeps the value with the smallest timestamp.
 */
Datum
analytics_first_transfn(PG_FUNCTION_ARGS)
{
	MemoryContext aggcxt;
	FirstLastState *state;

	if (!AggCheckCallContext(fcinfo, &aggcxt))
		ereport(ERROR, (errmsg("called in non-aggregate context")));

	if (PG_ARGISNULL(1) || PG_ARGISNULL(2))
		PG_RETURN_POINTER(PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));

	if (PG_ARGISNULL(0))
	{
		MemoryContext old = MemoryContextSwitchTo(aggcxt);

		state = palloc0(sizeof(FirstLastState));
		state->is_set = false;
		MemoryContextSwitchTo(old);
	}
	else
		state = (FirstLastState *) PG_GETARG_POINTER(0);

	{
		Datum		new_time = PG_GETARG_DATUM(2);

		if (!state->is_set ||
			DatumGetTimestampTz(new_time) < DatumGetTimestampTz(state->time))
		{
			MemoryContext old = MemoryContextSwitchTo(aggcxt);

			/* For simplicity, store as float8 (common case for time-series values) */
			state->value = PG_GETARG_DATUM(1);
			state->time = new_time;
			state->is_set = true;

			MemoryContextSwitchTo(old);
		}
	}

	PG_RETURN_POINTER(state);
}

Datum
analytics_first_finalfn(PG_FUNCTION_ARGS)
{
	FirstLastState *state;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (FirstLastState *) PG_GETARG_POINTER(0);
	if (!state->is_set)
		PG_RETURN_NULL();

	PG_RETURN_DATUM(state->value);
}

/*
 * analytics_last_transfn -- keeps value with the largest timestamp
 */
Datum
analytics_last_transfn(PG_FUNCTION_ARGS)
{
	MemoryContext aggcxt;
	FirstLastState *state;

	if (!AggCheckCallContext(fcinfo, &aggcxt))
		ereport(ERROR, (errmsg("called in non-aggregate context")));

	if (PG_ARGISNULL(1) || PG_ARGISNULL(2))
		PG_RETURN_POINTER(PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));

	if (PG_ARGISNULL(0))
	{
		MemoryContext old = MemoryContextSwitchTo(aggcxt);

		state = palloc0(sizeof(FirstLastState));
		state->is_set = false;
		MemoryContextSwitchTo(old);
	}
	else
		state = (FirstLastState *) PG_GETARG_POINTER(0);

	{
		Datum		new_time = PG_GETARG_DATUM(2);

		if (!state->is_set ||
			DatumGetTimestampTz(new_time) > DatumGetTimestampTz(state->time))
		{
			MemoryContext old = MemoryContextSwitchTo(aggcxt);

			state->value = PG_GETARG_DATUM(1);
			state->time = new_time;
			state->is_set = true;

			MemoryContextSwitchTo(old);
		}
	}

	PG_RETURN_POINTER(state);
}

Datum
analytics_last_finalfn(PG_FUNCTION_ARGS)
{
	FirstLastState *state;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (FirstLastState *) PG_GETARG_POINTER(0);
	if (!state->is_set)
		PG_RETURN_NULL();

	PG_RETURN_DATUM(state->value);
}

/*
 * analytics_delta(value float8) -- WINDOW FUNCTION
 * Returns difference between current and previous row's value.
 */
Datum
analytics_delta(PG_FUNCTION_ARGS)
{
	WindowObject winobj = PG_WINDOW_OBJECT();
	int64		curpos = WinGetCurrentPosition(winobj);
	bool		isnull;
	Datum		cur_val,
				prev_val;

	cur_val = WinGetFuncArgInPartition(winobj, 0, curpos, WINDOW_SEEK_HEAD, false, &isnull, NULL);
	if (isnull)
		PG_RETURN_NULL();

	if (curpos == 0)
		PG_RETURN_NULL();	/* no previous row */

	prev_val = WinGetFuncArgInPartition(winobj, 0, curpos - 1, WINDOW_SEEK_HEAD, false, &isnull, NULL);
	if (isnull)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8(DatumGetFloat8(cur_val) - DatumGetFloat8(prev_val));
}

/*
 * analytics_rate(value float8, time timestamptz) -- WINDOW FUNCTION
 * Returns delta(value) / delta(time) per second.
 */
Datum
analytics_rate(PG_FUNCTION_ARGS)
{
	WindowObject winobj = PG_WINDOW_OBJECT();
	int64		curpos = WinGetCurrentPosition(winobj);
	bool		isnull_v,
				isnull_t;
	Datum		cur_val,
				prev_val,
				cur_time,
				prev_time;
	float8		delta_val,
				delta_time_sec;

	if (curpos == 0)
		PG_RETURN_NULL();

	cur_val = WinGetFuncArgInPartition(winobj, 0, curpos, WINDOW_SEEK_HEAD, false, &isnull_v, NULL);
	cur_time = WinGetFuncArgInPartition(winobj, 1, curpos, WINDOW_SEEK_HEAD, false, &isnull_t, NULL);
	if (isnull_v || isnull_t)
		PG_RETURN_NULL();

	prev_val = WinGetFuncArgInPartition(winobj, 0, curpos - 1, WINDOW_SEEK_HEAD, false, &isnull_v, NULL);
	prev_time = WinGetFuncArgInPartition(winobj, 1, curpos - 1, WINDOW_SEEK_HEAD, false, &isnull_t, NULL);
	if (isnull_v || isnull_t)
		PG_RETURN_NULL();

	delta_val = DatumGetFloat8(cur_val) - DatumGetFloat8(prev_val);
	delta_time_sec = (float8) (DatumGetTimestampTz(cur_time) - DatumGetTimestampTz(prev_time)) / 1000000.0;

	if (delta_time_sec == 0)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8(delta_val / delta_time_sec);
}
