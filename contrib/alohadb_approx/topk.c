/*-------------------------------------------------------------------------
 *
 * topk.c
 *	  Space-Saving approximate top-K aggregate.
 *
 *	  Implements the Space-Saving algorithm (Metwally, Agrawal, El Abbadi,
 *	  2005) as a PostgreSQL aggregate function.  The aggregate maintains
 *	  a bounded set of at most k (item, count) pairs.  When a new item
 *	  arrives and the set is full, the item with the minimum count is
 *	  evicted and replaced by the new item with an incremented count.
 *
 *	  The aggregate approx_topk_agg(anyelement, int) returns internal.
 *	  The SRF approx_topk(internal) expands the result into a set of
 *	  (item text, count int8) rows sorted by count descending.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_approx/topk.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "alohadb_approx.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/typcache.h"

PG_FUNCTION_INFO_V1(approx_topk_transfn);
PG_FUNCTION_INFO_V1(approx_topk_finalfn);
PG_FUNCTION_INFO_V1(approx_topk);

/*
 * TopKEntry - a single tracked item with its estimated count.
 */
typedef struct TopKEntry
{
	char	   *item;			/* text representation of the item */
	int64		count;			/* estimated frequency */
} TopKEntry;

/*
 * TopKState - aggregate transition state.
 *
 * We maintain entries in an array and keep track of the current count.
 * When we need to find the minimum, we do a linear scan (k is typically
 * small, so this is fine).
 */
typedef struct TopKState
{
	int			k;				/* maximum number of entries */
	int			nentries;		/* current number of entries */
	Oid			inputType;		/* OID of the input type */
	FmgrInfo	outfn;			/* output function for converting input to text */
	bool		outfn_init;		/* whether outfn has been initialized */
	TopKEntry  *entries;		/* array of k entries */
} TopKState;

/*
 * Context for the multi-call SRF approx_topk.
 */
typedef struct TopKSRFContext
{
	int			nentries;
	int			current;
	TopKEntry  *entries;		/* sorted copy */
} TopKSRFContext;

/*
 * Convert a Datum of the input type to a palloc'd C string.
 */
static char *
topk_datum_to_cstring(TopKState *state, Datum val, Oid inputType)
{
	Oid			typoutput;
	bool		typIsVarlena;

	if (!state->outfn_init)
	{
		getTypeOutputInfo(inputType, &typoutput, &typIsVarlena);
		fmgr_info(typoutput, &state->outfn);
		state->outfn_init = true;
	}

	return OutputFunctionCall(&state->outfn, val);
}

/*
 * Find the index of the entry with the minimum count.
 * Returns -1 if the array is empty (should not happen in practice).
 */
static int
topk_find_min(TopKState *state)
{
	int			min_idx = 0;
	int64		min_count;
	int			i;

	if (state->nentries == 0)
		return -1;

	min_count = state->entries[0].count;
	for (i = 1; i < state->nentries; i++)
	{
		if (state->entries[i].count < min_count)
		{
			min_count = state->entries[i].count;
			min_idx = i;
		}
	}
	return min_idx;
}

/*
 * Find the index of an existing item, or -1 if not found.
 */
static int
topk_find_item(TopKState *state, const char *item)
{
	int			i;

	for (i = 0; i < state->nentries; i++)
	{
		if (strcmp(state->entries[i].item, item) == 0)
			return i;
	}
	return -1;
}

/*
 * Comparison function for qsort: sort entries by count descending.
 */
static int
topk_entry_cmp(const void *a, const void *b)
{
	const TopKEntry *ea = (const TopKEntry *) a;
	const TopKEntry *eb = (const TopKEntry *) b;

	if (ea->count > eb->count)
		return -1;
	if (ea->count < eb->count)
		return 1;
	return strcmp(ea->item, eb->item);
}

/*
 * approx_topk_transfn - transition function for the top-K aggregate.
 *
 * Arguments: (internal state, anyelement input, int k)
 *
 * Implements the Space-Saving algorithm:
 *   - If the item already exists in our tracked set, increment its count.
 *   - If the tracked set is not full, add the item with count 1.
 *   - Otherwise, replace the minimum-count item with the new item and
 *     set its count to (old minimum count + 1).
 */
Datum
approx_topk_transfn(PG_FUNCTION_ARGS)
{
	MemoryContext aggctx;
	MemoryContext oldctx;
	TopKState  *state;

	if (!AggCheckCallContext(fcinfo, &aggctx))
		elog(ERROR, "approx_topk_transfn called in non-aggregate context");

	/* Skip NULL inputs */
	if (PG_ARGISNULL(1))
	{
		if (PG_ARGISNULL(0))
			PG_RETURN_NULL();
		PG_RETURN_POINTER(PG_GETARG_POINTER(0));
	}

	/* k parameter must not be NULL */
	if (PG_ARGISNULL(2))
		elog(ERROR, "k parameter must not be NULL");

	if (PG_ARGISNULL(0))
	{
		int32		k = PG_GETARG_INT32(2);

		if (k <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("k must be a positive integer")));

		/* First call: allocate and initialize state in aggregate context */
		oldctx = MemoryContextSwitchTo(aggctx);

		state = (TopKState *) palloc0(sizeof(TopKState));
		state->k = k;
		state->nentries = 0;
		state->inputType = get_fn_expr_argtype(fcinfo->flinfo, 1);
		if (!OidIsValid(state->inputType))
			elog(ERROR, "could not determine input data type");
		state->outfn_init = false;
		state->entries = (TopKEntry *) palloc0(sizeof(TopKEntry) * k);

		MemoryContextSwitchTo(oldctx);
	}
	else
	{
		state = (TopKState *) PG_GETARG_POINTER(0);
	}

	/* Convert the input datum to a text representation for comparison */
	{
		Datum		val = PG_GETARG_DATUM(1);
		char	   *item_str;
		int			idx;

		oldctx = MemoryContextSwitchTo(aggctx);
		item_str = topk_datum_to_cstring(state, val, state->inputType);

		idx = topk_find_item(state, item_str);

		if (idx >= 0)
		{
			/* Item already tracked: increment its count */
			state->entries[idx].count++;
			pfree(item_str);
		}
		else if (state->nentries < state->k)
		{
			/* Room available: add new item */
			state->entries[state->nentries].item = item_str;
			state->entries[state->nentries].count = 1;
			state->nentries++;
		}
		else
		{
			/* Set is full: replace the minimum-count item */
			int			min_idx = topk_find_min(state);

			pfree(state->entries[min_idx].item);
			state->entries[min_idx].item = item_str;
			state->entries[min_idx].count++;
		}

		MemoryContextSwitchTo(oldctx);
	}

	PG_RETURN_POINTER(state);
}

/*
 * approx_topk_finalfn - final function for the top-K aggregate.
 *
 * Sorts the entries by count descending and returns the internal state
 * pointer.  The state is consumed by the approx_topk wrapper SRF.
 */
Datum
approx_topk_finalfn(PG_FUNCTION_ARGS)
{
	TopKState  *state;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (TopKState *) PG_GETARG_POINTER(0);

	/* Sort entries by count descending */
	if (state->nentries > 1)
		qsort(state->entries, state->nentries, sizeof(TopKEntry),
			  topk_entry_cmp);

	PG_RETURN_POINTER(state);
}

/*
 * approx_topk - SRF wrapper that returns the top-K result set.
 *
 * Takes the internal state from approx_topk_agg() and returns a set of
 * (item text, count int8) rows.
 *
 * Usage:
 *   SELECT item, count
 *   FROM approx_topk((SELECT approx_topk_agg(col, 10) FROM t));
 */
Datum
approx_topk(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	TopKSRFContext *srfctx;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldctx;
		TupleDesc	tupdesc;
		TopKState  *state;

		funcctx = SRF_FIRSTCALL_INIT();
		oldctx = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Build tuple descriptor for (item text, count int8) */
		tupdesc = CreateTemplateTupleDesc(2);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "item",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "count",
						   INT8OID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		/* Set up the SRF context */
		srfctx = (TopKSRFContext *) palloc0(sizeof(TopKSRFContext));

		if (!PG_ARGISNULL(0))
		{
			int			i;

			state = (TopKState *) PG_GETARG_POINTER(0);
			srfctx->nentries = state->nentries;
			srfctx->current = 0;

			/* Copy entries to multi_call_memory_ctx */
			srfctx->entries = (TopKEntry *) palloc(sizeof(TopKEntry) * state->nentries);
			for (i = 0; i < state->nentries; i++)
			{
				srfctx->entries[i].item = pstrdup(state->entries[i].item);
				srfctx->entries[i].count = state->entries[i].count;
			}
		}
		else
		{
			srfctx->nentries = 0;
			srfctx->current = 0;
			srfctx->entries = NULL;
		}

		funcctx->user_fctx = srfctx;
		MemoryContextSwitchTo(oldctx);
	}

	funcctx = SRF_PERCALL_SETUP();
	srfctx = (TopKSRFContext *) funcctx->user_fctx;

	if (srfctx->current < srfctx->nentries)
	{
		Datum		values[2];
		bool		nulls[2] = {false, false};
		HeapTuple	tuple;
		Datum		result;

		values[0] = CStringGetTextDatum(srfctx->entries[srfctx->current].item);
		values[1] = Int64GetDatum(srfctx->entries[srfctx->current].count);

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		srfctx->current++;

		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);
}
