/*-------------------------------------------------------------------------
 *
 * hll_agg.c
 *	  HyperLogLog approximate distinct counting aggregate.
 *
 *	  Wraps the internal HyperLogLog implementation from
 *	  src/backend/lib/hyperloglog.c to provide a SQL aggregate
 *	  approx_count_distinct(anyelement) that returns int8.
 *
 *	  Uses precision p=14 (16384 registers, ~0.8% standard error).
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_approx/hll_agg.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "common/hashfn.h"
#include "fmgr.h"
#include "lib/hyperloglog.h"
#include "alohadb_approx.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

PG_FUNCTION_INFO_V1(approx_count_distinct_transfn);
PG_FUNCTION_INFO_V1(approx_count_distinct_finalfn);

/*
 * Transition state for the approx_count_distinct aggregate.
 *
 * We store the hyperLogLogState and the cached hash function info
 * for the input type so that we only look it up once per group.
 */
typedef struct HllAggState
{
	hyperLogLogState hll;
	Oid			inputType;
	FmgrInfo	hashfn;
	bool		hashfn_init;
} HllAggState;

/*
 * approx_count_distinct_transfn - transition function
 *
 * Accumulates a hash of each non-NULL input value into the HLL state.
 */
Datum
approx_count_distinct_transfn(PG_FUNCTION_ARGS)
{
	MemoryContext aggctx;
	MemoryContext oldctx;
	HllAggState *state;

	if (!AggCheckCallContext(fcinfo, &aggctx))
		elog(ERROR, "approx_count_distinct_transfn called in non-aggregate context");

	/* Skip NULL inputs */
	if (PG_ARGISNULL(1))
	{
		if (PG_ARGISNULL(0))
			PG_RETURN_NULL();
		PG_RETURN_POINTER(PG_GETARG_POINTER(0));
	}

	if (PG_ARGISNULL(0))
	{
		/* First call: allocate and initialize state */
		oldctx = MemoryContextSwitchTo(aggctx);

		state = (HllAggState *) palloc0(sizeof(HllAggState));
		initHyperLogLog(&state->hll, HLL_BIT_WIDTH);
		state->inputType = get_fn_expr_argtype(fcinfo->flinfo, 1);
		if (!OidIsValid(state->inputType))
			elog(ERROR, "could not determine input data type");
		state->hashfn_init = false;

		MemoryContextSwitchTo(oldctx);
	}
	else
	{
		state = (HllAggState *) PG_GETARG_POINTER(0);
	}

	/* Look up the hash function for this type if we haven't already */
	if (!state->hashfn_init)
	{
		TypeCacheEntry *typentry;

		typentry = lookup_type_cache(state->inputType,
									 TYPECACHE_HASH_PROC_FINFO);
		if (!OidIsValid(typentry->hash_proc_finfo.fn_oid))
			elog(ERROR, "could not find hash function for type %u",
				 state->inputType);
		fmgr_info_copy(&state->hashfn, &typentry->hash_proc_finfo,
						CurrentMemoryContext);
		state->hashfn_init = true;
	}

	/* Hash the input value and add it to the HLL */
	{
		Datum		val = PG_GETARG_DATUM(1);
		uint32		hashval;

		hashval = DatumGetUInt32(FunctionCall1Coll(&state->hashfn,
												   PG_GET_COLLATION(),
												   val));
		addHyperLogLog(&state->hll, hashval);
	}

	PG_RETURN_POINTER(state);
}

/*
 * approx_count_distinct_finalfn - final function
 *
 * Returns the estimated cardinality from the HLL state as an int8.
 */
Datum
approx_count_distinct_finalfn(PG_FUNCTION_ARGS)
{
	HllAggState *state;
	double		estimate;

	/* If no rows were processed (all NULLs or empty set), return 0 */
	if (PG_ARGISNULL(0))
		PG_RETURN_INT64(0);

	state = (HllAggState *) PG_GETARG_POINTER(0);
	estimate = estimateHyperLogLog(&state->hll);

	PG_RETURN_INT64((int64) estimate);
}
