/*-------------------------------------------------------------------------
 *
 * pg_approx.c
 *	  Main entry point for the pg_approx extension.
 *
 *	  Provides approximate query processing primitives:
 *	    - HyperLogLog approximate distinct counting
 *	    - Count-Min Sketch frequency estimation
 *	    - Space-Saving top-K
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_approx/pg_approx.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "pg_approx.h"

PG_MODULE_MAGIC_EXT(
					.name = "pg_approx",
					.version = "1.0"
);

/*
 * PG_FUNCTION_INFO_V1 declarations are in each implementation file
 * (hll_agg.c, cms.c, topk.c) alongside the function definitions.
 */
