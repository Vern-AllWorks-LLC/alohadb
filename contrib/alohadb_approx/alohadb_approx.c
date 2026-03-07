/*-------------------------------------------------------------------------
 *
 * alohadb_approx.c
 *	  Main entry point for the alohadb_approx extension.
 *
 *	  Provides approximate query processing primitives:
 *	    - HyperLogLog approximate distinct counting
 *	    - Count-Min Sketch frequency estimation
 *	    - Space-Saving top-K
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_approx/alohadb_approx.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "alohadb_approx.h"

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_approx",
					.version = "1.0"
);

/*
 * PG_FUNCTION_INFO_V1 declarations are in each implementation file
 * (hll_agg.c, cms.c, topk.c) alongside the function definitions.
 */
