/*-------------------------------------------------------------------------
 *
 * alohadb_search.c
 *	  Main entry point for the alohadb_search extension.
 *
 *	  This file contains only the module magic block.  All SQL-callable
 *	  functions are implemented in their respective source files:
 *
 *	    search_bm25.c    - BM25 ranking
 *	    search_fuzzy.c   - Levenshtein distance, phonetic encoding
 *	    search_suggest.c - autocomplete, synonym expansion, text analysis
 *	    search_geo.c     - Haversine distance, nearby search
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_search/alohadb_search.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"

#include "alohadb_search.h"

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_search",
					.version = "1.0"
);
