/*-------------------------------------------------------------------------
 *
 * pg_ftsearch.c
 *	  Main entry point for the pg_ftsearch extension.
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
 *	  contrib/pg_ftsearch/pg_ftsearch.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"

#include "pg_ftsearch.h"

PG_MODULE_MAGIC_EXT(
					.name = "pg_ftsearch",
					.version = "1.0"
);
