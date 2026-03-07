/*-------------------------------------------------------------------------
 *
 * alohadb_search.h
 *	  Shared declarations for the alohadb_search extension.
 *
 *	  Provides advanced full-text search capabilities including BM25
 *	  scoring, fuzzy matching via Levenshtein distance, Double Metaphone
 *	  phonetic encoding, autocomplete suggestions, synonym expansion,
 *	  and geographic proximity search via the Haversine formula.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * contrib/alohadb_search/alohadb_search.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef ALOHADB_SEARCH_H
#define ALOHADB_SEARCH_H

#include "postgres.h"
#include "fmgr.h"

/*
 * Earth's mean radius in meters, used for Haversine calculations.
 */
#define EARTH_RADIUS_METERS		6371000.0

/*
 * Default BM25 parameters.
 */
#define BM25_DEFAULT_K1			1.2
#define BM25_DEFAULT_B			0.75
#define BM25_DEFAULT_AVG_DOC_LEN	256.0
#define BM25_DEFAULT_TOTAL_DOCS	1000.0
#define BM25_DEFAULT_MATCHING_DOCS	10.0

/*
 * Maximum metaphone code length.
 */
#define METAPHONE_CODE_LEN		4

/*
 * Name of the synonyms table created by the extension SQL script.
 */
#define SEARCH_SYNONYMS_TABLE	"alohadb_search_synonyms"

/* --- search_bm25.c --- */
extern Datum search_bm25(PG_FUNCTION_ARGS);

/* --- search_fuzzy.c --- */
extern Datum search_edit_distance(PG_FUNCTION_ARGS);
extern Datum search_fuzzy_match(PG_FUNCTION_ARGS);
extern Datum search_phonetic(PG_FUNCTION_ARGS);

/* --- search_suggest.c --- */
extern Datum search_autocomplete(PG_FUNCTION_ARGS);
extern Datum search_expand_synonyms(PG_FUNCTION_ARGS);
extern Datum search_analyze(PG_FUNCTION_ARGS);

/* --- search_geo.c --- */
extern Datum search_haversine(PG_FUNCTION_ARGS);
extern Datum search_nearby(PG_FUNCTION_ARGS);

#endif							/* ALOHADB_SEARCH_H */
