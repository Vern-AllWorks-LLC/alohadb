/*-------------------------------------------------------------------------
 *
 * alohadb_query_store.h
 *		AlohaDB Query Store - shared memory query tracking
 *
 * Copyright (c) 2025, OpenCAN / AlohaDB
 *
 *-------------------------------------------------------------------------
 */
#ifndef ALOHADB_QUERY_STORE_H
#define ALOHADB_QUERY_STORE_H

#include "postgres.h"
#include "fmgr.h"
#include "storage/lwlock.h"
#include "utils/timestamp.h"

#define QS_MAX_QUERY_LEN	1024
#define QS_MAX_ENTRIES		5000

/*
 * Hash table entry for a tracked query.
 * The key is query_hash (uint32), stored at the start per HASH_BLOBS convention.
 */
typedef struct QsEntry
{
	uint32		query_hash;					/* hash key - must be first */
	char		query_text[QS_MAX_QUERY_LEN];
	int64		calls;
	double		total_time;					/* milliseconds */
	double		min_time;					/* milliseconds */
	double		max_time;					/* milliseconds */
	int64		rows;
	int64		shared_blks_hit;
	int64		shared_blks_read;
	TimestampTz first_seen;
	TimestampTz last_seen;
} QsEntry;

/*
 * Shared state for the query store module.
 */
typedef struct QsSharedState
{
	LWLock	   *lock;						/* protects hash table access */
} QsSharedState;

/* Functions in alohadb_query_store.c */
extern Datum query_store_entries(PG_FUNCTION_ARGS);
extern Datum query_store_reset(PG_FUNCTION_ARGS);
extern Datum query_store_stats(PG_FUNCTION_ARGS);

/* Functions in qs_index_advisor.c */
extern Datum index_advisor_recommend(PG_FUNCTION_ARGS);
extern Datum index_advisor_unused_indexes(PG_FUNCTION_ARGS);
extern Datum autovacuum_suggestions(PG_FUNCTION_ARGS);

#endif							/* ALOHADB_QUERY_STORE_H */
