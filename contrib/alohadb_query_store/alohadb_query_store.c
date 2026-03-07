/*-------------------------------------------------------------------------
 *
 * alohadb_query_store.c
 *		AlohaDB Query Store - tracks query execution statistics in shared
 *		memory using an ExecutorEnd_hook.
 *
 * Must be loaded via shared_preload_libraries.
 *
 * Copyright (c) 2025, OpenCAN / AlohaDB
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "alohadb_query_store.h"

#include "executor/executor.h"
#include "executor/instrument.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"
#include "utils/tuplestore.h"
#include "common/hashfn.h"

PG_MODULE_MAGIC_EXT(
	.name = "alohadb_query_store",
	.version = PG_VERSION
);

/* GUC variables (currently none, but structure is here for future use) */

/* Saved hook values */
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;

/* Shared state */
static QsSharedState *qs_state = NULL;
static HTAB *qs_hash = NULL;

/* Function declarations for hooks */
static void qs_shmem_request(void);
static void qs_shmem_startup(void);
static void qs_ExecutorEnd(QueryDesc *queryDesc);

/* PG_FUNCTION_INFO_V1 declarations - MUST be in same file as implementation */
PG_FUNCTION_INFO_V1(query_store_entries);
PG_FUNCTION_INFO_V1(query_store_reset);
PG_FUNCTION_INFO_V1(query_store_stats);

/*
 * Estimate shared memory space needed.
 */
static Size
qs_memsize(void)
{
	Size		size;

	size = MAXALIGN(sizeof(QsSharedState));
	size = add_size(size, hash_estimate_size(QS_MAX_ENTRIES, sizeof(QsEntry)));

	return size;
}

/*
 * Module load callback.
 * Register hooks only when loaded via shared_preload_libraries.
 */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	/* Install shmem_request_hook */
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = qs_shmem_request;

	/* Install shmem_startup_hook */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = qs_shmem_startup;

	/* Install ExecutorEnd_hook */
	prev_ExecutorEnd_hook = ExecutorEnd_hook;
	ExecutorEnd_hook = qs_ExecutorEnd;
}

/*
 * shmem_request hook: request shared memory space.
 */
static void
qs_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(qs_memsize());
	RequestNamedLWLockTranche("alohadb_query_store", 1);
}

/*
 * shmem_startup hook: allocate and initialize shared memory.
 */
static void
qs_shmem_startup(void)
{
	bool		found;
	HASHCTL		info;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/* Reset in case of restart within a process */
	qs_state = NULL;
	qs_hash = NULL;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	qs_state = ShmemInitStruct("alohadb_query_store",
							   sizeof(QsSharedState),
							   &found);

	if (!found)
	{
		/* First time: initialize the shared state */
		qs_state->lock = &(GetNamedLWLockTranche("alohadb_query_store"))->lock;
	}

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(uint32);
	info.entrysize = sizeof(QsEntry);

	qs_hash = ShmemInitHash("alohadb_query_store_hash",
							QS_MAX_ENTRIES,
							QS_MAX_ENTRIES,
							&info,
							HASH_ELEM | HASH_BLOBS);

	LWLockRelease(AddinShmemInitLock);
}

/*
 * ExecutorEnd hook: capture query execution statistics.
 */
static void
qs_ExecutorEnd(QueryDesc *queryDesc)
{
	double		elapsed_ms = 0.0;
	int64		rows = 0;
	uint32		query_hash;
	const char *query_text;
	int			query_len;
	QsEntry	   *entry;
	bool		found;
	TimestampTz	now;

	/*
	 * Only track if we have shared memory initialized and the query has
	 * source text and timing information.
	 */
	if (qs_hash && qs_state && queryDesc->sourceText)
	{
		/* Get timing info if available */
		if (queryDesc->totaltime != NULL)
		{
			InstrEndLoop(queryDesc->totaltime);
			elapsed_ms = queryDesc->totaltime->total * 1000.0;	/* seconds to ms */
		}

		/* Get row count from estate */
		rows = queryDesc->estate->es_processed;

		/* Compute hash of query text */
		query_text = queryDesc->sourceText;
		query_len = strlen(query_text);
		query_hash = DatumGetUInt32(hash_any((const unsigned char *) query_text,
											 query_len));

		now = GetCurrentTimestamp();

		LWLockAcquire(qs_state->lock, LW_EXCLUSIVE);

		entry = (QsEntry *) hash_search(qs_hash, &query_hash,
										HASH_ENTER_NULL, &found);

		if (entry != NULL)
		{
			if (!found)
			{
				/* New entry: initialize */
				strlcpy(entry->query_text, query_text, QS_MAX_QUERY_LEN);
				entry->calls = 1;
				entry->total_time = elapsed_ms;
				entry->min_time = elapsed_ms;
				entry->max_time = elapsed_ms;
				entry->rows = rows;
				entry->shared_blks_hit = 0;
				entry->shared_blks_read = 0;
				entry->first_seen = now;
				entry->last_seen = now;

				/* Capture buffer stats if available */
				if (queryDesc->totaltime != NULL)
				{
					entry->shared_blks_hit = queryDesc->totaltime->bufusage.shared_blks_hit;
					entry->shared_blks_read = queryDesc->totaltime->bufusage.shared_blks_read;
				}
			}
			else
			{
				/* Existing entry: update accumulators */
				entry->calls++;
				entry->total_time += elapsed_ms;
				if (elapsed_ms < entry->min_time)
					entry->min_time = elapsed_ms;
				if (elapsed_ms > entry->max_time)
					entry->max_time = elapsed_ms;
				entry->rows += rows;
				entry->last_seen = now;

				/* Accumulate buffer stats if available */
				if (queryDesc->totaltime != NULL)
				{
					entry->shared_blks_hit += queryDesc->totaltime->bufusage.shared_blks_hit;
					entry->shared_blks_read += queryDesc->totaltime->bufusage.shared_blks_read;
				}
			}
		}

		LWLockRelease(qs_state->lock);
	}

	/* Call previous hook or standard function */
	if (prev_ExecutorEnd_hook)
		prev_ExecutorEnd_hook(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

/*
 * query_store_entries()
 *
 * Returns a set of rows with all tracked query entries.
 * Columns: query_hash bigint, query_text text, calls bigint,
 *          total_time float8, mean_time float8, min_time float8,
 *          max_time float8, rows bigint, first_seen timestamptz,
 *          last_seen timestamptz
 */
Datum
query_store_entries(PG_FUNCTION_ARGS)
{
#define QS_ENTRIES_COLS 10
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	HASH_SEQ_STATUS hash_seq;
	QsEntry		   *entry;

	if (!qs_hash || !qs_state)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("alohadb_query_store must be loaded via shared_preload_libraries")));

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	LWLockAcquire(qs_state->lock, LW_SHARED);

	hash_seq_init(&hash_seq, qs_hash);
	while ((entry = (QsEntry *) hash_seq_search(&hash_seq)) != NULL)
	{
		Datum		values[QS_ENTRIES_COLS];
		bool		nulls[QS_ENTRIES_COLS];
		double		mean_time;

		memset(nulls, 0, sizeof(nulls));

		mean_time = (entry->calls > 0) ? (entry->total_time / entry->calls) : 0.0;

		values[0] = Int64GetDatum((int64) entry->query_hash);
		values[1] = CStringGetTextDatum(entry->query_text);
		values[2] = Int64GetDatum(entry->calls);
		values[3] = Float8GetDatum(entry->total_time);
		values[4] = Float8GetDatum(mean_time);
		values[5] = Float8GetDatum(entry->min_time);
		values[6] = Float8GetDatum(entry->max_time);
		values[7] = Int64GetDatum(entry->rows);
		values[8] = TimestampTzGetDatum(entry->first_seen);
		values[9] = TimestampTzGetDatum(entry->last_seen);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	LWLockRelease(qs_state->lock);

	return (Datum) 0;
}

/*
 * query_store_reset()
 *
 * Clears all entries from the query store hash table.
 */
Datum
query_store_reset(PG_FUNCTION_ARGS)
{
	HASH_SEQ_STATUS hash_seq;
	QsEntry		   *entry;

	if (!qs_hash || !qs_state)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("alohadb_query_store must be loaded via shared_preload_libraries")));

	LWLockAcquire(qs_state->lock, LW_EXCLUSIVE);

	hash_seq_init(&hash_seq, qs_hash);
	while ((entry = (QsEntry *) hash_seq_search(&hash_seq)) != NULL)
	{
		hash_search(qs_hash, &entry->query_hash, HASH_REMOVE, NULL);
	}

	LWLockRelease(qs_state->lock);

	PG_RETURN_VOID();
}

/*
 * query_store_stats()
 *
 * Returns a single row with stats about the query store itself.
 * Columns: total_entries int, max_entries int
 */
Datum
query_store_stats(PG_FUNCTION_ARGS)
{
#define QS_STATS_COLS 2
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Datum			values[QS_STATS_COLS];
	bool			nulls[QS_STATS_COLS];
	long			num_entries;

	if (!qs_hash || !qs_state)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("alohadb_query_store must be loaded via shared_preload_libraries")));

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	memset(nulls, 0, sizeof(nulls));

	LWLockAcquire(qs_state->lock, LW_SHARED);
	num_entries = hash_get_num_entries(qs_hash);
	LWLockRelease(qs_state->lock);

	values[0] = Int32GetDatum((int32) num_entries);
	values[1] = Int32GetDatum((int32) QS_MAX_ENTRIES);

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
						 values, nulls);

	return (Datum) 0;
}
