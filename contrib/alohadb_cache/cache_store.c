/*-------------------------------------------------------------------------
 *
 * cache_store.c
 *	  Cache operations for the alohadb_cache extension.
 *
 *	  Implements the SQL-callable functions: cache_set, cache_get,
 *	  cache_delete, cache_flush, and cache_stats.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_cache/cache_store.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/timestamp.h"

#include "alohadb_cache.h"

/* SQL-callable function declarations -- must be in same file as implementations */
PG_FUNCTION_INFO_V1(cache_set);
PG_FUNCTION_INFO_V1(cache_get);
PG_FUNCTION_INFO_V1(cache_delete);
PG_FUNCTION_INFO_V1(cache_flush);
PG_FUNCTION_INFO_V1(cache_stats);

/* ----------------------------------------------------------------
 * Internal LRU helpers
 *
 * All of these assume the caller holds CacheCtl->lock in exclusive mode.
 * ---------------------------------------------------------------- */

/*
 * lru_remove -- unlink entry at slot_index from the LRU list.
 */
static void
lru_remove(int slot_index)
{
	CacheEntry *entry = &CacheEntries[slot_index];
	int			prev = entry->prev;
	int			next = entry->next;

	if (prev != CACHE_INVALID_INDEX)
		CacheEntries[prev].next = next;
	else
		CacheCtl->lru_head = next;

	if (next != CACHE_INVALID_INDEX)
		CacheEntries[next].prev = prev;
	else
		CacheCtl->lru_tail = prev;

	entry->prev = CACHE_INVALID_INDEX;
	entry->next = CACHE_INVALID_INDEX;
}

/*
 * lru_push_head -- insert entry at slot_index at the head (most recent).
 */
static void
lru_push_head(int slot_index)
{
	CacheEntry *entry = &CacheEntries[slot_index];

	entry->prev = CACHE_INVALID_INDEX;
	entry->next = CacheCtl->lru_head;

	if (CacheCtl->lru_head != CACHE_INVALID_INDEX)
		CacheEntries[CacheCtl->lru_head].prev = slot_index;
	else
		CacheCtl->lru_tail = slot_index;	/* list was empty */

	CacheCtl->lru_head = slot_index;
}

/*
 * lru_move_to_head -- move an existing entry to the head of the LRU list.
 */
static void
lru_move_to_head(int slot_index)
{
	/* Already at head? */
	if (CacheCtl->lru_head == slot_index)
		return;

	lru_remove(slot_index);
	lru_push_head(slot_index);
}

/*
 * find_free_slot -- find an unused slot in the entry array.
 * Returns the index, or CACHE_INVALID_INDEX if none available.
 */
static int
find_free_slot(void)
{
	int		i;

	for (i = 0; i < CacheCtl->max_entries; i++)
	{
		if (!CacheEntries[i].in_use)
			return i;
	}

	return CACHE_INVALID_INDEX;
}

/*
 * evict_lru_tail -- evict the least recently used entry to free a slot.
 * Returns the freed slot index, or CACHE_INVALID_INDEX on failure.
 */
static int
evict_lru_tail(void)
{
	int				tail;
	CacheEntry	   *entry;
	CacheHashEntry *hash_entry;

	tail = CacheCtl->lru_tail;
	if (tail == CACHE_INVALID_INDEX)
		return CACHE_INVALID_INDEX;

	entry = &CacheEntries[tail];

	/* Remove from hash table */
	hash_entry = (CacheHashEntry *) hash_search(CacheHash,
												entry->key,
												HASH_REMOVE,
												NULL);
	if (hash_entry == NULL)
		elog(WARNING, "alohadb_cache: LRU tail entry not found in hash table");

	/* Remove from LRU list */
	lru_remove(tail);

	/* Mark slot as free */
	entry->in_use = false;
	entry->key[0] = '\0';
	entry->value[0] = '\0';
	entry->value_len = 0;

	CacheCtl->num_entries--;
	CacheCtl->evictions++;

	return tail;
}

/*
 * check_shmem_available -- verify shared memory was initialized.
 */
static void
check_shmem_available(void)
{
	if (CacheCtl == NULL || CacheEntries == NULL || CacheHash == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("alohadb_cache must be loaded via shared_preload_libraries")));
}

/* ----------------------------------------------------------------
 * cache_set(key text, value jsonb, ttl interval DEFAULT NULL)
 *		RETURNS void
 *
 * Store a key-value pair in the cache.  If the key already exists,
 * update it.  If the cache is full, evict the LRU tail.
 * ---------------------------------------------------------------- */
Datum
cache_set(PG_FUNCTION_ARGS)
{
	text	   *key_text;
	Jsonb	   *jsonb_val;
	char	   *key_cstr;
	char	   *value_cstr;
	int			value_len;
	TimestampTz now;
	TimestampTz expires_at = 0;
	CacheHashEntry *hash_entry;
	bool		found;
	int			slot_index;
	CacheEntry *entry;

	check_shmem_available();

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("cache key must not be NULL")));

	if (PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("cache value must not be NULL")));

	key_text = PG_GETARG_TEXT_PP(0);
	jsonb_val = PG_GETARG_JSONB_P(1);

	/* Convert key to C string */
	key_cstr = text_to_cstring(key_text);
	if (strlen(key_cstr) >= CACHE_MAX_KEY_LEN)
		ereport(ERROR,
				(errcode(ERRCODE_STRING_DATA_LENGTH_MISMATCH),
				 errmsg("cache key too long"),
				 errdetail("Maximum key length is %d bytes.",
						   CACHE_MAX_KEY_LEN - 1)));

	/* Convert jsonb value to text representation */
	value_cstr = DatumGetCString(DirectFunctionCall1(jsonb_out,
													 JsonbPGetDatum(jsonb_val)));
	value_len = strlen(value_cstr);

	if (value_len >= CACHE_MAX_VALUE_LEN)
		ereport(ERROR,
				(errcode(ERRCODE_STRING_DATA_LENGTH_MISMATCH),
				 errmsg("cache value too large"),
				 errdetail("Maximum value size is %d bytes, got %d bytes.",
						   CACHE_MAX_VALUE_LEN - 1, value_len)));

	now = GetCurrentTimestamp();

	/* Compute expiry from optional TTL interval argument */
	if (!PG_ARGISNULL(2))
	{
		Interval   *ttl = PG_GETARG_INTERVAL_P(2);

		expires_at = DatumGetTimestampTz(
			DirectFunctionCall2(timestamptz_pl_interval,
								TimestampTzGetDatum(now),
								IntervalPGetDatum(ttl)));
	}

	LWLockAcquire(CacheCtl->lock, LW_EXCLUSIVE);

	/* Check if key already exists */
	hash_entry = (CacheHashEntry *) hash_search(CacheHash,
												key_cstr,
												HASH_FIND,
												NULL);

	if (hash_entry != NULL)
	{
		/* Update existing entry */
		slot_index = hash_entry->slot_index;
		entry = &CacheEntries[slot_index];

		memcpy(entry->value, value_cstr, value_len + 1);
		entry->value_len = value_len;
		entry->created_at = now;
		entry->expires_at = expires_at;

		/* Move to LRU head */
		lru_move_to_head(slot_index);
	}
	else
	{
		/* Find a free slot */
		slot_index = find_free_slot();
		if (slot_index == CACHE_INVALID_INDEX)
		{
			/* Cache full, evict LRU tail */
			slot_index = evict_lru_tail();
			if (slot_index == CACHE_INVALID_INDEX)
			{
				LWLockRelease(CacheCtl->lock);
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("alohadb_cache: unable to evict entry for new insertion")));
			}
		}

		/* Insert into hash table */
		hash_entry = (CacheHashEntry *) hash_search(CacheHash,
													key_cstr,
													HASH_ENTER,
													&found);
		if (hash_entry == NULL)
		{
			LWLockRelease(CacheCtl->lock);
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("alohadb_cache: out of shared memory for hash table")));
		}

		hash_entry->slot_index = slot_index;

		/* Populate the entry */
		entry = &CacheEntries[slot_index];
		strlcpy(entry->key, key_cstr, CACHE_MAX_KEY_LEN);
		memcpy(entry->value, value_cstr, value_len + 1);
		entry->value_len = value_len;
		entry->created_at = now;
		entry->expires_at = expires_at;
		entry->in_use = true;

		/* Add to LRU head */
		lru_push_head(slot_index);

		CacheCtl->num_entries++;
	}

	LWLockRelease(CacheCtl->lock);

	pfree(key_cstr);
	pfree(value_cstr);

	PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * cache_get(key text) RETURNS jsonb
 *
 * Look up a key in the cache.  Returns NULL if not found or expired.
 * On hit, moves the entry to the LRU head.
 * ---------------------------------------------------------------- */
Datum
cache_get(PG_FUNCTION_ARGS)
{
	text		   *key_text;
	char		   *key_cstr;
	CacheHashEntry *hash_entry;
	CacheEntry	   *entry;
	TimestampTz		now;
	char			value_buf[CACHE_MAX_VALUE_LEN];
	Datum			result;

	check_shmem_available();

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("cache key must not be NULL")));

	key_text = PG_GETARG_TEXT_PP(0);
	key_cstr = text_to_cstring(key_text);

	if (strlen(key_cstr) >= CACHE_MAX_KEY_LEN)
	{
		pfree(key_cstr);
		PG_RETURN_NULL();
	}

	now = GetCurrentTimestamp();

	/*
	 * We acquire an exclusive lock because we may need to modify the LRU
	 * list on hit, or perform lazy eviction on expiry.
	 */
	LWLockAcquire(CacheCtl->lock, LW_EXCLUSIVE);

	hash_entry = (CacheHashEntry *) hash_search(CacheHash,
												key_cstr,
												HASH_FIND,
												NULL);

	if (hash_entry == NULL)
	{
		/* Miss */
		CacheCtl->misses++;
		LWLockRelease(CacheCtl->lock);
		pfree(key_cstr);
		PG_RETURN_NULL();
	}

	entry = &CacheEntries[hash_entry->slot_index];

	/* Check expiry (lazy eviction) */
	if (entry->expires_at != 0 && entry->expires_at <= now)
	{
		int		slot_index = hash_entry->slot_index;

		/* Remove from hash */
		hash_search(CacheHash, key_cstr, HASH_REMOVE, NULL);

		/* Remove from LRU list */
		lru_remove(slot_index);

		/* Mark slot free */
		entry->in_use = false;
		entry->key[0] = '\0';
		entry->value[0] = '\0';
		entry->value_len = 0;
		CacheCtl->num_entries--;

		CacheCtl->misses++;
		LWLockRelease(CacheCtl->lock);
		pfree(key_cstr);
		PG_RETURN_NULL();
	}

	/* Hit -- copy value out while holding lock */
	memcpy(value_buf, entry->value, entry->value_len + 1);

	/* Move to LRU head */
	lru_move_to_head(hash_entry->slot_index);

	CacheCtl->hits++;
	LWLockRelease(CacheCtl->lock);
	pfree(key_cstr);

	/* Convert text representation back to jsonb */
	result = DirectFunctionCall1(jsonb_in,
								CStringGetDatum(value_buf));

	PG_RETURN_DATUM(result);
}

/* ----------------------------------------------------------------
 * cache_delete(key text) RETURNS boolean
 *
 * Remove an entry from the cache.  Returns true if the key existed.
 * ---------------------------------------------------------------- */
Datum
cache_delete(PG_FUNCTION_ARGS)
{
	text		   *key_text;
	char		   *key_cstr;
	CacheHashEntry *hash_entry;
	CacheEntry	   *entry;
	int				slot_index;

	check_shmem_available();

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("cache key must not be NULL")));

	key_text = PG_GETARG_TEXT_PP(0);
	key_cstr = text_to_cstring(key_text);

	if (strlen(key_cstr) >= CACHE_MAX_KEY_LEN)
	{
		pfree(key_cstr);
		PG_RETURN_BOOL(false);
	}

	LWLockAcquire(CacheCtl->lock, LW_EXCLUSIVE);

	hash_entry = (CacheHashEntry *) hash_search(CacheHash,
												key_cstr,
												HASH_FIND,
												NULL);

	if (hash_entry == NULL)
	{
		LWLockRelease(CacheCtl->lock);
		pfree(key_cstr);
		PG_RETURN_BOOL(false);
	}

	slot_index = hash_entry->slot_index;
	entry = &CacheEntries[slot_index];

	/* Remove from hash table */
	hash_search(CacheHash, key_cstr, HASH_REMOVE, NULL);

	/* Remove from LRU list */
	lru_remove(slot_index);

	/* Mark slot as free */
	entry->in_use = false;
	entry->key[0] = '\0';
	entry->value[0] = '\0';
	entry->value_len = 0;

	CacheCtl->num_entries--;

	LWLockRelease(CacheCtl->lock);
	pfree(key_cstr);

	PG_RETURN_BOOL(true);
}

/* ----------------------------------------------------------------
 * cache_flush() RETURNS bigint
 *
 * Remove all entries from the cache.  Returns the number of entries
 * that were flushed.
 * ---------------------------------------------------------------- */
Datum
cache_flush(PG_FUNCTION_ARGS)
{
	int64		flushed;
	int			i;
	HASH_SEQ_STATUS seq;
	CacheHashEntry *hash_entry;

	check_shmem_available();

	LWLockAcquire(CacheCtl->lock, LW_EXCLUSIVE);

	flushed = CacheCtl->num_entries;

	/* Remove all entries from the hash table */
	hash_seq_init(&seq, CacheHash);
	while ((hash_entry = (CacheHashEntry *) hash_seq_search(&seq)) != NULL)
	{
		hash_search(CacheHash, hash_entry->key, HASH_REMOVE, NULL);
	}

	/* Reset all entry slots */
	for (i = 0; i < CacheCtl->max_entries; i++)
	{
		CacheEntries[i].in_use = false;
		CacheEntries[i].key[0] = '\0';
		CacheEntries[i].value[0] = '\0';
		CacheEntries[i].value_len = 0;
		CacheEntries[i].created_at = 0;
		CacheEntries[i].expires_at = 0;
		CacheEntries[i].prev = CACHE_INVALID_INDEX;
		CacheEntries[i].next = CACHE_INVALID_INDEX;
	}

	/* Reset control structure */
	CacheCtl->num_entries = 0;
	CacheCtl->lru_head = CACHE_INVALID_INDEX;
	CacheCtl->lru_tail = CACHE_INVALID_INDEX;

	LWLockRelease(CacheCtl->lock);

	PG_RETURN_INT64(flushed);
}

/* ----------------------------------------------------------------
 * cache_stats() RETURNS TABLE(entries int, max_entries int,
 *                              hits bigint, misses bigint,
 *                              evictions bigint)
 *
 * Return cache statistics as a single-row set-returning function.
 * ---------------------------------------------------------------- */
Datum
cache_stats(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Datum		values[5];
	bool		nulls[5];

	check_shmem_available();

	InitMaterializedSRF(fcinfo, 0);

	memset(nulls, 0, sizeof(nulls));

	LWLockAcquire(CacheCtl->lock, LW_SHARED);

	values[0] = Int32GetDatum(CacheCtl->num_entries);
	values[1] = Int32GetDatum(CacheCtl->max_entries);
	values[2] = Int64GetDatum(CacheCtl->hits);
	values[3] = Int64GetDatum(CacheCtl->misses);
	values[4] = Int64GetDatum(CacheCtl->evictions);

	LWLockRelease(CacheCtl->lock);

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);

	return (Datum) 0;
}
