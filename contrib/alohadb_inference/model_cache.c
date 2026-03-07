/*-------------------------------------------------------------------------
 *
 * model_cache.c
 *	  In-memory LRU cache for ONNX model sessions.
 *
 *	  The cache is allocated in TopMemoryContext so it persists across
 *	  transactions.  Access is serialized with a PG spinlock to provide
 *	  thread-safety for parallel query workers.
 *
 *	  Eviction policy: when the cache is full and a new model must be
 *	  loaded, the entry with the lowest lru_counter is evicted.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_inference/model_cache.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <limits.h>

#include "miscadmin.h"
#include "storage/spin.h"
#include "utils/memutils.h"

#include "alohadb_inference.h"

/* The singleton model cache, allocated in TopMemoryContext. */
static ModelCache *cache = NULL;

/*
 * model_cache_init
 *	  Allocate and initialize the global model cache.
 *
 *	  Called once from _PG_init().  Safe to call again if the cache
 *	  has already been initialized (it will be a no-op).
 */
void
model_cache_init(int max_entries)
{
	MemoryContext oldctx;

	if (cache != NULL)
		return;

	if (max_entries <= 0)
		max_entries = DEFAULT_MAX_MODELS;

	oldctx = MemoryContextSwitchTo(TopMemoryContext);

	cache = (ModelCache *) palloc0(sizeof(ModelCache));
	cache->max_entries = max_entries;
	cache->num_entries = 0;
	cache->lru_clock = 0;
	SpinLockInit(&cache->lock);

	cache->entries = (ModelCacheEntry *) palloc0(
		sizeof(ModelCacheEntry) * max_entries);

	MemoryContextSwitchTo(oldctx);
}

/*
 * model_cache_find
 *	  Look up a model by name.
 *
 *	  Returns a pointer to the cache entry if found, or NULL.
 *	  Does NOT update the LRU counter; call model_cache_touch()
 *	  separately after a successful lookup if the model is being used.
 */
ModelCacheEntry *
model_cache_find(const char *name)
{
	int		i;

	if (cache == NULL)
		return NULL;

	SpinLockAcquire(&cache->lock);

	for (i = 0; i < cache->max_entries; i++)
	{
		if (cache->entries[i].in_use &&
			strcmp(cache->entries[i].name, name) == 0)
		{
			ModelCacheEntry *entry = &cache->entries[i];

			SpinLockRelease(&cache->lock);
			return entry;
		}
	}

	SpinLockRelease(&cache->lock);
	return NULL;
}

/*
 * model_cache_evict_lru
 *	  Find and free the least-recently-used cache entry.
 *
 *	  Caller must hold the cache spinlock.
 *	  Returns a pointer to the now-free slot, or NULL if the cache
 *	  is empty (should not happen when called from model_cache_allocate).
 */
static ModelCacheEntry *
model_cache_evict_lru(void)
{
	int		min_lru = INT_MAX;
	int		victim_idx = -1;
	int		i;

	for (i = 0; i < cache->max_entries; i++)
	{
		if (cache->entries[i].in_use &&
			cache->entries[i].lru_counter < min_lru)
		{
			min_lru = cache->entries[i].lru_counter;
			victim_idx = i;
		}
	}

	if (victim_idx < 0)
		return NULL;

	/* Release ONNX Runtime resources if available */
#ifdef HAVE_ONNXRUNTIME
	{
		ModelCacheEntry *victim = &cache->entries[victim_idx];
		const OrtApi  *api = OrtGetApiBase()->GetApi(ORT_API_VERSION);

		if (victim->session != NULL)
			api->ReleaseSession(victim->session);
		if (victim->session_options != NULL)
			api->ReleaseSessionOptions(victim->session_options);
		if (victim->input_name != NULL)
			pfree(victim->input_name);
		if (victim->output_name != NULL)
			pfree(victim->output_name);
		if (victim->input_dims != NULL)
			pfree(victim->input_dims);
		if (victim->output_dims != NULL)
			pfree(victim->output_dims);
	}
#endif

	/* Free shape strings */
	if (cache->entries[victim_idx].input_shape != NULL)
		pfree(cache->entries[victim_idx].input_shape);
	if (cache->entries[victim_idx].output_shape != NULL)
		pfree(cache->entries[victim_idx].output_shape);

	/* Clear the slot */
	memset(&cache->entries[victim_idx], 0, sizeof(ModelCacheEntry));
	cache->num_entries--;

	elog(DEBUG1, "alohadb_inference: evicted model from cache slot %d",
		 victim_idx);

	return &cache->entries[victim_idx];
}

/*
 * model_cache_allocate
 *	  Reserve a cache slot for a new model.
 *
 *	  If the cache is full, the LRU entry is evicted first.
 *	  The returned entry has its name set and in_use = true, but
 *	  all other fields are zeroed.  The caller is responsible for
 *	  populating the ONNX session and shape metadata.
 */
ModelCacheEntry *
model_cache_allocate(const char *name)
{
	ModelCacheEntry *entry = NULL;
	MemoryContext	 oldctx;
	int				 i;

	if (cache == NULL)
		elog(ERROR, "alohadb_inference: model cache not initialized");

	if (strlen(name) >= MODEL_NAME_MAXLEN)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("model name too long"),
				 errdetail("Maximum model name length is %d characters.",
						   MODEL_NAME_MAXLEN - 1)));

	oldctx = MemoryContextSwitchTo(TopMemoryContext);
	SpinLockAcquire(&cache->lock);

	/* Check if a model with this name already exists */
	for (i = 0; i < cache->max_entries; i++)
	{
		if (cache->entries[i].in_use &&
			strcmp(cache->entries[i].name, name) == 0)
		{
			SpinLockRelease(&cache->lock);
			MemoryContextSwitchTo(oldctx);
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("model \"%s\" is already loaded", name)));
		}
	}

	/* Find an empty slot */
	for (i = 0; i < cache->max_entries; i++)
	{
		if (!cache->entries[i].in_use)
		{
			entry = &cache->entries[i];
			break;
		}
	}

	/* If no empty slot, evict the LRU entry */
	if (entry == NULL)
	{
		entry = model_cache_evict_lru();
		if (entry == NULL)
		{
			SpinLockRelease(&cache->lock);
			MemoryContextSwitchTo(oldctx);
			elog(ERROR, "alohadb_inference: could not evict model from cache");
		}
	}

	/* Initialize the new slot */
	memset(entry, 0, sizeof(ModelCacheEntry));
	strlcpy(entry->name, name, MODEL_NAME_MAXLEN);
	entry->in_use = true;
	entry->loaded_at = GetCurrentTimestamp();
	entry->lru_counter = ++cache->lru_clock;
	cache->num_entries++;

	SpinLockRelease(&cache->lock);
	MemoryContextSwitchTo(oldctx);

	return entry;
}

/*
 * model_cache_remove
 *	  Remove a named model from the cache.
 *
 *	  Frees all ONNX Runtime resources and shape metadata associated
 *	  with the entry.  Raises ERROR if the model is not found.
 */
void
model_cache_remove(const char *name)
{
	int		i;
	bool	found = false;

	if (cache == NULL)
		elog(ERROR, "alohadb_inference: model cache not initialized");

	SpinLockAcquire(&cache->lock);

	for (i = 0; i < cache->max_entries; i++)
	{
		if (cache->entries[i].in_use &&
			strcmp(cache->entries[i].name, name) == 0)
		{
			ModelCacheEntry *entry = &cache->entries[i];

#ifdef HAVE_ONNXRUNTIME
			{
				const OrtApi *api = OrtGetApiBase()->GetApi(ORT_API_VERSION);

				if (entry->session != NULL)
					api->ReleaseSession(entry->session);
				if (entry->session_options != NULL)
					api->ReleaseSessionOptions(entry->session_options);
				if (entry->input_name != NULL)
					pfree(entry->input_name);
				if (entry->output_name != NULL)
					pfree(entry->output_name);
				if (entry->input_dims != NULL)
					pfree(entry->input_dims);
				if (entry->output_dims != NULL)
					pfree(entry->output_dims);
			}
#endif

			if (entry->input_shape != NULL)
				pfree(entry->input_shape);
			if (entry->output_shape != NULL)
				pfree(entry->output_shape);

			memset(entry, 0, sizeof(ModelCacheEntry));
			cache->num_entries--;
			found = true;
			break;
		}
	}

	SpinLockRelease(&cache->lock);

	if (!found)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("model \"%s\" is not loaded", name)));
}

/*
 * model_cache_touch
 *	  Update the LRU counter for a cache entry.
 *
 *	  Should be called each time a model is used for inference.
 */
void
model_cache_touch(ModelCacheEntry *entry)
{
	if (cache == NULL || entry == NULL)
		return;

	SpinLockAcquire(&cache->lock);
	entry->lru_counter = ++cache->lru_clock;
	SpinLockRelease(&cache->lock);
}

/*
 * model_cache_count
 *	  Return the number of currently loaded models.
 */
int
model_cache_count(void)
{
	int		count;

	if (cache == NULL)
		return 0;

	SpinLockAcquire(&cache->lock);
	count = cache->num_entries;
	SpinLockRelease(&cache->lock);

	return count;
}

/*
 * model_cache_get_entries
 *	  Return a pointer to the entries array and the max entry count.
 *
 *	  Used by the alohadb_list_models() SRF to iterate over all slots.
 *	  The caller must check each entry's in_use flag.
 */
ModelCacheEntry *
model_cache_get_entries(int *count)
{
	if (cache == NULL)
	{
		*count = 0;
		return NULL;
	}

	*count = cache->max_entries;
	return cache->entries;
}
