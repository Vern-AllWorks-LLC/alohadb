/*-------------------------------------------------------------------------
 *
 * alohadb_cache.c
 *	  Main entry point for the alohadb_cache extension.
 *
 *	  Implements a shared-memory key-value cache with LRU eviction.
 *	  Values are JSONB stored as their text representation in fixed-size
 *	  shared-memory buffers.
 *
 *	  This extension must be loaded via shared_preload_libraries so that
 *	  it can allocate shared memory and register its LWLock tranche.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_cache/alohadb_cache.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"

#include "alohadb_cache.h"

PG_MODULE_MAGIC_EXT(
	.name = "alohadb_cache",
	.version = "1.0"
);

/* Global shared-memory pointers */
CacheControl *CacheCtl = NULL;
CacheEntry *CacheEntries = NULL;
HTAB *CacheHash = NULL;

/* GUC variable */
int	cache_max_entries = CACHE_DEFAULT_MAX_ENTRIES;

/* Saved hook values for chaining */
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* Forward declarations */
static void cache_shmem_request(void);
static void cache_shmem_startup(void);
static Size cache_shmem_size(void);

/* ----------------------------------------------------------------
 * cache_shmem_size
 *
 * Calculate total shared memory needed for the cache.
 * ---------------------------------------------------------------- */
static Size
cache_shmem_size(void)
{
	Size		size;

	/* CacheControl struct */
	size = MAXALIGN(sizeof(CacheControl));

	/* CacheEntry array */
	size = add_size(size,
					mul_size((Size) cache_max_entries, MAXALIGN(sizeof(CacheEntry))));

	return size;
}

/* ----------------------------------------------------------------
 * cache_shmem_request
 *
 * Request shared memory space and an LWLock tranche.
 * ---------------------------------------------------------------- */
static void
cache_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(cache_shmem_size());

	/*
	 * Also request space for the hash table.  ShmemInitHash handles its
	 * own space allocation via ShmemAlloc, so we need to account for it.
	 */
	RequestAddinShmemSpace(hash_estimate_size(cache_max_entries,
											  sizeof(CacheHashEntry)));

	RequestNamedLWLockTranche("alohadb_cache", 1);
}

/* ----------------------------------------------------------------
 * cache_shmem_startup
 *
 * Initialize shared memory structures for the cache.
 * ---------------------------------------------------------------- */
static void
cache_shmem_startup(void)
{
	bool		found;
	HASHCTL		info;
	int			i;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	/* Initialize the control structure */
	CacheCtl = ShmemInitStruct("alohadb_cache control",
							   MAXALIGN(sizeof(CacheControl)),
							   &found);

	if (!found)
	{
		CacheCtl->lock = &(GetNamedLWLockTranche("alohadb_cache"))->lock;
		CacheCtl->num_entries = 0;
		CacheCtl->max_entries = cache_max_entries;
		CacheCtl->hits = 0;
		CacheCtl->misses = 0;
		CacheCtl->evictions = 0;
		CacheCtl->lru_head = CACHE_INVALID_INDEX;
		CacheCtl->lru_tail = CACHE_INVALID_INDEX;
	}

	/* Initialize the entry array */
	CacheEntries = ShmemInitStruct("alohadb_cache entries",
								   mul_size((Size) cache_max_entries,
											MAXALIGN(sizeof(CacheEntry))),
								   &found);

	if (!found)
	{
		for (i = 0; i < cache_max_entries; i++)
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
	}

	/* Initialize the hash table mapping key -> slot index */
	memset(&info, 0, sizeof(info));
	info.keysize = CACHE_MAX_KEY_LEN;
	info.entrysize = sizeof(CacheHashEntry);

	CacheHash = ShmemInitHash("alohadb_cache hash",
							  cache_max_entries,
							  cache_max_entries,
							  &info,
							  HASH_ELEM | HASH_STRINGS);

	LWLockRelease(AddinShmemInitLock);
}

/* ----------------------------------------------------------------
 * _PG_init
 *
 * Module load callback.  Registers GUC variables and hooks for
 * shared memory allocation.
 * ---------------------------------------------------------------- */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	/* Define GUC: alohadb.cache_max_entries */
	DefineCustomIntVariable("alohadb.cache_max_entries",
							"Maximum number of entries in the shared memory cache.",
							"This parameter can only be set at server start.",
							&cache_max_entries,
							CACHE_DEFAULT_MAX_ENTRIES,
							16,			/* min */
							100000,		/* max */
							PGC_POSTMASTER,
							0,
							NULL, NULL, NULL);

	MarkGUCPrefixReserved("alohadb.cache");

	/* Install shared memory hooks */
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = cache_shmem_request;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = cache_shmem_startup;
}
