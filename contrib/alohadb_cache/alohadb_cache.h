/*-------------------------------------------------------------------------
 *
 * alohadb_cache.h
 *	  Shared declarations for the alohadb_cache extension.
 *
 *	  Provides a shared-memory key-value cache with LRU eviction.
 *	  Values are stored as text representations of JSONB.
 *	  The extension must be loaded via shared_preload_libraries.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * contrib/alohadb_cache/alohadb_cache.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef ALOHADB_CACHE_H
#define ALOHADB_CACHE_H

#include "postgres.h"
#include "fmgr.h"
#include "storage/lwlock.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"

/* Maximum key length in bytes (including NUL terminator) */
#define CACHE_MAX_KEY_LEN		256

/* Maximum value length in bytes (8KB) */
#define CACHE_MAX_VALUE_LEN		8192

/* Default maximum number of cache entries */
#define CACHE_DEFAULT_MAX_ENTRIES	1000

/* Sentinel value for LRU list terminators (no entry) */
#define CACHE_INVALID_INDEX		(-1)

/*
 * CacheEntry -- one slot in the fixed-size entry array.
 *
 * The LRU doubly-linked list is index-based (not pointer-based) because
 * shared memory may be mapped at different addresses in each backend.
 */
typedef struct CacheEntry
{
	char		key[CACHE_MAX_KEY_LEN];
	char		value[CACHE_MAX_VALUE_LEN];
	int			value_len;
	TimestampTz created_at;
	TimestampTz expires_at;		/* 0 = no expiry */
	bool		in_use;
	int			prev;			/* LRU prev (toward head / most recent) */
	int			next;			/* LRU next (toward tail / least recent) */
} CacheEntry;

/*
 * CacheHashEntry -- entry in the shared-memory hash table.
 * Maps key -> index into the CacheEntry array.
 */
typedef struct CacheHashEntry
{
	char		key[CACHE_MAX_KEY_LEN];		/* hash key */
	int			slot_index;					/* index into entry array */
} CacheHashEntry;

/*
 * CacheControl -- shared-memory control structure.
 */
typedef struct CacheControl
{
	LWLock	   *lock;
	int			num_entries;
	int			max_entries;
	int64		hits;
	int64		misses;
	int64		evictions;
	int			lru_head;		/* most recently used */
	int			lru_tail;		/* least recently used */
} CacheControl;

/* Global pointers (set during shmem startup) */
extern CacheControl *CacheCtl;
extern CacheEntry *CacheEntries;
extern HTAB *CacheHash;

/* GUC variable */
extern int	cache_max_entries;

/* SQL-callable functions (in cache_store.c) */
extern Datum cache_set(PG_FUNCTION_ARGS);
extern Datum cache_get(PG_FUNCTION_ARGS);
extern Datum cache_delete(PG_FUNCTION_ARGS);
extern Datum cache_flush(PG_FUNCTION_ARGS);
extern Datum cache_stats(PG_FUNCTION_ARGS);

#endif							/* ALOHADB_CACHE_H */
