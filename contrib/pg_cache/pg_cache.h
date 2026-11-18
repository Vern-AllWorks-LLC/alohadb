/*-------------------------------------------------------------------------
 *
 * pg_cache.h
 *	  Shared declarations for the pg_cache extension.
 *
 *	  A shared-memory key-value cache:
 *	  - keys are text (up to CACHE_MAX_KEY_LEN - 1 bytes), values are
 *		variable-length byte strings (text or jsonb text) kept in a
 *		fixed-size dynamic shared memory area (pg_cache.max_memory);
 *	  - eviction uses SIEVE (Zhang et al., NSDI '24): a FIFO queue plus one
 *		"visited" bit per entry, so cache hits never reorder the queue and
 *		only need a shared lock;
 *	  - writes made inside a transaction are applied when it commits and
 *		discarded if it aborts (pg_cache.transactional, default on).
 *
 *	  The extension must be loaded via shared_preload_libraries.
 *
 * Copyright (c) 2025-2026, AlohaDB
 *
 * contrib/pg_cache/pg_cache.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CACHE_H
#define PG_CACHE_H

#include "postgres.h"

#include "lib/ilist.h"
#include "port/atomics.h"
#include "storage/lwlock.h"
#include "utils/dsa.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"

/* Maximum key length in bytes (including NUL terminator) */
#define CACHE_MAX_KEY_LEN		256

/* Item flags */
#define CACHE_ITEM_JSON			0x01	/* value was stored as jsonb */

/*
 * CacheItem -- one cache entry; it is also the shared hash table entry, so
 * it lives in the main shared memory segment, which is mapped at the same
 * address in every backend (pointers and dlist links are therefore safe).
 */
typedef struct CacheItem
{
	char		key[CACHE_MAX_KEY_LEN]; /* hash key, NUL-terminated */
	dsa_pointer value;			/* value bytes in the cache DSA */
	uint32		value_len;
	uint8		flags;
	volatile bool visited;		/* SIEVE bit, set by readers */
	TimestampTz created_at;
	TimestampTz expires_at;		/* 0 = no expiry */
	dlist_node	fifo;			/* SIEVE queue: head = newest */
} CacheItem;

/*
 * CacheControl -- shared control structure, followed in shared memory by the
 * in-place DSA area that holds the values.
 */
typedef struct CacheControl
{
	LWLock	   *lock;
	int			dsa_tranche;
	int			max_entries;
	Size		max_memory;
	int			num_entries;
	uint64		value_bytes;	/* sum of value_len of live items */
	dlist_head	queue;			/* SIEVE FIFO queue */
	CacheItem  *hand;			/* SIEVE hand, NULL = start at tail */
	pg_atomic_uint64 hits;
	pg_atomic_uint64 misses;
	pg_atomic_uint64 evictions;
	pg_atomic_uint64 expired;
	pg_atomic_uint64 dropped;	/* commit-time writes that did not fit */
	char		dsa_space[FLEXIBLE_ARRAY_MEMBER];
} CacheControl;

/* GUC variables */
extern int	cache_max_entries;
extern int	cache_max_memory_mb;
extern bool cache_transactional;

/* Shared state, set up by pg_cache.c */
extern CacheControl *CacheCtl;
extern HTAB *CacheHash;

extern dsa_area *cache_dsa(void);
extern void cache_check_loaded(void);

/* Core operations (cache_store.c); caller holds the lock exclusively */
extern bool cache_store_put(const char *key, const char *value, uint32 len,
							uint8 flags, TimestampTz expires_at,
							bool throw_errors);
extern bool cache_store_delete(const char *key);
extern int64 cache_store_flush(void);
extern bool cache_store_expire(const char *key, TimestampTz expires_at);
extern Size cache_max_value_len(void);

/* Pending (transactional) writes (pg_cache.c) */
typedef enum CacheOpKind
{
	CACHE_OP_SET,
	CACHE_OP_DELETE,
	CACHE_OP_EXPIRE,
	CACHE_OP_FLUSH,
} CacheOpKind;

typedef struct CachePendingOp
{
	CacheOpKind kind;
	int			nest_level;
	char		key[CACHE_MAX_KEY_LEN];
	char	   *value;
	uint32		value_len;
	uint8		flags;
	TimestampTz expires_at;
} CachePendingOp;

extern bool cache_in_transaction(void);
extern void cache_pending_add(CacheOpKind kind, const char *key,
							  const char *value, uint32 len, uint8 flags,
							  TimestampTz expires_at);

/*
 * Lookup of this transaction's own pending writes.  Returns false if the
 * transaction has not written the key; otherwise *op is the newest pending
 * operation affecting it (NULL means "deleted by a pending flush").
 */
extern bool cache_pending_lookup(const char *key, CachePendingOp **op);

#endif							/* PG_CACHE_H */
