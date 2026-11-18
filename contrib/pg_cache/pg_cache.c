/*-------------------------------------------------------------------------
 *
 * pg_cache.c
 *	  Shared memory set-up, GUCs and transactional write handling for the
 *	  pg_cache extension.
 *
 *	  Values live in a DSA area created in place inside the main shared
 *	  memory segment and capped at pg_cache.max_memory, so the cache can
 *	  never grow past its budget or compete with shared_buffers.
 *
 *	  When pg_cache.transactional is on (the default), cache_set,
 *	  cache_delete, cache_expire and cache_flush only queue their change in
 *	  backend-local memory.  The queue is applied to shared memory when the
 *	  transaction commits and dropped when it (or the subtransaction that
 *	  queued it) aborts, so a cache update and the table update it mirrors
 *	  become visible together or not at all.  The writing transaction sees
 *	  its own queued changes.
 *
 * Copyright (c) 2025-2026, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_cache/pg_cache.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/memutils.h"

#include "pg_cache.h"

PG_MODULE_MAGIC_EXT(
					.name = "pg_cache",
					.version = "2.0"
);

/* GUC variables */
int			cache_max_entries = 10000;
int			cache_max_memory_mb = 32;
bool		cache_transactional = true;

/* Shared state */
CacheControl *CacheCtl = NULL;
HTAB	   *CacheHash = NULL;

/* Backend-local attachment to the value area */
static dsa_area *cache_area = NULL;

static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* Pending writes of the current transaction (TopTransactionContext) */
static List *pending_ops = NIL;
static HTAB *pending_index = NULL;	/* key -> newest SET/DELETE op */
static bool pending_flush = false;	/* a FLUSH is queued */
static bool callbacks_registered = false;

typedef struct PendingIndexEntry
{
	char		key[CACHE_MAX_KEY_LEN];
	CachePendingOp *op;
} PendingIndexEntry;

void		_PG_init(void);

static Size
cache_memory_bytes(void)
{
	return (Size) cache_max_memory_mb * 1024 * 1024;
}

static Size
cache_shmem_size(void)
{
	Size		size;

	size = MAXALIGN(offsetof(CacheControl, dsa_space));
	size = add_size(size, cache_memory_bytes());
	size = add_size(size, hash_estimate_size(cache_max_entries,
											 sizeof(CacheItem)));
	return size;
}

static void
cache_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(cache_shmem_size());
	RequestNamedLWLockTranche("pg_cache", 1);
}

static void
cache_shmem_startup(void)
{
	bool		found;
	HASHCTL		info;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	CacheCtl = ShmemInitStruct("pg_cache control",
							   add_size(MAXALIGN(offsetof(CacheControl, dsa_space)),
										cache_memory_bytes()),
							   &found);
	if (!found)
	{
		dsa_area   *area;

		CacheCtl->lock = &(GetNamedLWLockTranche("pg_cache"))->lock;
		CacheCtl->dsa_tranche = LWLockNewTrancheId();
		CacheCtl->max_entries = cache_max_entries;
		CacheCtl->max_memory = cache_memory_bytes();
		CacheCtl->num_entries = 0;
		CacheCtl->value_bytes = 0;
		dlist_init(&CacheCtl->queue);
		CacheCtl->hand = NULL;
		pg_atomic_init_u64(&CacheCtl->hits, 0);
		pg_atomic_init_u64(&CacheCtl->misses, 0);
		pg_atomic_init_u64(&CacheCtl->evictions, 0);
		pg_atomic_init_u64(&CacheCtl->expired, 0);
		pg_atomic_init_u64(&CacheCtl->dropped, 0);

		LWLockRegisterTranche(CacheCtl->dsa_tranche, "pg_cache_dsa");
		area = dsa_create_in_place(CacheCtl->dsa_space, cache_memory_bytes(),
								   CacheCtl->dsa_tranche, NULL);
		dsa_pin(area);
		/* never create DSM segments: the whole area is in main shmem */
		dsa_set_size_limit(area, cache_memory_bytes());
		dsa_detach(area);
	}

	memset(&info, 0, sizeof(info));
	info.keysize = CACHE_MAX_KEY_LEN;
	info.entrysize = sizeof(CacheItem);
	CacheHash = ShmemInitHash("pg_cache hash",
							  cache_max_entries, cache_max_entries,
							  &info,
							  HASH_ELEM | HASH_STRINGS | HASH_FIXED_SIZE);

	LWLockRelease(AddinShmemInitLock);
}

void
cache_check_loaded(void)
{
	if (CacheCtl == NULL || CacheHash == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_cache must be loaded via \"shared_preload_libraries\"")));
}

dsa_area *
cache_dsa(void)
{
	if (cache_area == NULL)
	{
		MemoryContext old;

		cache_check_loaded();
		old = MemoryContextSwitchTo(TopMemoryContext);
		LWLockRegisterTranche(CacheCtl->dsa_tranche, "pg_cache_dsa");
		cache_area = dsa_attach_in_place(CacheCtl->dsa_space, NULL);
		dsa_pin_mapping(cache_area);
		MemoryContextSwitchTo(old);
	}
	return cache_area;
}

/* ----------------------------------------------------------------
 * Pending (transactional) writes
 * ---------------------------------------------------------------- */

bool
cache_in_transaction(void)
{
	return cache_transactional && IsTransactionState();
}

static void
pending_reset(void)
{
	/* the memory itself belongs to TopTransactionContext */
	pending_ops = NIL;
	pending_index = NULL;
	pending_flush = false;
}

static void
pending_index_put(CachePendingOp *op)
{
	PendingIndexEntry *e;

	if (pending_index == NULL)
	{
		HASHCTL		info;

		memset(&info, 0, sizeof(info));
		info.keysize = CACHE_MAX_KEY_LEN;
		info.entrysize = sizeof(PendingIndexEntry);
		info.hcxt = TopTransactionContext;
		pending_index = hash_create("pg_cache pending writes", 64, &info,
									HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);
	}
	e = hash_search(pending_index, op->key, HASH_ENTER, NULL);
	e->op = op;
}

/* Rebuild the key index after ops were removed by a subtransaction abort */
static void
pending_reindex(void)
{
	ListCell   *lc;

	if (pending_index)
		hash_destroy(pending_index);
	pending_index = NULL;
	pending_flush = false;
	foreach(lc, pending_ops)
	{
		CachePendingOp *op = lfirst(lc);

		if (op->kind == CACHE_OP_FLUSH)
		{
			if (pending_index)
				hash_destroy(pending_index);
			pending_index = NULL;
			pending_flush = true;
		}
		else if (op->kind != CACHE_OP_EXPIRE)
			pending_index_put(op);
	}
}

void
cache_pending_add(CacheOpKind kind, const char *key, const char *value,
				  uint32 len, uint8 flags, TimestampTz expires_at)
{
	MemoryContext old;
	CachePendingOp *op;

	(void) cache_dsa();			/* attach now, not at commit time */

	old = MemoryContextSwitchTo(TopTransactionContext);
	op = palloc0(sizeof(CachePendingOp));
	op->kind = kind;
	op->nest_level = GetCurrentTransactionNestLevel();
	if (key)
		strlcpy(op->key, key, CACHE_MAX_KEY_LEN);
	if (value)
	{
		op->value = palloc(len + 1);
		memcpy(op->value, value, len);
		op->value[len] = '\0';
	}
	op->value_len = len;
	op->flags = flags;
	op->expires_at = expires_at;
	pending_ops = lappend(pending_ops, op);

	if (kind == CACHE_OP_FLUSH)
	{
		if (pending_index)
			hash_destroy(pending_index);
		pending_index = NULL;
		pending_flush = true;
	}
	else if (kind != CACHE_OP_EXPIRE)
		pending_index_put(op);
	MemoryContextSwitchTo(old);
}

bool
cache_pending_lookup(const char *key, CachePendingOp **op)
{
	PendingIndexEntry *e = NULL;

	*op = NULL;
	if (pending_index)
		e = hash_search(pending_index, key, HASH_FIND, NULL);
	if (e)
	{
		*op = e->op;
		return true;
	}
	return pending_flush;
}

/*
 * Apply the queued writes.  Runs after the commit record is written, so it
 * must not throw: allocation failures are counted in "dropped" instead.
 */
static void
pending_apply(void)
{
	ListCell   *lc;

	if (pending_ops == NIL || CacheCtl == NULL || cache_area == NULL)
		return;

	LWLockAcquire(CacheCtl->lock, LW_EXCLUSIVE);
	foreach(lc, pending_ops)
	{
		CachePendingOp *op = lfirst(lc);

		switch (op->kind)
		{
			case CACHE_OP_SET:
				if (!cache_store_put(op->key, op->value, op->value_len,
									 op->flags, op->expires_at, false))
					pg_atomic_fetch_add_u64(&CacheCtl->dropped, 1);
				break;
			case CACHE_OP_DELETE:
				(void) cache_store_delete(op->key);
				break;
			case CACHE_OP_EXPIRE:
				(void) cache_store_expire(op->key, op->expires_at);
				break;
			case CACHE_OP_FLUSH:
				(void) cache_store_flush();
				break;
		}
	}
	LWLockRelease(CacheCtl->lock);
}

static void
cache_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_PRE_PREPARE:
			if (pending_ops != NIL)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot PREPARE a transaction that has pending pg_cache writes")));
			break;
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
			pending_apply();
			pending_reset();
			break;
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
		case XACT_EVENT_PREPARE:
			pending_reset();
			break;
		default:
			break;
	}
}

static void
cache_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
					   SubTransactionId parentSubid, void *arg)
{
	int			level = GetCurrentTransactionNestLevel();
	ListCell   *lc;

	if (pending_ops == NIL)
		return;

	if (event == SUBXACT_EVENT_COMMIT_SUB)
	{
		foreach(lc, pending_ops)
		{
			CachePendingOp *op = lfirst(lc);

			if (op->nest_level >= level)
				op->nest_level = level - 1;
		}
	}
	else if (event == SUBXACT_EVENT_ABORT_SUB)
	{
		bool		removed = false;

		foreach(lc, pending_ops)
		{
			CachePendingOp *op = lfirst(lc);

			if (op->nest_level >= level)
			{
				pending_ops = foreach_delete_current(pending_ops, lc);
				removed = true;
			}
		}
		if (removed)
			pending_reindex();
	}
}

void
_PG_init(void)
{
	/*
	 * Without preloading there is no shared memory; the SQL functions then
	 * report that.  Loading must still succeed so that CREATE EXTENSION,
	 * pg_restore and pg_upgrade can create the functions.
	 */
	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomIntVariable("pg_cache.max_entries",
							"Maximum number of entries in the shared memory cache.",
							NULL,
							&cache_max_entries,
							10000, 16, 50000000,
							PGC_POSTMASTER, 0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pg_cache.max_memory",
							"Memory reserved for cached values.",
							"Values are evicted (SIEVE) when it is full.",
							&cache_max_memory_mb,
							32, 1, 1024 * 1024,
							PGC_POSTMASTER, GUC_UNIT_MB,
							NULL, NULL, NULL);

	DefineCustomBoolVariable("pg_cache.transactional",
							 "Apply cache writes only when the writing transaction commits.",
							 "When off, cache_set, cache_delete, cache_expire and cache_flush take effect immediately.",
							 &cache_transactional,
							 true,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);

	MarkGUCPrefixReserved("pg_cache");

	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = cache_shmem_request;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = cache_shmem_startup;

	if (!callbacks_registered)
	{
		RegisterXactCallback(cache_xact_callback, NULL);
		RegisterSubXactCallback(cache_subxact_callback, NULL);
		callbacks_registered = true;
	}
}
