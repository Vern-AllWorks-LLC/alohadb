/*-------------------------------------------------------------------------
 *
 * cache_store.c
 *	  Cache storage (SIEVE eviction, TTL) and the SQL-callable functions of
 *	  the pg_cache extension.
 *
 *	  Locking: readers take the cache lock in shared mode and only set the
 *	  item's SIEVE "visited" flag (a benign racy store) and bump atomic
 *	  counters.  Every structural change takes it exclusively.
 *
 * Copyright (c) 2025-2026, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_cache/cache_store.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_collation_d.h"
#include "common/int.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/jsonb.h"
#include "utils/timestamp.h"

#include "pg_cache.h"

PG_FUNCTION_INFO_V1(cache_set);
PG_FUNCTION_INFO_V1(cache_set_text);
PG_FUNCTION_INFO_V1(cache_get);
PG_FUNCTION_INFO_V1(cache_get_text);
PG_FUNCTION_INFO_V1(cache_delete);
PG_FUNCTION_INFO_V1(cache_flush);
PG_FUNCTION_INFO_V1(cache_expire);
PG_FUNCTION_INFO_V1(cache_ttl);
PG_FUNCTION_INFO_V1(cache_add);
PG_FUNCTION_INFO_V1(cache_incr);
PG_FUNCTION_INFO_V1(cache_keys);
PG_FUNCTION_INFO_V1(cache_purge_expired);
PG_FUNCTION_INFO_V1(cache_stats);

/* ----------------------------------------------------------------
 * Storage primitives -- caller holds CacheCtl->lock exclusively
 * ---------------------------------------------------------------- */

Size
cache_max_value_len(void)
{
	Size		limit = CacheCtl->max_memory / 4;

	return Min(limit, (Size) MaxAllocSize - 1);
}

static inline bool
item_expired(CacheItem *item, TimestampTz now)
{
	return item->expires_at != 0 && item->expires_at <= now;
}

static void
item_remove(CacheItem *item)
{
	dsa_area   *area = cache_dsa();

	if (CacheCtl->hand == item)
		CacheCtl->hand = dlist_has_prev(&CacheCtl->queue, &item->fifo) ?
			dlist_container(CacheItem, fifo, dlist_prev_node(&CacheCtl->queue, &item->fifo)) :
			NULL;
	dlist_delete(&item->fifo);
	if (DsaPointerIsValid(item->value))
		dsa_free(area, item->value);
	CacheCtl->value_bytes -= item->value_len;
	CacheCtl->num_entries--;
	(void) hash_search(CacheHash, item->key, HASH_REMOVE, NULL);
}

/*
 * SIEVE: the hand walks from the oldest entry toward the newest, clearing
 * "visited" bits, and evicts the first entry it finds unvisited (or
 * expired).  New entries go to the head; hits never move entries.
 */
static bool
evict_one(TimestampTz now)
{
	CacheItem  *item;

	if (dlist_is_empty(&CacheCtl->queue))
		return false;

	item = CacheCtl->hand;
	if (item == NULL)
		item = dlist_tail_element(CacheItem, fifo, &CacheCtl->queue);

	for (;;)
	{
		if (!item->visited || item_expired(item, now))
			break;
		item->visited = false;
		if (dlist_has_prev(&CacheCtl->queue, &item->fifo))
			item = dlist_container(CacheItem, fifo,
								   dlist_prev_node(&CacheCtl->queue, &item->fifo));
		else
			item = dlist_tail_element(CacheItem, fifo, &CacheCtl->queue);
	}

	if (item_expired(item, now))
		pg_atomic_fetch_add_u64(&CacheCtl->expired, 1);
	else
		pg_atomic_fetch_add_u64(&CacheCtl->evictions, 1);
	CacheCtl->hand = item;		/* item_remove moves the hand past it */
	item_remove(item);
	return true;
}

/*
 * Insert or replace an entry.  With throw_errors false this never raises an
 * error (it runs from the commit callback) and returns false if the value
 * cannot be stored.
 */
bool
cache_store_put(const char *key, const char *value, uint32 len, uint8 flags,
				TimestampTz expires_at, bool throw_errors)
{
	dsa_area   *area = cache_dsa();
	TimestampTz now = GetCurrentTimestamp();
	CacheItem  *item;
	dsa_pointer dp;
	bool		found;

	if (len > cache_max_value_len())
	{
		if (throw_errors)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("pg_cache value too large (%u bytes, maximum is %zu)",
							len, cache_max_value_len())));
		return false;
	}

	item = hash_search(CacheHash, key, HASH_FIND, NULL);
	if (item)
		item_remove(item);

	while (CacheCtl->num_entries >= CacheCtl->max_entries)
		if (!evict_one(now))
			break;

	for (;;)
	{
		dp = dsa_allocate_extended(area, Max(len, 1), DSA_ALLOC_NO_OOM);
		if (DsaPointerIsValid(dp))
			break;
		if (!evict_one(now))
		{
			if (throw_errors)
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("pg_cache could not allocate %u bytes", len),
						 errhint("Increase \"pg_cache.max_memory\".")));
			return false;
		}
	}

	item = hash_search(CacheHash, key, HASH_ENTER_NULL, &found);
	if (item == NULL)
	{
		dsa_free(area, dp);
		if (throw_errors)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("pg_cache hash table is full")));
		return false;
	}
	Assert(!found);

	memcpy(dsa_get_address(area, dp), value, len);
	item->value = dp;
	item->value_len = len;
	item->flags = flags;
	item->visited = false;
	item->created_at = now;
	item->expires_at = expires_at;
	dlist_push_head(&CacheCtl->queue, &item->fifo);
	CacheCtl->num_entries++;
	CacheCtl->value_bytes += len;
	return true;
}

bool
cache_store_delete(const char *key)
{
	CacheItem  *item = hash_search(CacheHash, key, HASH_FIND, NULL);

	if (item == NULL)
		return false;
	item_remove(item);
	return true;
}

int64
cache_store_flush(void)
{
	int64		n = 0;

	while (!dlist_is_empty(&CacheCtl->queue))
	{
		item_remove(dlist_head_element(CacheItem, fifo, &CacheCtl->queue));
		n++;
	}
	CacheCtl->hand = NULL;
	return n;
}

bool
cache_store_expire(const char *key, TimestampTz expires_at)
{
	CacheItem  *item = hash_search(CacheHash, key, HASH_FIND, NULL);

	if (item == NULL || item_expired(item, GetCurrentTimestamp()))
		return false;
	item->expires_at = expires_at;
	return true;
}

/* ----------------------------------------------------------------
 * Helpers for the SQL functions
 * ---------------------------------------------------------------- */

static void
key_from_text(text *t, char *buf)
{
	int			len = VARSIZE_ANY_EXHDR(t);

	if (len <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_cache key must not be empty")));
	if (len >= CACHE_MAX_KEY_LEN)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("pg_cache key too long (%d bytes, maximum is %d)",
						len, CACHE_MAX_KEY_LEN - 1)));
	memcpy(buf, VARDATA_ANY(t), len);
	buf[len] = '\0';
	if (strlen(buf) != (size_t) len)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_cache key must not contain NUL bytes")));
}

/* expiry timestamp for a TTL argument; 0 if the TTL is NULL */
static TimestampTz
expiry_from_ttl(FunctionCallInfo fcinfo, int argno)
{
	Interval   *ttl;
	TimestampTz now;
	TimestampTz exp;

	if (PG_NARGS() <= argno || PG_ARGISNULL(argno))
		return 0;
	ttl = PG_GETARG_INTERVAL_P(argno);
	now = GetCurrentTimestamp();
	exp = DatumGetTimestampTz(DirectFunctionCall2(timestamptz_pl_interval,
												  TimestampTzGetDatum(now),
												  IntervalPGetDatum(ttl)));
	if (TIMESTAMP_NOT_FINITE(exp) || exp <= now)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_cache TTL must be a positive, finite interval")));
	return exp;
}

/*
 * Fetch the value visible to this transaction: its own pending writes first,
 * then shared memory.  Returns a palloc'd copy (NUL-terminated) or NULL.
 */
static char *
lookup_value(const char *key, uint32 *len, uint8 *flags,
			 TimestampTz *expires_at, bool count_stats)
{
	CachePendingOp *op;
	CacheItem  *item;
	char	   *result = NULL;
	TimestampTz now;

	if (cache_pending_lookup(key, &op))
	{
		if (op == NULL || op->kind != CACHE_OP_SET)
			return NULL;
		result = palloc(op->value_len + 1);
		memcpy(result, op->value, op->value_len + 1);
		*len = op->value_len;
		*flags = op->flags;
		if (expires_at)
			*expires_at = op->expires_at;
		return result;
	}

	now = GetCurrentTimestamp();
	LWLockAcquire(CacheCtl->lock, LW_SHARED);
	item = hash_search(CacheHash, key, HASH_FIND, NULL);
	if (item && !item_expired(item, now))
	{
		result = palloc(item->value_len + 1);
		memcpy(result, dsa_get_address(cache_dsa(), item->value),
			   item->value_len);
		result[item->value_len] = '\0';
		*len = item->value_len;
		*flags = item->flags;
		if (expires_at)
			*expires_at = item->expires_at;
		item->visited = true;
	}
	LWLockRelease(CacheCtl->lock);

	if (count_stats)
		pg_atomic_fetch_add_u64(result ? &CacheCtl->hits : &CacheCtl->misses, 1);
	return result;
}

static void
do_set(const char *key, const char *value, uint32 len, uint8 flags,
	   TimestampTz expires_at)
{
	if (len > cache_max_value_len())
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("pg_cache value too large (%u bytes, maximum is %zu)",
						len, cache_max_value_len())));

	if (cache_in_transaction())
	{
		cache_pending_add(CACHE_OP_SET, key, value, len, flags, expires_at);
		return;
	}
	LWLockAcquire(CacheCtl->lock, LW_EXCLUSIVE);
	(void) cache_store_put(key, value, len, flags, expires_at, true);
	LWLockRelease(CacheCtl->lock);
}

/* ----------------------------------------------------------------
 * SQL-callable functions
 * ---------------------------------------------------------------- */

/* cache_set(key text, value jsonb, ttl interval DEFAULT NULL) RETURNS void */
Datum
cache_set(PG_FUNCTION_ARGS)
{
	char		key[CACHE_MAX_KEY_LEN];
	Jsonb	   *jb;
	char	   *str;

	cache_check_loaded();
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("pg_cache key and value must not be NULL")));
	key_from_text(PG_GETARG_TEXT_PP(0), key);
	jb = PG_GETARG_JSONB_P(1);
	str = JsonbToCString(NULL, &jb->root, VARSIZE(jb));
	do_set(key, str, strlen(str), CACHE_ITEM_JSON, expiry_from_ttl(fcinfo, 2));
	PG_RETURN_VOID();
}

/* cache_set_text(key text, value text, ttl interval DEFAULT NULL) RETURNS void */
Datum
cache_set_text(PG_FUNCTION_ARGS)
{
	char		key[CACHE_MAX_KEY_LEN];
	text	   *val;

	cache_check_loaded();
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("pg_cache key and value must not be NULL")));
	key_from_text(PG_GETARG_TEXT_PP(0), key);
	val = PG_GETARG_TEXT_PP(1);
	do_set(key, VARDATA_ANY(val), VARSIZE_ANY_EXHDR(val), 0,
		   expiry_from_ttl(fcinfo, 2));
	PG_RETURN_VOID();
}

/* cache_get(key text) RETURNS jsonb */
Datum
cache_get(PG_FUNCTION_ARGS)
{
	char		key[CACHE_MAX_KEY_LEN];
	char	   *val;
	uint32		len;
	uint8		flags;

	cache_check_loaded();
	key_from_text(PG_GETARG_TEXT_PP(0), key);
	val = lookup_value(key, &len, &flags, NULL, true);
	if (val == NULL)
		PG_RETURN_NULL();
	PG_RETURN_DATUM(DirectFunctionCall1(jsonb_in, CStringGetDatum(val)));
}

/* cache_get_text(key text) RETURNS text */
Datum
cache_get_text(PG_FUNCTION_ARGS)
{
	char		key[CACHE_MAX_KEY_LEN];
	char	   *val;
	uint32		len;
	uint8		flags;

	cache_check_loaded();
	key_from_text(PG_GETARG_TEXT_PP(0), key);
	val = lookup_value(key, &len, &flags, NULL, true);
	if (val == NULL)
		PG_RETURN_NULL();
	PG_RETURN_TEXT_P(cstring_to_text_with_len(val, len));
}

/* cache_delete(key text) RETURNS boolean: did the key exist? */
Datum
cache_delete(PG_FUNCTION_ARGS)
{
	char		key[CACHE_MAX_KEY_LEN];
	bool		existed;

	cache_check_loaded();
	key_from_text(PG_GETARG_TEXT_PP(0), key);

	if (cache_in_transaction())
	{
		uint32		len;
		uint8		flags;

		existed = lookup_value(key, &len, &flags, NULL, false) != NULL;
		cache_pending_add(CACHE_OP_DELETE, key, NULL, 0, 0, 0);
		PG_RETURN_BOOL(existed);
	}
	LWLockAcquire(CacheCtl->lock, LW_EXCLUSIVE);
	existed = cache_store_delete(key);
	LWLockRelease(CacheCtl->lock);
	PG_RETURN_BOOL(existed);
}

/* cache_flush() RETURNS bigint: number of entries removed */
Datum
cache_flush(PG_FUNCTION_ARGS)
{
	int64		n;

	cache_check_loaded();
	if (cache_in_transaction())
	{
		LWLockAcquire(CacheCtl->lock, LW_SHARED);
		n = CacheCtl->num_entries;
		LWLockRelease(CacheCtl->lock);
		cache_pending_add(CACHE_OP_FLUSH, NULL, NULL, 0, 0, 0);
		PG_RETURN_INT64(n);
	}
	LWLockAcquire(CacheCtl->lock, LW_EXCLUSIVE);
	n = cache_store_flush();
	LWLockRelease(CacheCtl->lock);
	PG_RETURN_INT64(n);
}

/*
 * cache_expire(key text, ttl interval) RETURNS boolean
 * Set (or with NULL, remove) the expiry of an existing key.
 */
Datum
cache_expire(PG_FUNCTION_ARGS)
{
	char		key[CACHE_MAX_KEY_LEN];
	TimestampTz exp;
	bool		ok;

	cache_check_loaded();
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	key_from_text(PG_GETARG_TEXT_PP(0), key);
	exp = expiry_from_ttl(fcinfo, 1);

	if (cache_in_transaction())
	{
		CachePendingOp *op;
		char	   *val;
		uint32		len;
		uint8		flags;

		if (cache_pending_lookup(key, &op))
		{
			/* this transaction wrote the key: re-queue it with the new TTL */
			if (op == NULL || op->kind != CACHE_OP_SET)
				PG_RETURN_BOOL(false);
			val = pstrdup(op->value);
			cache_pending_add(CACHE_OP_SET, key, val, op->value_len,
							  op->flags, exp);
			PG_RETURN_BOOL(true);
		}
		val = lookup_value(key, &len, &flags, NULL, false);
		if (val == NULL)
			PG_RETURN_BOOL(false);
		cache_pending_add(CACHE_OP_EXPIRE, key, NULL, 0, 0, exp);
		PG_RETURN_BOOL(true);
	}

	LWLockAcquire(CacheCtl->lock, LW_EXCLUSIVE);
	ok = cache_store_expire(key, exp);
	LWLockRelease(CacheCtl->lock);
	PG_RETURN_BOOL(ok);
}

/*
 * cache_ttl(key text) RETURNS interval
 * Remaining time to live; 'infinity' if the key never expires, NULL if the
 * key does not exist.
 */
Datum
cache_ttl(PG_FUNCTION_ARGS)
{
	char		key[CACHE_MAX_KEY_LEN];
	char	   *val;
	uint32		len;
	uint8		flags;
	TimestampTz exp = 0;
	Interval   *result;

	cache_check_loaded();
	key_from_text(PG_GETARG_TEXT_PP(0), key);
	val = lookup_value(key, &len, &flags, &exp, false);
	if (val == NULL)
		PG_RETURN_NULL();

	result = palloc0(sizeof(Interval));
	if (exp == 0)
		INTERVAL_NOEND(result);
	else
	{
		TimestampTz now = GetCurrentTimestamp();

		result->time = exp > now ? exp - now : 0;
	}
	PG_RETURN_INTERVAL_P(result);
}

/*
 * cache_add(key text, value text, ttl interval DEFAULT NULL) RETURNS boolean
 * Store the value only if the key does not exist (like Redis SET NX).  This
 * takes effect immediately, even inside a transaction, so it can be used
 * for locks and idempotency keys.
 */
Datum
cache_add(PG_FUNCTION_ARGS)
{
	char		key[CACHE_MAX_KEY_LEN];
	text	   *val;
	TimestampTz exp;
	CachePendingOp *op;
	CacheItem  *item;
	bool		added = false;

	cache_check_loaded();
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("pg_cache key and value must not be NULL")));
	key_from_text(PG_GETARG_TEXT_PP(0), key);
	val = PG_GETARG_TEXT_PP(1);
	exp = expiry_from_ttl(fcinfo, 2);
	if ((Size) VARSIZE_ANY_EXHDR(val) > cache_max_value_len())
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("pg_cache value too large")));

	/* a key this transaction already set counts as existing */
	if (cache_pending_lookup(key, &op) && op && op->kind == CACHE_OP_SET)
		PG_RETURN_BOOL(false);

	(void) cache_dsa();
	LWLockAcquire(CacheCtl->lock, LW_EXCLUSIVE);
	item = hash_search(CacheHash, key, HASH_FIND, NULL);
	if (item == NULL || item_expired(item, GetCurrentTimestamp()))
		added = cache_store_put(key, VARDATA_ANY(val), VARSIZE_ANY_EXHDR(val),
								0, exp, true);
	LWLockRelease(CacheCtl->lock);
	PG_RETURN_BOOL(added);
}

/*
 * cache_incr(key text, delta bigint DEFAULT 1, ttl interval DEFAULT NULL)
 * RETURNS bigint
 * Atomically add delta to an integer value (a missing key counts as 0) and
 * return the result.  Like nextval(), this takes effect immediately and is
 * not rolled back.  An existing key keeps its expiry; ttl applies only when
 * the key is created.
 */
Datum
cache_incr(PG_FUNCTION_ARGS)
{
	char		key[CACHE_MAX_KEY_LEN];
	int64		delta = PG_ARGISNULL(1) ? 1 : PG_GETARG_INT64(1);
	TimestampTz exp;
	TimestampTz now;
	CacheItem  *item;
	int64		cur = 0;
	int64		result;
	char		buf[32];
	int			len;

	cache_check_loaded();
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	key_from_text(PG_GETARG_TEXT_PP(0), key);
	exp = expiry_from_ttl(fcinfo, 2);
	now = GetCurrentTimestamp();

	(void) cache_dsa();
	LWLockAcquire(CacheCtl->lock, LW_EXCLUSIVE);
	item = hash_search(CacheHash, key, HASH_FIND, NULL);
	if (item && !item_expired(item, now))
	{
		char		nbuf[32];
		char	   *end;

		if (item->value_len == 0 || item->value_len >= sizeof(nbuf))
			goto not_integer;
		memcpy(nbuf, dsa_get_address(cache_dsa(), item->value),
			   item->value_len);
		nbuf[item->value_len] = '\0';
		errno = 0;
		cur = strtoi64(nbuf, &end, 10);
		if (errno != 0 || *end != '\0' || end == nbuf)
			goto not_integer;
		exp = item->expires_at;
	}
	if (pg_add_s64_overflow(cur, delta, &result))
	{
		LWLockRelease(CacheCtl->lock);
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("cache_incr result out of range")));
	}
	len = snprintf(buf, sizeof(buf), INT64_FORMAT, result);
	(void) cache_store_put(key, buf, len, 0, exp, true);
	LWLockRelease(CacheCtl->lock);
	PG_RETURN_INT64(result);

not_integer:
	LWLockRelease(CacheCtl->lock);
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("pg_cache value for key \"%s\" is not an integer", key)));
	PG_RETURN_NULL();			/* keep compiler quiet */
}

/*
 * cache_keys(pattern text DEFAULT '%') RETURNS SETOF text
 * Committed, unexpired keys matching a LIKE pattern (C collation).
 */
Datum
cache_keys(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	text	   *pattern;
	HASH_SEQ_STATUS status;
	CacheItem  *item;
	List	   *keys = NIL;
	ListCell   *lc;
	TimestampTz now = GetCurrentTimestamp();

	cache_check_loaded();
	pattern = PG_ARGISNULL(0) ? cstring_to_text("%") : PG_GETARG_TEXT_PP(0);
	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	LWLockAcquire(CacheCtl->lock, LW_SHARED);
	hash_seq_init(&status, CacheHash);
	while ((item = hash_seq_search(&status)) != NULL)
		if (!item_expired(item, now))
			keys = lappend(keys, pstrdup(item->key));
	LWLockRelease(CacheCtl->lock);

	foreach(lc, keys)
	{
		text	   *k = cstring_to_text(lfirst(lc));
		Datum		value;
		bool		isnull = false;

		if (!DatumGetBool(DirectFunctionCall2Coll(textlike, C_COLLATION_OID,
												  PointerGetDatum(k),
												  PointerGetDatum(pattern))))
			continue;
		value = PointerGetDatum(k);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, &value, &isnull);
	}
	return (Datum) 0;
}

/* cache_purge_expired() RETURNS bigint */
Datum
cache_purge_expired(PG_FUNCTION_ARGS)
{
	TimestampTz now = GetCurrentTimestamp();
	dlist_mutable_iter iter;
	int64		n = 0;

	cache_check_loaded();
	(void) cache_dsa();
	LWLockAcquire(CacheCtl->lock, LW_EXCLUSIVE);
	dlist_foreach_modify(iter, &CacheCtl->queue)
	{
		CacheItem  *item = dlist_container(CacheItem, fifo, iter.cur);

		if (item_expired(item, now))
		{
			item_remove(item);
			n++;
		}
	}
	LWLockRelease(CacheCtl->lock);
	pg_atomic_fetch_add_u64(&CacheCtl->expired, n);
	PG_RETURN_INT64(n);
}

/* cache_stats() */
Datum
cache_stats(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Datum		values[9];
	bool		nulls[9] = {0};

	cache_check_loaded();
	InitMaterializedSRF(fcinfo, 0);

	LWLockAcquire(CacheCtl->lock, LW_SHARED);
	values[0] = Int32GetDatum(CacheCtl->num_entries);
	values[1] = Int32GetDatum(CacheCtl->max_entries);
	values[2] = Int64GetDatum((int64) CacheCtl->value_bytes);
	values[3] = Int64GetDatum((int64) CacheCtl->max_memory);
	LWLockRelease(CacheCtl->lock);
	values[4] = Int64GetDatum((int64) pg_atomic_read_u64(&CacheCtl->hits));
	values[5] = Int64GetDatum((int64) pg_atomic_read_u64(&CacheCtl->misses));
	values[6] = Int64GetDatum((int64) pg_atomic_read_u64(&CacheCtl->evictions));
	values[7] = Int64GetDatum((int64) pg_atomic_read_u64(&CacheCtl->expired));
	values[8] = Int64GetDatum((int64) pg_atomic_read_u64(&CacheCtl->dropped));
	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	return (Datum) 0;
}
