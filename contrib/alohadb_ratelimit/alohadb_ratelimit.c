#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "alohadb_ratelimit.h"

PG_MODULE_MAGIC_EXT(.name = "alohadb_ratelimit", .version = "1.0");

PG_FUNCTION_INFO_V1(ratelimit_check);
PG_FUNCTION_INFO_V1(ratelimit_remaining);
PG_FUNCTION_INFO_V1(ratelimit_reset);
PG_FUNCTION_INFO_V1(ratelimit_reset_all);
PG_FUNCTION_INFO_V1(ratelimit_sliding_window);
PG_FUNCTION_INFO_V1(ratelimit_stats);

/* GUC */
static int ratelimit_max_entries = 10000;

/* Shared memory state */
static HTAB *ratelimit_hash = NULL;
static LWLock *ratelimit_lock = NULL;

/* Hook save pointers */
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static Size
ratelimit_memsize(void)
{
    return hash_estimate_size(ratelimit_max_entries, sizeof(RateLimitEntry));
}

static void
ratelimit_shmem_request(void)
{
    if (prev_shmem_request_hook)
        prev_shmem_request_hook();

    RequestAddinShmemSpace(ratelimit_memsize());
    RequestNamedLWLockTranche("alohadb_ratelimit", 1);
}

static void
ratelimit_shmem_startup(void)
{
    HASHCTL info;

    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    memset(&info, 0, sizeof(info));
    info.keysize = RATELIMIT_MAX_KEY_LEN;
    info.entrysize = sizeof(RateLimitEntry);

    ratelimit_hash = ShmemInitHash("alohadb_ratelimit_hash",
                                    ratelimit_max_entries,
                                    ratelimit_max_entries,
                                    &info,
                                    HASH_ELEM | HASH_STRINGS);

    ratelimit_lock = &(GetNamedLWLockTranche("alohadb_ratelimit"))->lock;

    LWLockRelease(AddinShmemInitLock);
}

void
_PG_init(void)
{
    if (!process_shared_preload_libraries_in_progress)
        return;

    DefineCustomIntVariable("alohadb.ratelimit_max_entries",
                            "Maximum number of rate limit entries",
                            NULL,
                            &ratelimit_max_entries,
                            10000,
                            100,
                            1000000,
                            PGC_POSTMASTER,
                            0,
                            NULL, NULL, NULL);

    prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook = ratelimit_shmem_request;

    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = ratelimit_shmem_startup;
}

/* Helper: get current time in microseconds */
static int64
get_current_time_us(void)
{
    TimestampTz now = GetCurrentTimestamp();
    /* Convert from PG epoch to unix epoch not needed for relative comparison */
    return now;
}

/* Helper: refill tokens based on elapsed time */
static void
refill_tokens(RateLimitEntry *entry, int64 now_us)
{
    int64 elapsed_us;
    double intervals;
    double new_tokens;

    if (entry->refill_interval_us <= 0)
        return;

    elapsed_us = now_us - entry->last_refill_time;
    if (elapsed_us <= 0)
        return;

    intervals = (double) elapsed_us / (double) entry->refill_interval_us;
    new_tokens = intervals * entry->refill_rate;

    entry->tokens += new_tokens;
    if (entry->tokens > entry->max_tokens)
        entry->tokens = entry->max_tokens;

    entry->last_refill_time = now_us;
}

/*
 * ratelimit_check(key text, max_tokens int, refill_rate float8, refill_interval interval)
 * Returns true if request is allowed (token available), false if rate limited.
 */
Datum
ratelimit_check(PG_FUNCTION_ARGS)
{
    char *key = text_to_cstring(PG_GETARG_TEXT_PP(0));
    int32 max_tokens = PG_GETARG_INT32(1);
    float8 refill_rate = PG_GETARG_FLOAT8(2);
    Interval *refill_iv = PG_GETARG_INTERVAL_P(3);
    int64 refill_us;
    RateLimitEntry *entry;
    bool found;
    bool allowed;
    int64 now_us;

    if (!ratelimit_hash)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("alohadb_ratelimit must be loaded via shared_preload_libraries")));

    refill_us = refill_iv->time + (int64)refill_iv->day * USECS_PER_DAY +
                (int64)refill_iv->month * 30 * USECS_PER_DAY;

    now_us = get_current_time_us();

    LWLockAcquire(ratelimit_lock, LW_EXCLUSIVE);

    entry = (RateLimitEntry *) hash_search(ratelimit_hash, key, HASH_ENTER, &found);

    if (!found)
    {
        /* Initialize new entry */
        entry->tokens = max_tokens;  /* start full */
        entry->max_tokens = max_tokens;
        entry->refill_rate = refill_rate;
        entry->refill_interval_us = refill_us;
        entry->last_refill_time = now_us;
        memset(entry->window_counts, 0, sizeof(entry->window_counts));
        entry->window_start = 0;
    }

    /* Update parameters in case they changed */
    entry->max_tokens = max_tokens;
    entry->refill_rate = refill_rate;
    entry->refill_interval_us = refill_us;

    /* Refill tokens */
    refill_tokens(entry, now_us);

    /* Try to consume a token */
    if (entry->tokens >= 1.0)
    {
        entry->tokens -= 1.0;
        allowed = true;
    }
    else
        allowed = false;

    LWLockRelease(ratelimit_lock);

    pfree(key);
    PG_RETURN_BOOL(allowed);
}

/*
 * ratelimit_remaining(key text) -> int
 */
Datum
ratelimit_remaining(PG_FUNCTION_ARGS)
{
    char *key = text_to_cstring(PG_GETARG_TEXT_PP(0));
    RateLimitEntry *entry;
    bool found;
    int remaining = 0;

    if (!ratelimit_hash)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("alohadb_ratelimit must be loaded via shared_preload_libraries")));

    LWLockAcquire(ratelimit_lock, LW_SHARED);

    entry = (RateLimitEntry *) hash_search(ratelimit_hash, key, HASH_FIND, &found);
    if (found)
    {
        refill_tokens(entry, get_current_time_us());
        remaining = (int) entry->tokens;
    }

    LWLockRelease(ratelimit_lock);

    pfree(key);
    PG_RETURN_INT32(remaining);
}

Datum
ratelimit_reset(PG_FUNCTION_ARGS)
{
    char *key = text_to_cstring(PG_GETARG_TEXT_PP(0));

    if (!ratelimit_hash)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("alohadb_ratelimit must be loaded via shared_preload_libraries")));

    LWLockAcquire(ratelimit_lock, LW_EXCLUSIVE);
    hash_search(ratelimit_hash, key, HASH_REMOVE, NULL);
    LWLockRelease(ratelimit_lock);

    pfree(key);
    PG_RETURN_VOID();
}

Datum
ratelimit_reset_all(PG_FUNCTION_ARGS)
{
    HASH_SEQ_STATUS status;
    RateLimitEntry *entry;

    if (!ratelimit_hash)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("alohadb_ratelimit must be loaded via shared_preload_libraries")));

    LWLockAcquire(ratelimit_lock, LW_EXCLUSIVE);

    hash_seq_init(&status, ratelimit_hash);
    while ((entry = (RateLimitEntry *) hash_seq_search(&status)) != NULL)
        hash_search(ratelimit_hash, entry->key, HASH_REMOVE, NULL);

    LWLockRelease(ratelimit_lock);

    PG_RETURN_VOID();
}

/*
 * ratelimit_sliding_window(key text, max_requests int, window interval)
 * Returns true if allowed, false if rate limited.
 * Uses a sliding window of per-second counters in shared memory.
 */
Datum
ratelimit_sliding_window(PG_FUNCTION_ARGS)
{
    char *key = text_to_cstring(PG_GETARG_TEXT_PP(0));
    int32 max_requests = PG_GETARG_INT32(1);
    Interval *window_iv = PG_GETARG_INTERVAL_P(2);
    int64 window_secs;
    RateLimitEntry *entry;
    bool found;
    bool allowed;
    int64 now_sec;
    int64 total;
    int i;

    if (!ratelimit_hash)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("alohadb_ratelimit must be loaded via shared_preload_libraries")));

    window_secs = (window_iv->time / 1000000) + window_iv->day * 86400 +
                  window_iv->month * 30 * 86400;
    if (window_secs <= 0) window_secs = 1;
    if (window_secs > 60) window_secs = 60; /* max 60-second window for array */

    now_sec = get_current_time_us() / 1000000;

    LWLockAcquire(ratelimit_lock, LW_EXCLUSIVE);

    entry = (RateLimitEntry *) hash_search(ratelimit_hash, key, HASH_ENTER, &found);

    if (!found)
    {
        memset(entry->window_counts, 0, sizeof(entry->window_counts));
        entry->window_start = now_sec;
        entry->tokens = 0;
        entry->max_tokens = 0;
        entry->refill_rate = 0;
        entry->refill_interval_us = 0;
        entry->last_refill_time = 0;
    }

    /* Expire old seconds */
    if (now_sec > entry->window_start)
    {
        int64 shift = now_sec - entry->window_start;
        if (shift >= 60)
        {
            memset(entry->window_counts, 0, sizeof(entry->window_counts));
        }
        else
        {
            /* Shift the array */
            memmove(entry->window_counts,
                    entry->window_counts + shift,
                    (60 - shift) * sizeof(int64));
            memset(entry->window_counts + (60 - shift), 0,
                   shift * sizeof(int64));
        }
        entry->window_start = now_sec;
    }

    /* Count requests in the window */
    total = 0;
    for (i = 60 - (int)window_secs; i < 60; i++)
    {
        if (i >= 0)
            total += entry->window_counts[i];
    }

    if (total < max_requests)
    {
        entry->window_counts[59]++;
        allowed = true;
    }
    else
        allowed = false;

    LWLockRelease(ratelimit_lock);

    pfree(key);
    PG_RETURN_BOOL(allowed);
}

/*
 * ratelimit_stats() -> TABLE
 */
Datum
ratelimit_stats(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    HASH_SEQ_STATUS status;
    RateLimitEntry *entry;

    if (!ratelimit_hash)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("alohadb_ratelimit must be loaded via shared_preload_libraries")));

    InitMaterializedSRF(fcinfo, 0);

    LWLockAcquire(ratelimit_lock, LW_SHARED);

    hash_seq_init(&status, ratelimit_hash);
    while ((entry = (RateLimitEntry *) hash_seq_search(&status)) != NULL)
    {
        Datum values[4];
        bool nulls[4] = {false};

        values[0] = CStringGetTextDatum(entry->key);
        values[1] = Float8GetDatum(entry->tokens);
        values[2] = Float8GetDatum(entry->max_tokens);
        values[3] = Int64GetDatum(entry->last_refill_time);

        tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
    }

    LWLockRelease(ratelimit_lock);

    PG_RETURN_NULL();
}
