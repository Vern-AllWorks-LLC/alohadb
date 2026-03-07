#ifndef ALOHADB_RATELIMIT_H
#define ALOHADB_RATELIMIT_H

#include "postgres.h"
#include "fmgr.h"

#define RATELIMIT_MAX_KEY_LEN 256
#define RATELIMIT_MAX_BUCKETS 10000
#define RATELIMIT_SESSION_TABLE "alohadb_ratelimit_sessions"

/* Rate limit entry in shared memory */
typedef struct RateLimitEntry
{
    char    key[RATELIMIT_MAX_KEY_LEN];
    double  tokens;
    double  max_tokens;
    double  refill_rate;        /* tokens per refill */
    int64   refill_interval_us; /* microseconds */
    int64   last_refill_time;   /* microseconds since epoch */
    int64   window_counts[60];  /* sliding window: per-second counters */
    int64   window_start;       /* epoch second of window_counts[0] */
} RateLimitEntry;

/* Function declarations */
extern Datum ratelimit_check(PG_FUNCTION_ARGS);
extern Datum ratelimit_remaining(PG_FUNCTION_ARGS);
extern Datum ratelimit_reset(PG_FUNCTION_ARGS);
extern Datum ratelimit_reset_all(PG_FUNCTION_ARGS);
extern Datum ratelimit_sliding_window(PG_FUNCTION_ARGS);
extern Datum ratelimit_stats(PG_FUNCTION_ARGS);

extern Datum session_set(PG_FUNCTION_ARGS);
extern Datum session_get(PG_FUNCTION_ARGS);
extern Datum session_get_all(PG_FUNCTION_ARGS);
extern Datum session_delete(PG_FUNCTION_ARGS);
extern Datum session_touch(PG_FUNCTION_ARGS);

#endif
