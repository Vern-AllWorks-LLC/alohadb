/* contrib/alohadb_ratelimit/alohadb_ratelimit--1.0.sql */

\echo Use "CREATE EXTENSION alohadb_ratelimit" to load this file.\quit

-- Session store table
CREATE TABLE alohadb_ratelimit_sessions (
    session_id text NOT NULL,
    key text NOT NULL,
    value jsonb NOT NULL,
    expires_at timestamptz NOT NULL DEFAULT now() + interval '24 hours',
    PRIMARY KEY (session_id, key)
);

CREATE INDEX ON alohadb_ratelimit_sessions (expires_at);

-- Rate limiting functions (shared memory token bucket)
CREATE FUNCTION ratelimit_check(
    key text,
    max_tokens int,
    refill_rate float8,
    refill_interval interval
)
RETURNS bool
LANGUAGE C
AS 'MODULE_PATHNAME', 'ratelimit_check';

CREATE FUNCTION ratelimit_remaining(key text)
RETURNS int
LANGUAGE C
AS 'MODULE_PATHNAME', 'ratelimit_remaining';

CREATE FUNCTION ratelimit_reset(key text)
RETURNS void
LANGUAGE C
AS 'MODULE_PATHNAME', 'ratelimit_reset';

CREATE FUNCTION ratelimit_reset_all()
RETURNS void
LANGUAGE C
AS 'MODULE_PATHNAME', 'ratelimit_reset_all';

CREATE FUNCTION ratelimit_sliding_window(
    key text,
    max_requests int,
    window_size interval
)
RETURNS bool
LANGUAGE C
AS 'MODULE_PATHNAME', 'ratelimit_sliding_window';

CREATE FUNCTION ratelimit_stats()
RETURNS TABLE(
    key text,
    tokens float8,
    max_tokens float8,
    last_refill_time bigint
)
LANGUAGE C
AS 'MODULE_PATHNAME', 'ratelimit_stats';

-- Session store functions
CREATE FUNCTION session_set(
    session_id text,
    key text,
    value jsonb,
    ttl text DEFAULT '24 hours'
)
RETURNS void
LANGUAGE C
AS 'MODULE_PATHNAME', 'session_set';

CREATE FUNCTION session_get(session_id text, key text)
RETURNS jsonb
LANGUAGE C
AS 'MODULE_PATHNAME', 'session_get';

CREATE FUNCTION session_get_all(session_id text)
RETURNS jsonb
LANGUAGE C
AS 'MODULE_PATHNAME', 'session_get_all';

CREATE FUNCTION session_delete(session_id text)
RETURNS void
LANGUAGE C
AS 'MODULE_PATHNAME', 'session_delete';

CREATE FUNCTION session_touch(session_id text, ttl interval DEFAULT '24 hours')
RETURNS void
LANGUAGE C
AS 'MODULE_PATHNAME', 'session_touch';

-- Comments
COMMENT ON FUNCTION ratelimit_check(text, int, float8, interval) IS
    'Token bucket rate limiter: returns true if request allowed';
COMMENT ON FUNCTION ratelimit_sliding_window(text, int, interval) IS
    'Sliding window rate limiter: returns true if under limit';
COMMENT ON FUNCTION session_set(text, text, jsonb, text) IS
    'Set a session key-value pair with TTL';
COMMENT ON FUNCTION session_get(text, text) IS
    'Get a session value by session_id and key';
