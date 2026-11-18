/* contrib/pg_cache/pg_cache--2.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_cache" to load this file. \quit

-- Writes (cache_set, cache_set_text, cache_delete, cache_expire,
-- cache_flush) are applied when the calling transaction commits and
-- discarded if it aborts, unless pg_cache.transactional is off.
-- cache_add and cache_incr always take effect immediately.

CREATE FUNCTION cache_set(key text, value jsonb, ttl interval DEFAULT NULL)
RETURNS void
AS 'MODULE_PATHNAME', 'cache_set'
LANGUAGE C VOLATILE PARALLEL UNSAFE;

COMMENT ON FUNCTION cache_set(text, jsonb, interval) IS
'Store a jsonb value under a key, with an optional time to live';

CREATE FUNCTION cache_set_text(key text, value text, ttl interval DEFAULT NULL)
RETURNS void
AS 'MODULE_PATHNAME', 'cache_set_text'
LANGUAGE C VOLATILE PARALLEL UNSAFE;

COMMENT ON FUNCTION cache_set_text(text, text, interval) IS
'Store a text value under a key, with an optional time to live';

CREATE FUNCTION cache_get(key text)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'cache_get'
LANGUAGE C VOLATILE STRICT PARALLEL UNSAFE;

COMMENT ON FUNCTION cache_get(text) IS
'Return the value of a key as jsonb, or NULL if it is missing or expired';

CREATE FUNCTION cache_get_text(key text)
RETURNS text
AS 'MODULE_PATHNAME', 'cache_get_text'
LANGUAGE C VOLATILE STRICT PARALLEL UNSAFE;

COMMENT ON FUNCTION cache_get_text(text) IS
'Return the value of a key as text, or NULL if it is missing or expired';

CREATE FUNCTION cache_delete(key text)
RETURNS boolean
AS 'MODULE_PATHNAME', 'cache_delete'
LANGUAGE C VOLATILE STRICT PARALLEL UNSAFE;

COMMENT ON FUNCTION cache_delete(text) IS
'Delete a key; returns true if it existed';

CREATE FUNCTION cache_flush()
RETURNS bigint
AS 'MODULE_PATHNAME', 'cache_flush'
LANGUAGE C VOLATILE PARALLEL UNSAFE;

COMMENT ON FUNCTION cache_flush() IS
'Remove all entries; returns the number of entries';

CREATE FUNCTION cache_expire(key text, ttl interval)
RETURNS boolean
AS 'MODULE_PATHNAME', 'cache_expire'
LANGUAGE C VOLATILE PARALLEL UNSAFE;

COMMENT ON FUNCTION cache_expire(text, interval) IS
'Set the time to live of an existing key (NULL removes the expiry); returns false if the key does not exist';

CREATE FUNCTION cache_ttl(key text)
RETURNS interval
AS 'MODULE_PATHNAME', 'cache_ttl'
LANGUAGE C VOLATILE STRICT PARALLEL UNSAFE;

COMMENT ON FUNCTION cache_ttl(text) IS
'Remaining time to live of a key: infinity if it never expires, NULL if it does not exist';

CREATE FUNCTION cache_add(key text, value text, ttl interval DEFAULT NULL)
RETURNS boolean
AS 'MODULE_PATHNAME', 'cache_add'
LANGUAGE C VOLATILE PARALLEL UNSAFE;

COMMENT ON FUNCTION cache_add(text, text, interval) IS
'Store a value only if the key does not exist (immediate, not transactional); returns true if stored';

CREATE FUNCTION cache_incr(key text, delta bigint DEFAULT 1, ttl interval DEFAULT NULL)
RETURNS bigint
AS 'MODULE_PATHNAME', 'cache_incr'
LANGUAGE C VOLATILE PARALLEL UNSAFE;

COMMENT ON FUNCTION cache_incr(text, bigint, interval) IS
'Atomically add delta to an integer value and return the result (immediate, not transactional)';

CREATE FUNCTION cache_keys(pattern text DEFAULT '%')
RETURNS SETOF text
AS 'MODULE_PATHNAME', 'cache_keys'
LANGUAGE C VOLATILE PARALLEL UNSAFE;

COMMENT ON FUNCTION cache_keys(text) IS
'Committed, unexpired keys matching a LIKE pattern';

CREATE FUNCTION cache_purge_expired()
RETURNS bigint
AS 'MODULE_PATHNAME', 'cache_purge_expired'
LANGUAGE C VOLATILE PARALLEL UNSAFE;

COMMENT ON FUNCTION cache_purge_expired() IS
'Remove expired entries now; returns how many were removed';

CREATE FUNCTION cache_stats(
    OUT entries int,
    OUT max_entries int,
    OUT used_bytes bigint,
    OUT max_bytes bigint,
    OUT hits bigint,
    OUT misses bigint,
    OUT evictions bigint,
    OUT expired bigint,
    OUT dropped bigint
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'cache_stats'
LANGUAGE C VOLATILE PARALLEL UNSAFE;

COMMENT ON FUNCTION cache_stats() IS
'Cache statistics; dropped counts commit-time writes that did not fit';

-- Only superusers may use the cache by default.
REVOKE ALL ON FUNCTION cache_set(text, jsonb, interval), cache_set_text(text, text, interval),
    cache_get(text), cache_get_text(text), cache_delete(text), cache_flush(),
    cache_expire(text, interval), cache_ttl(text), cache_add(text, text, interval),
    cache_incr(text, bigint, interval), cache_keys(text), cache_purge_expired(),
    cache_stats() FROM PUBLIC;
