/* contrib/alohadb_cache/alohadb_cache--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_cache" to load this file. \quit

-- ----------------------------------------------------------------
-- cache_set(key text, value jsonb, ttl interval DEFAULT NULL)
--
-- Store a key-value pair in the shared memory cache.
-- If the key already exists, update the value and TTL.
-- If the cache is full, the least recently used entry is evicted.
-- If ttl is provided, the entry expires after that interval.
-- ----------------------------------------------------------------
CREATE FUNCTION cache_set(
    key text,
    value jsonb,
    ttl interval DEFAULT NULL
)
RETURNS void
AS 'MODULE_PATHNAME', 'cache_set'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION cache_set(text, jsonb, interval) IS
'Store a key-value pair in the shared memory cache with optional TTL';

-- ----------------------------------------------------------------
-- cache_get(key text)
--
-- Retrieve a value from the cache by key.
-- Returns NULL if the key does not exist or has expired.
-- Expired entries are lazily evicted on access.
-- ----------------------------------------------------------------
CREATE FUNCTION cache_get(
    key text
)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'cache_get'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION cache_get(text) IS
'Retrieve a cached value by key, returns NULL if not found or expired';

-- ----------------------------------------------------------------
-- cache_delete(key text)
--
-- Remove an entry from the cache.
-- Returns true if the key existed and was deleted.
-- ----------------------------------------------------------------
CREATE FUNCTION cache_delete(
    key text
)
RETURNS boolean
AS 'MODULE_PATHNAME', 'cache_delete'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION cache_delete(text) IS
'Delete a cached entry by key, returns true if the key existed';

-- ----------------------------------------------------------------
-- cache_flush()
--
-- Remove all entries from the cache.
-- Returns the number of entries that were flushed.
-- ----------------------------------------------------------------
CREATE FUNCTION cache_flush()
RETURNS bigint
AS 'MODULE_PATHNAME', 'cache_flush'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION cache_flush() IS
'Remove all entries from the cache, returns number of entries flushed';

-- ----------------------------------------------------------------
-- cache_stats()
--
-- Return cache statistics including current entries, maximum size,
-- hit/miss counts, and eviction count.
-- ----------------------------------------------------------------
CREATE FUNCTION cache_stats(
    OUT entries int,
    OUT max_entries int,
    OUT hits bigint,
    OUT misses bigint,
    OUT evictions bigint
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'cache_stats'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION cache_stats() IS
'Return cache statistics: entries, max_entries, hits, misses, evictions';
