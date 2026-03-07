/* contrib/alohadb_pool/alohadb_pool--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_pool" to load this file. \quit

-- ----------------------------------------------------------------
-- pool_status()
--
-- Returns one row per tracked connection pool with current
-- statistics from shared memory.
-- ----------------------------------------------------------------
CREATE FUNCTION pool_status()
RETURNS TABLE(
    pool_name text,
    active_connections int,
    idle_connections int,
    waiting_clients int,
    total_served bigint,
    pool_size int
)
LANGUAGE C
AS 'MODULE_PATHNAME', 'pool_status';

COMMENT ON FUNCTION pool_status() IS
'Show connection pool statistics: active, idle, waiting, total served';

-- ----------------------------------------------------------------
-- pool_reset(pool_name text)
--
-- Reset statistics for a specific pool, or all pools if NULL.
-- ----------------------------------------------------------------
CREATE FUNCTION pool_reset(pool_name text DEFAULT NULL)
RETURNS void
LANGUAGE C
AS 'MODULE_PATHNAME', 'pool_reset';

COMMENT ON FUNCTION pool_reset(text) IS
'Reset connection pool statistics for the named pool, or all pools if NULL';

-- ----------------------------------------------------------------
-- pool_settings()
--
-- Returns current connection pooler GUC settings.
-- ----------------------------------------------------------------
CREATE FUNCTION pool_settings()
RETURNS TABLE(
    setting_name text,
    setting_value text
)
LANGUAGE C
AS 'MODULE_PATHNAME', 'pool_settings';

COMMENT ON FUNCTION pool_settings() IS
'Show current connection pooler configuration settings';
