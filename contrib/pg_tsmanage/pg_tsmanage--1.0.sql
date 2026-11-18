/* contrib/pg_tsmanage/pg_tsmanage--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_tsmanage" to load this file. \quit

-- ----------------------------------------------------------------
-- pg_tsmanage_config
--
-- Each row defines a time-series partition management policy.
-- The background worker creates future partitions and drops
-- expired ones based on these settings.
-- ----------------------------------------------------------------
CREATE TABLE pg_tsmanage_config (
    table_name         regclass    PRIMARY KEY,
    partition_column   text        NOT NULL,
    partition_interval interval    NOT NULL,
    retention_interval interval,
    premake_count      int         DEFAULT 3,
    enabled            boolean     DEFAULT true,
    created_at         timestamptz DEFAULT now()
);

COMMENT ON TABLE pg_tsmanage_config IS
'Configuration for automatic time-series partition management';

COMMENT ON COLUMN pg_tsmanage_config.table_name IS
'The partitioned (parent) table to manage';

COMMENT ON COLUMN pg_tsmanage_config.partition_column IS
'Name of the timestamp column used as the partition key';

COMMENT ON COLUMN pg_tsmanage_config.partition_interval IS
'Width of each partition (e.g., ''1 day'', ''1 hour'')';

COMMENT ON COLUMN pg_tsmanage_config.retention_interval IS
'Drop partitions older than this interval from now (NULL = no retention)';

COMMENT ON COLUMN pg_tsmanage_config.premake_count IS
'Number of future partitions to pre-create ahead of current time';

COMMENT ON COLUMN pg_tsmanage_config.enabled IS
'Set to false to temporarily disable management of this table';

-- ----------------------------------------------------------------
-- pg_tsmanage_manage(regclass, text, interval, interval)
--
-- Register a partitioned table for automatic partition management.
-- ----------------------------------------------------------------
CREATE FUNCTION pg_tsmanage_manage(
    p_table_name       regclass,
    p_partition_column text,
    p_partition_interval interval,
    p_retention_interval interval DEFAULT NULL
)
RETURNS void
AS 'MODULE_PATHNAME', 'pg_tsmanage_manage'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION pg_tsmanage_manage(regclass, text, interval, interval) IS
'Register a partitioned table for automatic time-series partition management';

-- ----------------------------------------------------------------
-- pg_tsmanage_unmanage(regclass)
--
-- Remove a table from automatic partition management.
-- ----------------------------------------------------------------
CREATE FUNCTION pg_tsmanage_unmanage(
    p_table_name regclass
)
RETURNS void
AS 'MODULE_PATHNAME', 'pg_tsmanage_unmanage'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION pg_tsmanage_unmanage(regclass) IS
'Remove a table from automatic time-series partition management';

-- ----------------------------------------------------------------
-- pg_time_bucket(interval, timestamptz)
--
-- Truncate a timestamptz value to the start of its bucket.
-- ----------------------------------------------------------------
CREATE FUNCTION pg_time_bucket(
    bucket_width interval,
    ts           timestamptz
)
RETURNS timestamptz
AS 'MODULE_PATHNAME', 'pg_time_bucket_timestamptz'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

COMMENT ON FUNCTION pg_time_bucket(interval, timestamptz) IS
'Truncate a timestamptz to the start of its time bucket';

-- ----------------------------------------------------------------
-- pg_time_bucket(interval, timestamp)
--
-- Overload for timestamp without time zone.
-- ----------------------------------------------------------------
CREATE FUNCTION pg_time_bucket(
    bucket_width interval,
    ts           timestamp
)
RETURNS timestamp
AS 'MODULE_PATHNAME', 'pg_time_bucket_timestamp'
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

COMMENT ON FUNCTION pg_time_bucket(interval, timestamp) IS
'Truncate a timestamp to the start of its time bucket';

-- ----------------------------------------------------------------
-- pg_tsmanage_status()
--
-- Returns current partition status for all managed tables.
-- ----------------------------------------------------------------
CREATE FUNCTION pg_tsmanage_status(
    OUT table_name         text,
    OUT partition_column   text,
    OUT partition_interval interval,
    OUT retention_interval interval,
    OUT partition_count    int,
    OUT enabled            boolean
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_tsmanage_status'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION pg_tsmanage_status() IS
'Show partition management status for all configured time-series tables';

-- ----------------------------------------------------------------
-- pg_tsmanage_maintain_now()
--
-- Manually trigger partition maintenance from the current session.
-- ----------------------------------------------------------------
CREATE FUNCTION pg_tsmanage_maintain_now()
RETURNS void
AS 'MODULE_PATHNAME', 'pg_tsmanage_maintain_now'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION pg_tsmanage_maintain_now() IS
'Immediately run partition maintenance (create future partitions, drop expired ones)';
