/* contrib/alohadb_temporal/alohadb_temporal--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_temporal" to load this file. \quit

-- ----------------------------------------------------------------
-- alohadb_temporal_tables
--
-- Registry of tables with system versioning enabled.
-- ----------------------------------------------------------------
CREATE TABLE alohadb_temporal_tables (
    table_name    regclass    PRIMARY KEY,
    history_table text        NOT NULL,
    enabled_at    timestamptz DEFAULT now()
);

COMMENT ON TABLE alohadb_temporal_tables IS
'Registry of tables with SQL:2011 system versioning enabled';

COMMENT ON COLUMN alohadb_temporal_tables.table_name IS
'OID of the base table with system versioning';

COMMENT ON COLUMN alohadb_temporal_tables.history_table IS
'Name of the history table that stores old row versions';

COMMENT ON COLUMN alohadb_temporal_tables.enabled_at IS
'Timestamp when system versioning was enabled on this table';

-- ----------------------------------------------------------------
-- alohadb_temporal_versioning_trigger()
--
-- Row-level BEFORE trigger that copies old row versions to the
-- history table on UPDATE and DELETE.
-- ----------------------------------------------------------------
CREATE FUNCTION alohadb_temporal_versioning_trigger()
RETURNS trigger
AS 'MODULE_PATHNAME', 'alohadb_temporal_versioning_trigger'
LANGUAGE C;

COMMENT ON FUNCTION alohadb_temporal_versioning_trigger() IS
'Trigger function that copies old row versions to the history table';

-- ----------------------------------------------------------------
-- alohadb_enable_system_versioning(regclass)
--
-- Enable system versioning on the specified table: add row_start
-- and row_end columns, create the history table, install the
-- versioning trigger, and register in alohadb_temporal_tables.
-- ----------------------------------------------------------------
CREATE FUNCTION alohadb_enable_system_versioning(table_name regclass)
RETURNS void
AS 'MODULE_PATHNAME', 'alohadb_enable_system_versioning'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION alohadb_enable_system_versioning(regclass) IS
'Enable SQL:2011 system versioning on the specified table';

-- ----------------------------------------------------------------
-- alohadb_disable_system_versioning(regclass)
--
-- Disable system versioning: drop the trigger, remove the
-- registration, optionally drop the history table, and remove the
-- row_start/row_end columns.
-- ----------------------------------------------------------------
CREATE FUNCTION alohadb_disable_system_versioning(table_name regclass)
RETURNS void
AS 'MODULE_PATHNAME', 'alohadb_disable_system_versioning'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION alohadb_disable_system_versioning(regclass) IS
'Disable SQL:2011 system versioning on the specified table';

-- ----------------------------------------------------------------
-- alohadb_as_of(regclass, timestamptz)
--
-- Time-travel query: returns all rows as they existed at the given
-- point in time, combining current and history tables.
-- ----------------------------------------------------------------
CREATE FUNCTION alohadb_as_of(table_name regclass, ts timestamptz)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'alohadb_as_of'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION alohadb_as_of(regclass, timestamptz) IS
'Return all rows as they existed at the specified point in time';

-- ----------------------------------------------------------------
-- alohadb_versions_between(regclass, timestamptz, timestamptz)
--
-- Return all row versions that were valid at any point within the
-- specified time range.
-- ----------------------------------------------------------------
CREATE FUNCTION alohadb_versions_between(
    table_name regclass,
    ts_start   timestamptz,
    ts_end     timestamptz
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'alohadb_versions_between'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION alohadb_versions_between(regclass, timestamptz, timestamptz) IS
'Return all row versions valid within the specified time range';

-- ----------------------------------------------------------------
-- alohadb_temporal_status()
--
-- Show all tables with system versioning enabled.
-- ----------------------------------------------------------------
CREATE FUNCTION alohadb_temporal_status(
    OUT table_name    text,
    OUT history_table text,
    OUT enabled_at    timestamptz
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'alohadb_temporal_status'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION alohadb_temporal_status() IS
'Show all tables with SQL:2011 system versioning enabled';
