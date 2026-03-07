/* contrib/alohadb_tiering/alohadb_tiering--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_tiering" to load this file. \quit

-- ----------------------------------------------------------------
-- alohadb_tiering_rules
--
-- Each row defines a tiering policy: partitions of parent_table
-- whose upper range boundary is older than age_threshold are
-- moved to target_tablespace.
-- ----------------------------------------------------------------
CREATE TABLE alohadb_tiering_rules (
    id                serial PRIMARY KEY,
    parent_table      regclass NOT NULL,
    age_threshold     interval NOT NULL,
    target_tablespace name     NOT NULL,
    enabled           boolean  DEFAULT true
);

COMMENT ON TABLE alohadb_tiering_rules IS
'Rules for automatic partition tiering based on time thresholds';

COMMENT ON COLUMN alohadb_tiering_rules.parent_table IS
'The partitioned (parent) table whose children may be moved';

COMMENT ON COLUMN alohadb_tiering_rules.age_threshold IS
'Move partitions whose upper range bound is older than this interval from now';

COMMENT ON COLUMN alohadb_tiering_rules.target_tablespace IS
'Tablespace to move cold partitions to';

COMMENT ON COLUMN alohadb_tiering_rules.enabled IS
'Set to false to temporarily disable this rule';

-- ----------------------------------------------------------------
-- alohadb_tiering_status()
--
-- Returns the background worker''s in-memory status for each rule
-- it has processed since startup.
-- ----------------------------------------------------------------
CREATE FUNCTION alohadb_tiering_status(
    OUT rule_id          int,
    OUT parent_table     text,
    OUT partitions_moved int,
    OUT last_check       timestamptz
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'alohadb_tiering_status'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION alohadb_tiering_status() IS
'Show tiering background worker status: rules processed, partitions moved, last check time';

-- ----------------------------------------------------------------
-- alohadb_tiering_check_now()
--
-- Manually trigger a tiering check from the current session.
-- This does NOT require the background worker to be running.
-- ----------------------------------------------------------------
CREATE FUNCTION alohadb_tiering_check_now()
RETURNS void
AS 'MODULE_PATHNAME', 'alohadb_tiering_check_now'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION alohadb_tiering_check_now() IS
'Immediately evaluate all enabled tiering rules and move qualifying partitions';
