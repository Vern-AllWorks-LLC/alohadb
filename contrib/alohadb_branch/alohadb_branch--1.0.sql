/* contrib/alohadb_branch/alohadb_branch--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_branch" to load this file. \quit

-- =====================================================================
-- Metadata table: tracks all branches created by this extension
-- =====================================================================

CREATE TABLE alohadb_branches (
    name        text PRIMARY KEY,
    parent_lsn  pg_lsn,
    port        int NOT NULL,
    data_dir    text NOT NULL,
    status      text DEFAULT 'running',
    created_at  timestamptz DEFAULT now()
);

COMMENT ON TABLE alohadb_branches IS
    'Tracks database branches created by the alohadb_branch extension.';

-- Restrict direct modification to superusers
REVOKE ALL ON alohadb_branches FROM PUBLIC;
GRANT SELECT ON alohadb_branches TO PUBLIC;

-- =====================================================================
-- alohadb_create_branch: create a new database branch
-- =====================================================================

CREATE FUNCTION alohadb_create_branch(
    name        text,
    from_lsn    pg_lsn DEFAULT NULL,
    OUT branch_name text,
    OUT port int,
    OUT data_dir text
)
RETURNS record
AS 'MODULE_PATHNAME', 'alohadb_create_branch'
LANGUAGE C VOLATILE CALLED ON NULL INPUT;

COMMENT ON FUNCTION alohadb_create_branch(text, pg_lsn) IS
    'Create a lightweight database branch from the current server state, '
    'optionally targeting a specific WAL LSN for point-in-time recovery.';

-- =====================================================================
-- alohadb_list_branches: enumerate all branches
-- =====================================================================

CREATE FUNCTION alohadb_list_branches()
RETURNS TABLE(
    name        text,
    lsn         pg_lsn,
    port        int,
    data_dir    text,
    status      text,
    created_at  timestamptz
)
AS 'MODULE_PATHNAME', 'alohadb_list_branches'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION alohadb_list_branches() IS
    'List all database branches with their metadata and current status.';

-- =====================================================================
-- alohadb_drop_branch: stop and remove a branch
-- =====================================================================

CREATE FUNCTION alohadb_drop_branch(name text)
RETURNS void
AS 'MODULE_PATHNAME', 'alohadb_drop_branch'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION alohadb_drop_branch(text) IS
    'Stop a running branch postmaster and remove its data directory and metadata.';
