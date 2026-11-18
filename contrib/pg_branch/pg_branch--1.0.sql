/* contrib/pg_branch/pg_branch--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_branch" to load this file. \quit

-- =====================================================================
-- Metadata table: tracks all branches created by this extension
-- =====================================================================

CREATE TABLE pg_branches (
    name        text PRIMARY KEY,
    parent_lsn  pg_lsn,
    port        int NOT NULL,
    data_dir    text NOT NULL,
    status      text DEFAULT 'running',
    created_at  timestamptz DEFAULT now()
);

COMMENT ON TABLE pg_branches IS
    'Tracks database branches created by the pg_branch extension.';

-- Restrict direct modification to superusers
REVOKE ALL ON pg_branches FROM PUBLIC;
GRANT SELECT ON pg_branches TO PUBLIC;

-- =====================================================================
-- pg_create_branch: create a new database branch
-- =====================================================================

CREATE FUNCTION pg_create_branch(
    name        text,
    from_lsn    pg_lsn DEFAULT NULL,
    OUT branch_name text,
    OUT port int,
    OUT data_dir text
)
RETURNS record
AS 'MODULE_PATHNAME', 'pg_create_branch'
LANGUAGE C VOLATILE CALLED ON NULL INPUT;

COMMENT ON FUNCTION pg_create_branch(text, pg_lsn) IS
    'Create a lightweight database branch from the current server state, '
    'optionally targeting a specific WAL LSN for point-in-time recovery.';

-- =====================================================================
-- pg_list_branches: enumerate all branches
-- =====================================================================

CREATE FUNCTION pg_list_branches()
RETURNS TABLE(
    name        text,
    lsn         pg_lsn,
    port        int,
    data_dir    text,
    status      text,
    created_at  timestamptz
)
AS 'MODULE_PATHNAME', 'pg_list_branches'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION pg_list_branches() IS
    'List all database branches with their metadata and current status.';

-- =====================================================================
-- pg_drop_branch: stop and remove a branch
-- =====================================================================

CREATE FUNCTION pg_drop_branch(name text)
RETURNS void
AS 'MODULE_PATHNAME', 'pg_drop_branch'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION pg_drop_branch(text) IS
    'Stop a running branch postmaster and remove its data directory and metadata.';
