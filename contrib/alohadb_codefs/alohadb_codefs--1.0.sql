/* contrib/alohadb_codefs/alohadb_codefs--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_codefs" to load this file. \quit

-- ================================================================
-- ROLE for agent identification
-- ================================================================
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'codefs_agent') THEN
        CREATE ROLE codefs_agent NOLOGIN;
    END IF;
END
$$;

-- ================================================================
-- TABLE: codefs_files -- the virtual filesystem
-- ================================================================
CREATE TABLE codefs_files (
    file_id     bigserial PRIMARY KEY,
    path        text NOT NULL,
    content     text,
    size_bytes  bigint NOT NULL DEFAULT 0,
    mime_type   text DEFAULT 'text/plain',
    is_binary   boolean NOT NULL DEFAULT false,
    checksum    text,
    branch      text NOT NULL DEFAULT 'main',
    created_by  text NOT NULL DEFAULT current_user,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    is_deleted  boolean NOT NULL DEFAULT false,
    metadata    jsonb DEFAULT '{}',
    UNIQUE(path, branch)
);

CREATE INDEX ON codefs_files (branch, path);
CREATE INDEX ON codefs_files (created_by);
CREATE INDEX ON codefs_files USING gin (metadata);

COMMENT ON TABLE codefs_files IS
    'Virtual filesystem: each row is a file with path, content, and metadata.';

-- ================================================================
-- TABLE: codefs_versions -- version history
-- ================================================================
CREATE TABLE codefs_versions (
    version_id      bigserial PRIMARY KEY,
    file_id         bigint NOT NULL REFERENCES codefs_files(file_id) ON DELETE CASCADE,
    version_num     int NOT NULL,
    content         text,
    checksum        text,
    size_bytes      bigint NOT NULL DEFAULT 0,
    commit_msg      text,
    committed_by    text NOT NULL DEFAULT current_user,
    committed_at    timestamptz NOT NULL DEFAULT now(),
    parent_version_id bigint REFERENCES codefs_versions(version_id),
    is_archived     boolean NOT NULL DEFAULT false,
    archive_path    text,
    UNIQUE(file_id, version_num)
);

CREATE INDEX ON codefs_versions (file_id, version_num DESC);
CREATE INDEX ON codefs_versions (committed_at);

COMMENT ON TABLE codefs_versions IS
    'Version history for codefs files. Each write creates a new version.';

-- ================================================================
-- TABLE: codefs_branches -- branch registry
-- ================================================================
CREATE TABLE codefs_branches (
    branch_id       serial PRIMARY KEY,
    name            text NOT NULL UNIQUE,
    parent_branch   text,
    created_by      text NOT NULL DEFAULT current_user,
    created_at      timestamptz NOT NULL DEFAULT now(),
    is_agent        boolean NOT NULL DEFAULT false,
    agent_name      text,
    status          text NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active','merged','abandoned','locked')),
    merged_at       timestamptz,
    merged_by       text
);

COMMENT ON TABLE codefs_branches IS
    'Registry of codefs branches. Agent branches require human merge by default.';

-- Insert default main branch
INSERT INTO codefs_branches (name, parent_branch, status)
VALUES ('main', NULL, 'active');

-- ================================================================
-- TABLE: codefs_locks -- advisory lock tracking
-- ================================================================
CREATE TABLE codefs_locks (
    lock_id         bigserial PRIMARY KEY,
    file_id         bigint NOT NULL REFERENCES codefs_files(file_id) ON DELETE CASCADE,
    branch          text NOT NULL,
    locked_by       text NOT NULL DEFAULT current_user,
    locked_at       timestamptz NOT NULL DEFAULT now(),
    lock_timeout    interval NOT NULL DEFAULT '5 minutes',
    UNIQUE(file_id, branch)
);

COMMENT ON TABLE codefs_locks IS
    'Tracks active file locks. Expired locks are cleaned by the policy worker.';

-- ================================================================
-- TABLE: codefs_policies -- human-editable only (superuser)
-- ================================================================
CREATE TABLE codefs_policies (
    policy_id               serial PRIMARY KEY,
    policy_name             text NOT NULL UNIQUE DEFAULT 'default',
    store_binaries_in_db    boolean NOT NULL DEFAULT false,
    max_versions            int NOT NULL DEFAULT 50,
    archive_storage_path    text NOT NULL DEFAULT '/var/lib/alohadb/codefs/archive',
    binary_storage_path     text NOT NULL DEFAULT '/var/lib/alohadb/codefs/binaries',
    require_human_merge     boolean NOT NULL DEFAULT true,
    agent_write_scope       text DEFAULT '*',
    max_file_size_bytes     bigint NOT NULL DEFAULT 10485760,
    sync_interval_secs      int NOT NULL DEFAULT 30,
    sync_base_path          text DEFAULT '/var/lib/alohadb/codefs/workspace',
    auto_branch_on_agent    boolean NOT NULL DEFAULT true,
    retention_days          int NOT NULL DEFAULT 365,
    compress_archived       boolean NOT NULL DEFAULT true,
    allowed_extensions      text DEFAULT '*',
    denied_extensions       text DEFAULT '.exe,.dll,.so,.o,.a,.dylib',
    max_total_size_mb       int NOT NULL DEFAULT 10240,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE codefs_policies IS
    'CodeFS policy settings. Only superusers can modify this table.';

COMMENT ON COLUMN codefs_policies.store_binaries_in_db IS
    'If false, large binaries are offloaded to binary_storage_path on disk.';
COMMENT ON COLUMN codefs_policies.max_versions IS
    'Maximum versions to keep in DB per file. Older versions are archived to disk.';
COMMENT ON COLUMN codefs_policies.archive_storage_path IS
    'Disk path for compressed archived versions.';
COMMENT ON COLUMN codefs_policies.binary_storage_path IS
    'Disk path for large binary files offloaded from DB.';
COMMENT ON COLUMN codefs_policies.require_human_merge IS
    'If true, agents cannot merge branches; only human users can.';
COMMENT ON COLUMN codefs_policies.agent_write_scope IS
    'Comma-separated glob patterns restricting agent write paths. * = allow all.';
COMMENT ON COLUMN codefs_policies.auto_branch_on_agent IS
    'If true, agents writing to main are auto-redirected to agent/<username> branch.';
COMMENT ON COLUMN codefs_policies.allowed_extensions IS
    'Comma-separated file extensions allowed. * = allow all.';
COMMENT ON COLUMN codefs_policies.denied_extensions IS
    'Comma-separated file extensions denied (overrides allowed).';
COMMENT ON COLUMN codefs_policies.max_total_size_mb IS
    'Maximum total size of all files in codefs (MB). 0 = unlimited.';

-- Restrict to superuser only
REVOKE ALL ON codefs_policies FROM PUBLIC;
GRANT SELECT ON codefs_policies TO PUBLIC;

-- Insert default policy
INSERT INTO codefs_policies (policy_name) VALUES ('default');

-- Trigger to enforce superuser-only updates and set updated_at
CREATE OR REPLACE FUNCTION codefs_policies_check_superuser()
RETURNS trigger AS $$
BEGIN
    IF NOT (SELECT usesuper FROM pg_user WHERE usename = current_user) THEN
        RAISE EXCEPTION 'codefs: only superusers can modify codefs_policies';
    END IF;
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER codefs_policies_superuser_check
    BEFORE INSERT OR UPDATE OR DELETE ON codefs_policies
    FOR EACH ROW EXECUTE FUNCTION codefs_policies_check_superuser();

-- ================================================================
-- TABLE: codefs_sync_status -- sync tracking
-- ================================================================
CREATE TABLE codefs_sync_status (
    sync_id         bigserial PRIMARY KEY,
    direction       text NOT NULL CHECK (direction IN ('db_to_disk','disk_to_db')),
    file_path       text NOT NULL,
    branch          text NOT NULL DEFAULT 'main',
    status          text NOT NULL CHECK (status IN ('pending','synced','error','conflict')),
    last_synced_at  timestamptz,
    error_message   text,
    disk_checksum   text,
    db_checksum     text
);

COMMENT ON TABLE codefs_sync_status IS
    'Tracks bidirectional sync status between DB and disk filesystem.';

-- ================================================================
-- ROW LEVEL SECURITY
-- ================================================================
ALTER TABLE codefs_files ENABLE ROW LEVEL SECURITY;

-- Everyone can read all files
CREATE POLICY codefs_files_select ON codefs_files
    FOR SELECT USING (true);

-- Non-agents can write anywhere; agents can only write to non-main branches
CREATE POLICY codefs_files_insert ON codefs_files
    FOR INSERT WITH CHECK (
        NOT EXISTS(SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.roleid WHERE r.rolname = 'codefs_agent' AND m.member = (SELECT oid FROM pg_roles WHERE rolname = current_user))
        OR branch != 'main'
    );

CREATE POLICY codefs_files_update ON codefs_files
    FOR UPDATE USING (
        NOT EXISTS(SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.roleid WHERE r.rolname = 'codefs_agent' AND m.member = (SELECT oid FROM pg_roles WHERE rolname = current_user))
        OR (branch != 'main')
    );

CREATE POLICY codefs_files_delete ON codefs_files
    FOR DELETE USING (
        NOT EXISTS(SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.roleid WHERE r.rolname = 'codefs_agent' AND m.member = (SELECT oid FROM pg_roles WHERE rolname = current_user))
        OR branch != 'main'
    );

-- ================================================================
-- FUNCTIONS: File operations
-- ================================================================

CREATE FUNCTION codefs_write(
    path text,
    content text,
    branch text DEFAULT 'main',
    commit_msg text DEFAULT NULL
)
RETURNS bigint
AS 'MODULE_PATHNAME', 'codefs_write'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION codefs_write IS
    'Write a file to the virtual filesystem. Content is stored as data only, never executed. '
    'Returns the file_id.';

CREATE FUNCTION codefs_read(
    path text,
    branch text DEFAULT 'main',
    version int DEFAULT NULL
)
RETURNS text
AS 'MODULE_PATHNAME', 'codefs_read'
LANGUAGE C STABLE;

COMMENT ON FUNCTION codefs_read IS
    'Read file content. Optionally specify a version number for historical content.';

CREATE FUNCTION codefs_delete(
    path text,
    branch text DEFAULT 'main',
    commit_msg text DEFAULT NULL
)
RETURNS boolean
AS 'MODULE_PATHNAME', 'codefs_delete'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION codefs_delete IS
    'Soft-delete a file (preserves version history). Returns true if file existed.';

CREATE FUNCTION codefs_rename(
    old_path text,
    new_path text,
    branch text DEFAULT 'main'
)
RETURNS boolean
AS 'MODULE_PATHNAME', 'codefs_rename'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION codefs_rename IS
    'Rename/move a file within the virtual filesystem.';

CREATE FUNCTION codefs_list(
    branch text DEFAULT 'main',
    pattern text DEFAULT '*'
)
RETURNS TABLE(
    path text,
    size_bytes bigint,
    mime_type text,
    updated_at timestamptz,
    created_by text
)
AS 'MODULE_PATHNAME', 'codefs_list'
LANGUAGE C STABLE;

COMMENT ON FUNCTION codefs_list IS
    'List files on a branch. Supports glob patterns (e.g. src/*.py).';

-- ================================================================
-- FUNCTIONS: Versioning
-- ================================================================

CREATE FUNCTION codefs_commit(
    branch text,
    message text,
    paths text[] DEFAULT NULL
)
RETURNS bigint
AS 'MODULE_PATHNAME', 'codefs_commit'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION codefs_commit IS
    'Stamp uncommitted versions with a commit message. Returns count of versions stamped.';

CREATE FUNCTION codefs_history(
    path text,
    branch text DEFAULT 'main',
    max_entries int DEFAULT 20
)
RETURNS TABLE(
    version_num int,
    commit_msg text,
    committed_by text,
    committed_at timestamptz,
    size_bytes bigint
)
AS 'MODULE_PATHNAME', 'codefs_history'
LANGUAGE C STABLE;

COMMENT ON FUNCTION codefs_history IS
    'Show version history for a file, most recent first.';

CREATE FUNCTION codefs_diff(
    path text,
    version_a int,
    version_b int,
    branch text DEFAULT 'main'
)
RETURNS text
AS 'MODULE_PATHNAME', 'codefs_diff'
LANGUAGE C STABLE;

COMMENT ON FUNCTION codefs_diff IS
    'Show line-by-line diff between two versions of a file.';

CREATE FUNCTION codefs_revert(
    path text,
    to_version int,
    branch text DEFAULT 'main'
)
RETURNS boolean
AS 'MODULE_PATHNAME', 'codefs_revert'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION codefs_revert IS
    'Revert a file to a previous version, creating a new version entry.';

-- ================================================================
-- FUNCTIONS: Branching
-- ================================================================

CREATE FUNCTION codefs_branch_create(
    name text,
    from_branch text DEFAULT 'main',
    agent_name text DEFAULT NULL
)
RETURNS text
AS 'MODULE_PATHNAME', 'codefs_branch_create'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION codefs_branch_create IS
    'Create a new branch by copying files from an existing branch.';

CREATE FUNCTION codefs_branch_list()
RETURNS TABLE(
    name text,
    parent_branch text,
    created_by text,
    created_at timestamptz,
    is_agent boolean,
    status text
)
AS 'MODULE_PATHNAME', 'codefs_branch_list'
LANGUAGE C STABLE;

COMMENT ON FUNCTION codefs_branch_list IS
    'List all branches with their status and metadata.';

CREATE FUNCTION codefs_branch_merge(
    source_branch text,
    target_branch text DEFAULT 'main'
)
RETURNS TABLE(
    path text,
    action text,
    status text
)
AS 'MODULE_PATHNAME', 'codefs_branch_merge'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION codefs_branch_merge IS
    'Merge a source branch into a target branch. '
    'Respects require_human_merge policy (agents cannot merge if enabled).';

CREATE FUNCTION codefs_branch_delete(name text)
RETURNS boolean
AS 'MODULE_PATHNAME', 'codefs_branch_delete'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION codefs_branch_delete IS
    'Abandon a branch (soft-delete). Cannot delete main.';

-- ================================================================
-- FUNCTIONS: Locking
-- ================================================================

CREATE FUNCTION codefs_lock_file(
    path text,
    branch text DEFAULT 'main',
    timeout_secs int DEFAULT 5
)
RETURNS boolean
AS 'MODULE_PATHNAME', 'codefs_lock_file'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION codefs_lock_file IS
    'Acquire an advisory lock on a file. Returns false if lock unavailable.';

CREATE FUNCTION codefs_unlock_file(
    path text,
    branch text DEFAULT 'main'
)
RETURNS boolean
AS 'MODULE_PATHNAME', 'codefs_unlock_file'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION codefs_unlock_file IS
    'Release an advisory lock on a file.';

-- ================================================================
-- FUNCTIONS: Search & Status
-- ================================================================

CREATE FUNCTION codefs_search(
    pattern text,
    branch text DEFAULT 'main',
    max_results int DEFAULT 100
)
RETURNS TABLE(
    path text,
    line_num int,
    line_text text
)
AS 'MODULE_PATHNAME', 'codefs_search'
LANGUAGE C STABLE;

COMMENT ON FUNCTION codefs_search IS
    'Search file content by regex pattern. Returns matching lines with file path and line number.';

CREATE FUNCTION codefs_status()
RETURNS TABLE(
    setting_name text,
    setting_value text
)
AS 'MODULE_PATHNAME', 'codefs_status'
LANGUAGE C STABLE;

COMMENT ON FUNCTION codefs_status IS
    'Show codefs worker status: sync active, policy active, last run times, counters.';

-- ================================================================
-- FUNCTIONS: Export / Import
-- ================================================================

CREATE FUNCTION codefs_export_to_disk(
    branch text DEFAULT 'main',
    target_dir text DEFAULT NULL
)
RETURNS TABLE(
    path text,
    status text
)
AS 'MODULE_PATHNAME', 'codefs_export_to_disk'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION codefs_export_to_disk IS
    'Export all files on a branch to a disk directory.';

CREATE FUNCTION codefs_import_from_disk(
    source_dir text,
    branch text DEFAULT 'main'
)
RETURNS TABLE(
    path text,
    status text
)
AS 'MODULE_PATHNAME', 'codefs_import_from_disk'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION codefs_import_from_disk IS
    'Import files from a disk directory into codefs.';
