\echo Use "CREATE EXTENSION alohadb_schema_change" to load this file.\quit

CREATE TABLE alohadb_schema_changes (
    change_id bigserial PRIMARY KEY,
    ddl_statement text NOT NULL,
    table_name text NOT NULL,
    status text NOT NULL DEFAULT 'running',
    started_at timestamptz DEFAULT now(),
    completed_at timestamptz
);

CREATE FUNCTION online_alter_table(ddl_statement text)
RETURNS bigint
LANGUAGE C
AS 'MODULE_PATHNAME', 'online_alter_table';

CREATE FUNCTION online_alter_status(change_id bigint DEFAULT NULL)
RETURNS TABLE(
    id text,
    ddl_statement text,
    table_name text,
    status text,
    started_at text,
    completed_at text
)
LANGUAGE C
AS 'MODULE_PATHNAME', 'online_alter_status';

CREATE FUNCTION online_alter_cancel(change_id bigint)
RETURNS bool
LANGUAGE C
AS 'MODULE_PATHNAME', 'online_alter_cancel';

COMMENT ON FUNCTION online_alter_table(text) IS
    'Perform DDL online using copy-swap technique (like pg_repack)';
