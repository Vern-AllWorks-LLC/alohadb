/* contrib/alohadb_vault/alohadb_vault--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_vault" to load this file. \quit

-- ----------------------------------------------------------------
-- alohadb_vault_admin role
--
-- Members of this role may access the secrets store.
-- ----------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'alohadb_vault_admin'
    ) THEN
        CREATE ROLE alohadb_vault_admin NOLOGIN;
    END IF;
END
$$;

-- ----------------------------------------------------------------
-- alohadb_vault_secrets
--
-- Stores encrypted secret values keyed by a text identifier.
-- ----------------------------------------------------------------
CREATE TABLE alohadb_vault_secrets (
    key          text        PRIMARY KEY,
    value        bytea       NOT NULL,
    description  text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE alohadb_vault_secrets IS
'Encrypted secrets store for AlohaDB; values are PGP-symmetrically encrypted';

-- ----------------------------------------------------------------
-- alohadb_vault_audit
--
-- Audit trail for all vault operations.
-- ----------------------------------------------------------------
CREATE TABLE alohadb_vault_audit (
    id            bigserial   PRIMARY KEY,
    operation     text        NOT NULL,
    key_name      text        NOT NULL,
    performed_by  text        NOT NULL DEFAULT current_user,
    performed_at  timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE alohadb_vault_audit IS
'Audit log for alohadb_vault operations (store, fetch, delete, rotate)';

-- ----------------------------------------------------------------
-- Row-Level Security on alohadb_vault_secrets
--
-- Only superusers or members of alohadb_vault_admin may access rows.
-- ----------------------------------------------------------------
ALTER TABLE alohadb_vault_secrets ENABLE ROW LEVEL SECURITY;

CREATE POLICY vault_secrets_policy ON alohadb_vault_secrets
    USING (
        pg_has_role(current_user, 'alohadb_vault_admin', 'MEMBER')
        OR
        (SELECT usesuper FROM pg_catalog.pg_user WHERE usename = current_user)
    );

-- Grant table-level privileges to vault admins
GRANT SELECT, INSERT, UPDATE, DELETE ON alohadb_vault_secrets TO alohadb_vault_admin;
GRANT SELECT, INSERT ON alohadb_vault_audit TO alohadb_vault_admin;
GRANT USAGE, SELECT ON SEQUENCE alohadb_vault_audit_id_seq TO alohadb_vault_admin;

-- ----------------------------------------------------------------
-- SQL-callable C functions
-- ----------------------------------------------------------------

CREATE FUNCTION alohadb_vault_store(
    p_key   text,
    p_value text,
    p_desc  text DEFAULT NULL
)
RETURNS void
AS 'MODULE_PATHNAME', 'alohadb_vault_store'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION alohadb_vault_store(text, text, text) IS
'Store or update an encrypted secret in the vault';

CREATE FUNCTION alohadb_vault_fetch(
    p_key text
)
RETURNS text
AS 'MODULE_PATHNAME', 'alohadb_vault_fetch'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION alohadb_vault_fetch(text) IS
'Fetch and decrypt a secret from the vault by key';

CREATE FUNCTION alohadb_vault_delete(
    p_key text
)
RETURNS void
AS 'MODULE_PATHNAME', 'alohadb_vault_delete'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION alohadb_vault_delete(text) IS
'Delete a secret from the vault by key';

CREATE FUNCTION alohadb_vault_list(
    OUT key         text,
    OUT description text,
    OUT created_at  timestamptz,
    OUT updated_at  timestamptz
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'alohadb_vault_list'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION alohadb_vault_list() IS
'List all secret keys with their metadata (does not return decrypted values)';

CREATE FUNCTION alohadb_vault_rotate_key(
    p_old_passphrase text,
    p_new_passphrase text
)
RETURNS int
AS 'MODULE_PATHNAME', 'alohadb_vault_rotate_key'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION alohadb_vault_rotate_key(text, text) IS
'Re-encrypt all secrets with a new passphrase; returns count of rotated secrets';
