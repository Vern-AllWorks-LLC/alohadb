/* contrib/pg_vault/pg_vault--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_vault" to load this file. \quit

-- ----------------------------------------------------------------
-- vault_admin role
--
-- Members of this role may access the secrets store.
-- ----------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'vault_admin'
    ) THEN
        CREATE ROLE vault_admin NOLOGIN;
    END IF;
END
$$;

-- ----------------------------------------------------------------
-- pg_vault_secrets
--
-- Stores encrypted secret values keyed by a text identifier.
-- ----------------------------------------------------------------
CREATE TABLE pg_vault_secrets (
    key          text        PRIMARY KEY,
    value        bytea       NOT NULL,
    description  text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE pg_vault_secrets IS
'Encrypted secrets store for AlohaDB; values are PGP-symmetrically encrypted';

-- ----------------------------------------------------------------
-- pg_vault_audit
--
-- Audit trail for all vault operations.
-- ----------------------------------------------------------------
CREATE TABLE pg_vault_audit (
    id            bigserial   PRIMARY KEY,
    operation     text        NOT NULL,
    key_name      text        NOT NULL,
    performed_by  text        NOT NULL DEFAULT current_user,
    performed_at  timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE pg_vault_audit IS
'Audit log for pg_vault operations (store, fetch, delete, rotate)';

-- ----------------------------------------------------------------
-- Row-Level Security on pg_vault_secrets
--
-- Only superusers or members of vault_admin may access rows.
-- ----------------------------------------------------------------
ALTER TABLE pg_vault_secrets ENABLE ROW LEVEL SECURITY;

CREATE POLICY vault_secrets_policy ON pg_vault_secrets
    USING (
        pg_has_role(current_user, 'vault_admin', 'MEMBER')
        OR
        (SELECT usesuper FROM pg_catalog.pg_user WHERE usename = current_user)
    );

-- Grant table-level privileges to vault admins
GRANT SELECT, INSERT, UPDATE, DELETE ON pg_vault_secrets TO vault_admin;
GRANT SELECT, INSERT ON pg_vault_audit TO vault_admin;
GRANT USAGE, SELECT ON SEQUENCE pg_vault_audit_id_seq TO vault_admin;

-- ----------------------------------------------------------------
-- SQL-callable C functions
-- ----------------------------------------------------------------

CREATE FUNCTION pg_vault_store(
    p_key   text,
    p_value text,
    p_desc  text DEFAULT NULL
)
RETURNS void
AS 'MODULE_PATHNAME', 'pg_vault_store'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION pg_vault_store(text, text, text) IS
'Store or update an encrypted secret in the vault';

CREATE FUNCTION pg_vault_fetch(
    p_key text
)
RETURNS text
AS 'MODULE_PATHNAME', 'pg_vault_fetch'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION pg_vault_fetch(text) IS
'Fetch and decrypt a secret from the vault by key';

CREATE FUNCTION pg_vault_delete(
    p_key text
)
RETURNS void
AS 'MODULE_PATHNAME', 'pg_vault_delete'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION pg_vault_delete(text) IS
'Delete a secret from the vault by key';

CREATE FUNCTION pg_vault_list(
    OUT key         text,
    OUT description text,
    OUT created_at  timestamptz,
    OUT updated_at  timestamptz
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_vault_list'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION pg_vault_list() IS
'List all secret keys with their metadata (does not return decrypted values)';

CREATE FUNCTION pg_vault_rotate_key(
    p_old_passphrase text,
    p_new_passphrase text
)
RETURNS int
AS 'MODULE_PATHNAME', 'pg_vault_rotate_key'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION pg_vault_rotate_key(text, text) IS
'Re-encrypt all secrets with a new passphrase; returns count of rotated secrets';
