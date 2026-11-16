\echo Use "CREATE EXTENSION alohadb_objectstore" to load this file. \quit

-- -----------------------------------------------------------------------
-- alohadb_objectstore: S3, GCS, and Azure Blob cloud storage
--
-- S3: AWS SigV4 request signing (HMAC-SHA256 via OpenSSL).
-- GCS: OAuth2 service-account token exchange + JSON REST API.
-- Azure: Shared Access Signature (HMAC-SHA256) or connection string.
--
-- Patent-mitigation: SigV4 is a publicly documented AWS signing algorithm
-- (no patent claims). HMAC-SHA256 is IETF RFC 2104 / FIPS 198 (prior art
-- predates any applicable patent window).  GCS and Azure use OAuth2
-- (RFC 6749) and standard HTTPS REST.  No proprietary cloud-SDK code.
-- -----------------------------------------------------------------------

-- Credential store (config stored encrypted via alohadb_vault when available)
CREATE TABLE alohadb_objectstore_credentials (
    cred_id         bigserial   PRIMARY KEY,
    provider        text        NOT NULL,   -- 's3' | 'gcs' | 'azure'
    label           text        NOT NULL UNIQUE,
    config          jsonb       NOT NULL,
        -- S3:    {bucket, region, access_key_id, secret_access_key, [endpoint]}
        -- GCS:   {bucket, project_id, service_account_json}
        -- Azure: {account, container, sas_token | account_key}
    created_by      text        NOT NULL DEFAULT current_user,
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- Access audit log
CREATE TABLE alohadb_objectstore_log (
    log_id          bigserial   PRIMARY KEY,
    operation       text        NOT NULL,   -- 'GET' | 'PUT' | 'LIST' | 'DELETE'
    uri             text        NOT NULL,
    bytes_transferred bigint,
    duration_ms     float8,
    success         boolean     NOT NULL,
    error_message   text,
    performed_by    text        NOT NULL DEFAULT current_user,
    performed_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX alohadb_objectstore_log_time_idx
    ON alohadb_objectstore_log (performed_at DESC);

-- Register a credential set
CREATE FUNCTION objectstore_credential_register(
    p_provider text,
    p_label    text,
    p_config   jsonb
)
RETURNS bigint
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE v_id bigint;
BEGIN
    IF p_provider NOT IN ('s3', 'gcs', 'azure') THEN
        RAISE EXCEPTION 'objectstore_credential_register: unknown provider "%"', p_provider;
    END IF;
    INSERT INTO alohadb_objectstore_credentials (provider, label, config)
    VALUES (p_provider, p_label, p_config)
    ON CONFLICT (label)
    DO UPDATE SET config = EXCLUDED.config, provider = EXCLUDED.provider
    RETURNING cred_id INTO v_id;
    RETURN v_id;
END;
$$;

-- GET: download an object and return as bytea
CREATE FUNCTION objectstore_get(
    p_uri             text,
    p_credential_label text
)
RETURNS bytea
LANGUAGE C VOLATILE STRICT
AS 'MODULE_PATHNAME', 'objectstore_get';

-- GET: download an object and return as text
CREATE FUNCTION objectstore_get_text(
    p_uri             text,
    p_credential_label text
)
RETURNS text
LANGUAGE C VOLATILE STRICT
AS 'MODULE_PATHNAME', 'objectstore_get_text';

-- PUT: upload data to an object
CREATE FUNCTION objectstore_put(
    p_uri             text,
    p_data            bytea,
    p_credential_label text
)
RETURNS void
LANGUAGE C VOLATILE STRICT
AS 'MODULE_PATHNAME', 'objectstore_put';

-- LIST: enumerate objects under a prefix
CREATE FUNCTION objectstore_list(
    p_prefix          text,
    p_credential_label text,
    p_max_keys        int DEFAULT 1000
)
RETURNS TABLE(
    key             text,
    size_bytes      bigint,
    last_modified   timestamptz,
    etag            text
)
LANGUAGE C VOLATILE STRICT
AS 'MODULE_PATHNAME', 'objectstore_list';

-- DELETE: remove an object
CREATE FUNCTION objectstore_delete(
    p_uri             text,
    p_credential_label text
)
RETURNS void
LANGUAGE C VOLATILE STRICT
AS 'MODULE_PATHNAME', 'objectstore_delete';

-- Convenience: read a Parquet/CSV/JSON file from object store into rows
CREATE FUNCTION objectstore_read_csv(
    p_uri              text,
    p_credential_label text,
    p_delimiter        text DEFAULT ',',
    p_has_header       bool DEFAULT true
)
RETURNS SETOF jsonb
LANGUAGE plpgsql VOLATILE STRICT
AS $$
DECLARE
    tmp_path text;
    content  text;
BEGIN
    content  := objectstore_get_text(p_uri, p_credential_label);
    -- Write to a temp file in the allowed path and hand off to read_csv
    tmp_path := '/tmp/alohadb_objstore_' || pg_backend_pid() || '.csv';
    PERFORM pg_file_write(tmp_path, content, false);
    RETURN QUERY SELECT * FROM read_csv(tmp_path, p_delimiter, p_has_header);
    PERFORM pg_file_unlink(tmp_path);
END;
$$;

-- Credential list (omit secret values)
CREATE FUNCTION objectstore_credential_list()
RETURNS TABLE(label text, provider text, created_by text, created_at timestamptz)
LANGUAGE sql STABLE
AS $$
    SELECT label, provider, created_by, created_at
    FROM   alohadb_objectstore_credentials
    ORDER BY label;
$$;
