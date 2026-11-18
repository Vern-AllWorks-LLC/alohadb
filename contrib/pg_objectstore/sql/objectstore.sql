-- pg_objectstore regression test
-- Tests: credential management, table structure, function existence
-- Note: actual GET/PUT/DELETE calls require a live S3/GCS/Azure endpoint
--       and are exercised in integration tests, not regression tests.

CREATE EXTENSION pg_objectstore;

-- -----------------------------------------------------------------------
-- Table structure
-- -----------------------------------------------------------------------

SELECT column_name, data_type
FROM   information_schema.columns
WHERE  table_schema = 'public'
  AND  table_name   = 'pg_objectstore_credentials'
ORDER BY ordinal_position;

SELECT column_name, data_type
FROM   information_schema.columns
WHERE  table_schema = 'public'
  AND  table_name   = 'pg_objectstore_log'
ORDER BY ordinal_position;

-- -----------------------------------------------------------------------
-- Credential registration
-- -----------------------------------------------------------------------

SELECT objectstore_credential_register(
    's3',
    'prod-s3',
    '{"bucket":"my-bucket","region":"us-east-1","access_key_id":"AKIATEST","secret_access_key":"secret"}'::jsonb
) AS cred_id;

SELECT objectstore_credential_register(
    'gcs',
    'prod-gcs',
    '{"bucket":"my-gcs-bucket","project_id":"my-project","client_email":"sa@project.iam.gserviceaccount.com","private_key":"-----BEGIN RSA PRIVATE KEY-----\nMIIE...\n-----END RSA PRIVATE KEY-----"}'::jsonb
) AS cred_id;

SELECT objectstore_credential_register(
    'azure',
    'prod-azure',
    '{"account":"mystorageaccount","container":"mycontainer","sas_token":"sv=2020-08-04&ss=b&srt=co&sp=rwdlacuptfx&se=2025-01-01T00:00:00Z&st=2024-01-01T00:00:00Z&spr=https&sig=PLACEHOLDER"}'::jsonb
) AS cred_id;

-- List credentials (config column is NOT returned for security)
SELECT label, provider
FROM   objectstore_credential_list()
ORDER BY label;

-- Upsert: re-registering same label updates config
SELECT objectstore_credential_register(
    's3',
    'prod-s3',
    '{"bucket":"my-bucket-v2","region":"us-west-2","access_key_id":"AKIATEST2","secret_access_key":"secret2"}'::jsonb
) AS cred_id;

SELECT count(*) AS total_creds FROM objectstore_credential_list();

-- -----------------------------------------------------------------------
-- Error: unknown provider
-- -----------------------------------------------------------------------

\set ON_ERROR_STOP 0
SELECT objectstore_credential_register('gdriveee', 'bad', '{}'::jsonb);
\set ON_ERROR_STOP 1

-- -----------------------------------------------------------------------
-- SQL-callable C functions exist in pg_proc
-- -----------------------------------------------------------------------

SELECT proname
FROM   pg_proc
WHERE  proname IN (
    'objectstore_get',
    'objectstore_get_text',
    'objectstore_put',
    'objectstore_list',
    'objectstore_delete'
)
ORDER BY proname;

-- -----------------------------------------------------------------------
-- Audit log structure
-- -----------------------------------------------------------------------

SELECT count(*) AS log_entries FROM pg_objectstore_log;

-- -----------------------------------------------------------------------
-- Cleanup
-- -----------------------------------------------------------------------

DELETE FROM pg_objectstore_credentials;
DROP EXTENSION pg_objectstore;
