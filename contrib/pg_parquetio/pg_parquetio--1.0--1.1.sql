/* contrib/pg_parquetio/pg_parquetio--1.0--1.1.sql */

-- Upgrade script: replaces the stub read_parquet() with a full C implementation
-- that reads column data using plain/dictionary encoding and
-- snappy/uncompressed compression.

-- Drop the old stub
DROP FUNCTION read_parquet(text);

-- Full Parquet column reader.
-- Supports: PLAIN + PLAIN_DICTIONARY encoding; UNCOMPRESSED + SNAPPY compression;
-- column types INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY (text), BOOLEAN.
-- Returns one jsonb object per row.  Nested / repeated schemas are not
-- supported; use libarrow for complex Parquet files.
CREATE FUNCTION read_parquet(
    file_path text,
    options   jsonb DEFAULT '{}'
)
RETURNS SETOF jsonb
LANGUAGE C VOLATILE STRICT
AS 'MODULE_PATHNAME', 'read_parquet_full';

COMMENT ON FUNCTION read_parquet(text, jsonb) IS
    'Read a Parquet file and return rows as jsonb objects. '
    'Supports PLAIN and PLAIN_DICTIONARY encoding, '
    'UNCOMPRESSED and SNAPPY compression, flat schemas.';
