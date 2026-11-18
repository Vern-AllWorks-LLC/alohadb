/* contrib/pg_parquetio/pg_parquetio--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_parquetio UPDATE TO '1.2'" to load this file. \quit

-- Writers: export query results as Parquet / Arrow IPC without an ETL step.
-- Writing server files requires superuser or membership in
-- pg_write_server_files (checked in C, like COPY ... TO 'file'), and the
-- target directory must be listed in pg_parquetio.allowed_paths.

CREATE FUNCTION write_parquet(
    query          text,
    path           text,
    compression    text DEFAULT 'snappy',
    row_group_rows integer DEFAULT 131072
)
RETURNS bigint
LANGUAGE C VOLATILE STRICT
AS 'MODULE_PATHNAME', 'write_parquet';

CREATE FUNCTION write_arrow(
    query      text,
    path       text,
    batch_rows integer DEFAULT 65536
)
RETURNS bigint
LANGUAGE C VOLATILE STRICT
AS 'MODULE_PATHNAME', 'write_arrow';

CREATE FUNCTION arrow_ipc_stream(
    query      text,
    batch_rows integer DEFAULT 65536
)
RETURNS SETOF bytea
LANGUAGE C VOLATILE STRICT
AS 'MODULE_PATHNAME', 'arrow_ipc_stream';

COMMENT ON FUNCTION write_parquet(text, text, text, integer) IS
    'Run a query and write its result to a Parquet file (compression snappy or none); returns rows written';
COMMENT ON FUNCTION write_arrow(text, text, integer) IS
    'Run a query and write its result to an Arrow IPC file (memory-mappable by PyArrow, Polars, DuckDB); returns rows written';
COMMENT ON FUNCTION arrow_ipc_stream(text, integer) IS
    'Run a query and return its result as Arrow IPC stream messages (schema, record batches, end-of-stream)';
COMMENT ON FUNCTION read_parquet(text, jsonb) IS
    'Read a Parquet file and return rows as jsonb objects. '
    'Supports PLAIN, dictionary and RLE encodings, UNCOMPRESSED and SNAPPY '
    'compression, optional columns and single-level lists.';
