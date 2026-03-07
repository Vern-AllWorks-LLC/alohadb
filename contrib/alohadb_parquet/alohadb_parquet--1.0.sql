/* contrib/alohadb_parquet/alohadb_parquet--1.0.sql */

\echo Use "CREATE EXTENSION alohadb_parquet" to load this file.\quit

-- CSV file reader
CREATE FUNCTION read_csv(
    file_path text,
    delimiter text DEFAULT ',',
    has_header bool DEFAULT true
)
RETURNS SETOF jsonb
LANGUAGE C
AS 'MODULE_PATHNAME', 'read_csv';

-- JSON Lines file reader
CREATE FUNCTION read_json(file_path text)
RETURNS SETOF jsonb
LANGUAGE C
AS 'MODULE_PATHNAME', 'read_json_file';

-- Parquet file reader (validates format; full reading requires libarrow)
CREATE FUNCTION read_parquet(file_path text)
RETURNS SETOF jsonb
LANGUAGE C
AS 'MODULE_PATHNAME', 'read_parquet';

COMMENT ON FUNCTION read_csv(text, text, bool) IS
    'Read a CSV file and return rows as jsonb objects';
COMMENT ON FUNCTION read_json(text) IS
    'Read a JSON Lines file and return each line as jsonb';
COMMENT ON FUNCTION read_parquet(text) IS
    'Read a Parquet file (validates format; full reading requires libarrow)';
