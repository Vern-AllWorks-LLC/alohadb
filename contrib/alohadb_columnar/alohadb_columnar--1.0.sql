/* contrib/alohadb_columnar/alohadb_columnar--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_columnar" to load this file. \quit

-- Handler function for the table access method
CREATE FUNCTION columnar_tableam_handler(internal) RETURNS table_am_handler
AS 'MODULE_PATHNAME', 'columnar_tableam_handler' LANGUAGE C;

-- Create the columnar access method
CREATE ACCESS METHOD columnar TYPE TABLE HANDLER columnar_tableam_handler;

COMMENT ON ACCESS METHOD columnar IS 'Columnar storage with zstd compression';

-- Info function
CREATE FUNCTION alohadb_columnar_info(
    rel regclass,
    OUT total_stripes  int8,
    OUT total_rows     int8,
    OUT total_size     int8,
    OUT compression    text
)
RETURNS record
AS 'MODULE_PATHNAME', 'alohadb_columnar_info'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION alohadb_columnar_info(regclass) IS
'Show stripe count, row count, storage size, and compression stats for a columnar table';
