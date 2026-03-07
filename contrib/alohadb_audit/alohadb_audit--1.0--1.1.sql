/* contrib/alohadb_audit/alohadb_audit--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION alohadb_audit UPDATE" to load this file. \quit

-- Replace status function with encryption_active column
DROP FUNCTION IF EXISTS audit_log_status();
CREATE FUNCTION audit_log_status(
    OUT enabled boolean,
    OUT log_directory text,
    OUT log_format text,
    OUT databases text,
    OUT operations text,
    OUT log_query_text boolean,
    OUT encryption_active boolean
)
RETURNS record
AS 'MODULE_PATHNAME', 'audit_log_status'
LANGUAGE C STABLE;

-- New: decrypt + redact function
CREATE OR REPLACE FUNCTION audit_decrypt_log(
    line text,
    key text,
    redact boolean DEFAULT true
)
RETURNS text
AS 'MODULE_PATHNAME', 'audit_decrypt_log'
LANGUAGE C VOLATILE STRICT;
