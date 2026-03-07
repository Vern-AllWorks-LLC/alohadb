/* contrib/alohadb_audit/alohadb_audit--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_audit" to load this file. \quit

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

CREATE FUNCTION audit_decrypt_log(
    line text,
    key text,
    redact boolean DEFAULT true
)
RETURNS text
AS 'MODULE_PATHNAME', 'audit_decrypt_log'
LANGUAGE C VOLATILE STRICT;
