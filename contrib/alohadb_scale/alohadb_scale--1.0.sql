/* contrib/alohadb_scale/alohadb_scale--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_scale" to load this file.\quit

CREATE FUNCTION scale_status()
RETURNS TABLE(
    setting_name text,
    setting_value text
)
LANGUAGE C
AS 'MODULE_PATHNAME', 'scale_status';

CREATE FUNCTION scale_suspend()
RETURNS text
LANGUAGE C
AS 'MODULE_PATHNAME', 'scale_suspend';

CREATE FUNCTION scale_configure(
    suspend_after interval DEFAULT NULL,
    min_connections int DEFAULT NULL
)
RETURNS void
LANGUAGE C
AS 'MODULE_PATHNAME', 'scale_configure';

CREATE FUNCTION scale_activity()
RETURNS TABLE(
    total_connections int,
    active_connections int,
    idle_connections int,
    idle_seconds float8,
    last_activity timestamptz
)
LANGUAGE C
AS 'MODULE_PATHNAME', 'scale_activity';
