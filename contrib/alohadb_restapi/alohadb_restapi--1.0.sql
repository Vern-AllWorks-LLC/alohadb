/* contrib/alohadb_restapi/alohadb_restapi--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_restapi" to load this file. \quit

-- ----------------------------------------------------------------
-- alohadb_restapi_handle(method, path, body, headers, query_params)
--
-- Directly invoke the REST API handler from SQL.  Useful for testing
-- and for calling the API without going through the HTTP layer.
-- ----------------------------------------------------------------
CREATE FUNCTION alohadb_restapi_handle(
    method       text,
    path         text,
    body         json DEFAULT NULL,
    headers      json DEFAULT NULL,
    query_params json DEFAULT NULL
)
RETURNS json
AS 'MODULE_PATHNAME', 'alohadb_restapi_handle'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION alohadb_restapi_handle(text, text, json, json, json) IS
'Invoke the REST API handler directly: method (GET/POST/PUT/DELETE), path (/api/table[/id])';

-- ----------------------------------------------------------------
-- alohadb_restapi_status()
--
-- Returns the current configuration settings for the REST API
-- background worker.
-- ----------------------------------------------------------------
CREATE FUNCTION alohadb_restapi_status(
    OUT setting text,
    OUT value   text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'alohadb_restapi_status'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION alohadb_restapi_status() IS
'Show REST API background worker configuration: port, database, schema, default role';

-- ----------------------------------------------------------------
-- alohadb_restapi_endpoints()
--
-- Returns the list of available REST API endpoints by scanning
-- pg_catalog.pg_tables for tables in the configured schema.
-- ----------------------------------------------------------------
CREATE FUNCTION alohadb_restapi_endpoints(
    OUT method      text,
    OUT path        text,
    OUT description text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'alohadb_restapi_endpoints'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION alohadb_restapi_endpoints() IS
'List all auto-generated REST API endpoints derived from tables in the configured schema';
