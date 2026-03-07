/* contrib/alohadb_graphql/alohadb_graphql--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_graphql" to load this file.\quit

--
-- graphql_execute(query text, variables jsonb, operation_name text)
--
-- Execute a GraphQL query or mutation against the current database.
-- The query is parsed, translated to SQL, executed via SPI, and the
-- result is returned as a jsonb document with a {"data": {...}} wrapper.
--
CREATE FUNCTION graphql_execute(
    query text,
    variables jsonb DEFAULT NULL,
    operation_name text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE C STABLE
AS 'MODULE_PATHNAME', 'graphql_execute';

COMMENT ON FUNCTION graphql_execute(text, jsonb, text) IS
    'Execute a GraphQL query or mutation and return the result as jsonb';

--
-- graphql_schema()
--
-- Auto-generate a GraphQL SDL schema from the current database's
-- information_schema (public schema tables only).
--
CREATE FUNCTION graphql_schema()
RETURNS text
LANGUAGE C STABLE
AS 'MODULE_PATHNAME', 'graphql_schema';

COMMENT ON FUNCTION graphql_schema() IS
    'Return auto-generated GraphQL SDL schema from information_schema';

--
-- graphql_schema_json()
--
-- Same as graphql_schema() but returns the schema as a jsonb document.
--
CREATE FUNCTION graphql_schema_json()
RETURNS jsonb
LANGUAGE C STABLE
AS 'MODULE_PATHNAME', 'graphql_schema_json';

COMMENT ON FUNCTION graphql_schema_json() IS
    'Return auto-generated GraphQL schema as jsonb from information_schema';
