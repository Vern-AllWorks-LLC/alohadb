/* contrib/alohadb_jsonschema/alohadb_jsonschema--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_jsonschema" to load this file. \quit

-- ----------------------------------------------------------------
-- alohadb_jsonschema_registry
--
-- Optional registry for named schemas, so that CHECK constraints
-- and application code can reference schemas by name.
-- ----------------------------------------------------------------
CREATE TABLE alohadb_jsonschema_registry (
    schema_name text PRIMARY KEY,
    schema_doc  jsonb NOT NULL,
    created_at  timestamptz DEFAULT now()
);

COMMENT ON TABLE alohadb_jsonschema_registry IS
'Registry of named JSON Schema documents for reuse in validation';

-- ----------------------------------------------------------------
-- jsonschema_is_valid(doc jsonb, schema jsonb) -> boolean
--
-- Returns true if the document validates against the schema.
-- Designed for use in CHECK constraints (IMMUTABLE, PARALLEL SAFE).
-- ----------------------------------------------------------------
CREATE FUNCTION jsonschema_is_valid(doc jsonb, schema jsonb)
    RETURNS boolean
    AS 'MODULE_PATHNAME', 'jsonschema_is_valid'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION jsonschema_is_valid(jsonb, jsonb) IS
'Validate a JSONB document against a JSON Schema Draft-07; returns true if valid';

-- ----------------------------------------------------------------
-- jsonschema_validate(doc jsonb, schema jsonb)
--     -> TABLE(valid boolean, error_path text, error_message text)
--
-- Returns detailed validation results with error paths and messages.
-- ----------------------------------------------------------------
CREATE FUNCTION jsonschema_validate(doc jsonb, schema jsonb)
    RETURNS TABLE(valid boolean, error_path text, error_message text)
    AS 'MODULE_PATHNAME', 'jsonschema_validate'
    LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION jsonschema_validate(jsonb, jsonb) IS
'Validate a JSONB document against a JSON Schema Draft-07; returns error details';

-- ----------------------------------------------------------------
-- jsonschema_register(schema_name text, schema_doc jsonb) -> void
--
-- Register a named schema in the registry table.
-- ----------------------------------------------------------------
CREATE FUNCTION jsonschema_register(schema_name text, schema_doc jsonb)
    RETURNS void
    AS 'MODULE_PATHNAME', 'jsonschema_register'
    LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION jsonschema_register(text, jsonb) IS
'Store a named JSON Schema in the registry for later use with jsonschema_validate_named';

-- ----------------------------------------------------------------
-- jsonschema_validate_named(doc jsonb, schema_name text) -> boolean
--
-- Look up a schema by name from the registry and validate.
-- ----------------------------------------------------------------
CREATE FUNCTION jsonschema_validate_named(doc jsonb, schema_name text)
    RETURNS boolean
    AS 'MODULE_PATHNAME', 'jsonschema_validate_named'
    LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION jsonschema_validate_named(jsonb, text) IS
'Validate a JSONB document against a named schema from the registry';
