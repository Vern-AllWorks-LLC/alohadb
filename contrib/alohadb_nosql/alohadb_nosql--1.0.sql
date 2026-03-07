/* contrib/alohadb_nosql/alohadb_nosql--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_nosql" to load this file. \quit

-- ----------------------------------------------------------------
-- alohadb_nosql_collections
--
-- Metadata table tracking all document collections managed by
-- this extension.
-- ----------------------------------------------------------------
CREATE TABLE alohadb_nosql_collections (
    name        text    PRIMARY KEY,
    has_schema  boolean NOT NULL DEFAULT false,
    created_at  timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE alohadb_nosql_collections IS
'Metadata registry of document collections managed by alohadb_nosql';

COMMENT ON COLUMN alohadb_nosql_collections.name IS
'The collection name, which is also the underlying table name';

COMMENT ON COLUMN alohadb_nosql_collections.has_schema IS
'Whether a JSON schema validation constraint was added at creation time';

-- ================================================================
-- Collection management functions
-- ================================================================

CREATE FUNCTION doc_create_collection(
    p_name   text,
    p_schema jsonb DEFAULT NULL
)
RETURNS void
AS 'MODULE_PATHNAME', 'doc_create_collection'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION doc_create_collection(text, jsonb) IS
'Create a new document collection with an optional JSON schema constraint';

CREATE FUNCTION doc_drop_collection(
    p_name text
)
RETURNS void
AS 'MODULE_PATHNAME', 'doc_drop_collection'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION doc_drop_collection(text) IS
'Drop a document collection and remove its metadata';

CREATE FUNCTION doc_list_collections(
    OUT name       text,
    OUT doc_count  int8,
    OUT has_schema bool
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'doc_list_collections'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION doc_list_collections() IS
'List all document collections with row counts and schema status';

-- ================================================================
-- CRUD functions
-- ================================================================

CREATE FUNCTION doc_insert(
    p_collection text,
    p_doc        jsonb
)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'doc_insert'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION doc_insert(text, jsonb) IS
'Insert a document into a collection, returning {"_id": "..."}';

CREATE FUNCTION doc_insert_batch(
    p_collection text,
    p_docs       jsonb[]
)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'doc_insert_batch'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION doc_insert_batch(text, jsonb[]) IS
'Insert multiple documents into a collection, returning {"inserted": N}';

CREATE FUNCTION doc_get(
    p_collection text,
    p_id         text
)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'doc_get'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION doc_get(text, text) IS
'Fetch a single document by _id from a collection';

CREATE FUNCTION doc_put(
    p_collection text,
    p_id         text,
    p_doc        jsonb
)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'doc_put'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION doc_put(text, text, jsonb) IS
'Replace a document by _id, returning {"modified": 0|1}';

CREATE FUNCTION doc_remove(
    p_collection text,
    p_filter     jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'doc_remove'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION doc_remove(text, jsonb) IS
'Delete documents matching a filter, returning {"deleted": N}';

CREATE FUNCTION doc_count(
    p_collection text,
    p_filter     jsonb DEFAULT '{}'::jsonb
)
RETURNS int8
AS 'MODULE_PATHNAME', 'doc_count'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION doc_count(text, jsonb) IS
'Count documents in a collection matching an optional filter';

CREATE FUNCTION doc_patch(
    p_collection text,
    p_filter     jsonb,
    p_changes    jsonb
)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'doc_patch'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION doc_patch(text, jsonb, jsonb) IS
'Partial update: merge changes into documents matching the filter, returning {"matched": N, "modified": N}';

-- ================================================================
-- Query / Search / Analytics functions
-- ================================================================

CREATE FUNCTION doc_search(
    p_collection text,
    p_filter     jsonb DEFAULT '{}'::jsonb,
    p_opts       jsonb DEFAULT NULL
)
RETURNS SETOF jsonb
AS 'MODULE_PATHNAME', 'doc_search'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION doc_search(text, jsonb, jsonb) IS
'Search a collection with filter, sort, limit, offset, and field projection options';

CREATE FUNCTION doc_query(
    p_collection text,
    p_sql_where  text
)
RETURNS SETOF jsonb
AS 'MODULE_PATHNAME', 'doc_query'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION doc_query(text, text) IS
'Query a collection using a raw SQL WHERE clause for full PostgreSQL expression power';

CREATE FUNCTION doc_group(
    p_collection text,
    p_group_by   text,
    p_agg_expr   text,
    p_filter     jsonb DEFAULT NULL
)
RETURNS SETOF jsonb
AS 'MODULE_PATHNAME', 'doc_group'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION doc_group(text, text, text, jsonb) IS
'Group-by analytics over a collection, delegating entirely to the PostgreSQL planner';

-- ================================================================
-- Array manipulation functions (IMMUTABLE, pure transforms)
-- ================================================================

CREATE FUNCTION doc_array_append(
    p_doc   jsonb,
    p_path  text,
    p_value jsonb
)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'doc_array_append'
LANGUAGE C IMMUTABLE STRICT;

COMMENT ON FUNCTION doc_array_append(jsonb, text, jsonb) IS
'Append a value to a JSON array at the specified path';

CREATE FUNCTION doc_array_remove(
    p_doc   jsonb,
    p_path  text,
    p_value jsonb
)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'doc_array_remove'
LANGUAGE C IMMUTABLE STRICT;

COMMENT ON FUNCTION doc_array_remove(jsonb, text, jsonb) IS
'Remove all occurrences of a value from a JSON array at the specified path';

CREATE FUNCTION doc_array_add_unique(
    p_doc   jsonb,
    p_path  text,
    p_value jsonb
)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'doc_array_add_unique'
LANGUAGE C IMMUTABLE STRICT;

COMMENT ON FUNCTION doc_array_add_unique(jsonb, text, jsonb) IS
'Append a value to a JSON array at the specified path only if not already present';
