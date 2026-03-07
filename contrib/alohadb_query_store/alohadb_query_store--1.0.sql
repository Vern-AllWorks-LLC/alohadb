/* contrib/alohadb_query_store/alohadb_query_store--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_query_store" to load this file.\quit

CREATE FUNCTION query_store_entries()
RETURNS TABLE(
    query_hash bigint,
    query_text text,
    calls bigint,
    total_time float8,
    mean_time float8,
    min_time float8,
    max_time float8,
    rows bigint,
    first_seen timestamptz,
    last_seen timestamptz
)
LANGUAGE C
AS 'MODULE_PATHNAME', 'query_store_entries';

CREATE FUNCTION query_store_reset()
RETURNS void
LANGUAGE C
AS 'MODULE_PATHNAME', 'query_store_reset';

CREATE FUNCTION query_store_stats()
RETURNS TABLE(
    total_entries int,
    max_entries int
)
LANGUAGE C
AS 'MODULE_PATHNAME', 'query_store_stats';

CREATE FUNCTION index_advisor_recommend()
RETURNS TABLE(
    table_name text,
    column_name text,
    index_type text,
    reason text,
    create_statement text
)
LANGUAGE C
AS 'MODULE_PATHNAME', 'index_advisor_recommend';

CREATE FUNCTION index_advisor_unused_indexes()
RETURNS TABLE(
    index_name text,
    table_name text,
    index_size text,
    idx_scan bigint
)
LANGUAGE C
AS 'MODULE_PATHNAME', 'index_advisor_unused_indexes';

CREATE FUNCTION autovacuum_suggestions()
RETURNS TABLE(
    table_name text,
    dead_tuples bigint,
    live_tuples bigint,
    dead_ratio float8,
    last_autovacuum text,
    suggestion text
)
LANGUAGE C
AS 'MODULE_PATHNAME', 'autovacuum_suggestions';
