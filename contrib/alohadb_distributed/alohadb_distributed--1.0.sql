/* contrib/alohadb_distributed/alohadb_distributed--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_distributed" to load this file. \quit

-- ----------------------------------------------------------------
-- Metadata tables
-- ----------------------------------------------------------------

CREATE TABLE alohadb_dist_nodes (
    node_id serial PRIMARY KEY,
    node_name text NOT NULL UNIQUE,
    host text NOT NULL,
    port int NOT NULL DEFAULT 5432,
    dbname text NOT NULL DEFAULT 'postgres',
    status text NOT NULL DEFAULT 'active',
    rack text,
    region text,
    added_at timestamptz DEFAULT now()
);

CREATE TABLE alohadb_dist_tables (
    table_name text PRIMARY KEY,
    dist_column text NOT NULL,
    dist_method text NOT NULL DEFAULT 'hash',
    shard_count int NOT NULL DEFAULT 32,
    colocate_with text,
    created_at timestamptz DEFAULT now()
);

CREATE TABLE alohadb_dist_shards (
    shard_id serial PRIMARY KEY,
    table_name text NOT NULL REFERENCES alohadb_dist_tables(table_name),
    shard_index int NOT NULL,
    node_id int REFERENCES alohadb_dist_nodes(node_id),
    min_value text,
    max_value text,
    status text DEFAULT 'active'
);

-- ----------------------------------------------------------------
-- Functions
-- ----------------------------------------------------------------

CREATE FUNCTION distribute_table(
    tbl_name text,
    dist_column text,
    shard_count int DEFAULT 32
) RETURNS void
AS 'MODULE_PATHNAME', 'alohadb_distributed_distribute_table'
LANGUAGE C VOLATILE;

CREATE FUNCTION undistribute_table(
    tbl_name text
) RETURNS void
AS 'MODULE_PATHNAME', 'alohadb_distributed_undistribute_table'
LANGUAGE C VOLATILE;

CREATE FUNCTION create_reference_table(
    tbl_name text
) RETURNS void
AS 'MODULE_PATHNAME', 'alohadb_distributed_create_reference_table'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION add_node(
    node_name text,
    host text,
    port int DEFAULT 5432,
    dbname text DEFAULT 'postgres'
) RETURNS int
AS 'MODULE_PATHNAME', 'alohadb_distributed_add_node'
LANGUAGE C VOLATILE;

CREATE FUNCTION remove_node(
    node_name text
) RETURNS void
AS 'MODULE_PATHNAME', 'alohadb_distributed_remove_node'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION dist_table_info(
    tbl_name text
) RETURNS TABLE(shard_id int, shard_index int, node_name text, status text, row_count bigint)
AS 'MODULE_PATHNAME', 'alohadb_distributed_table_info'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION rebalance_shards()
RETURNS TABLE(shard_id int, from_node text, to_node text, status text)
AS 'MODULE_PATHNAME', 'alohadb_distributed_rebalance_shards'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION run_on_all_nodes(
    command text
) RETURNS TABLE(node_name text, result text, success bool)
AS 'MODULE_PATHNAME', 'alohadb_distributed_run_on_all_nodes'
LANGUAGE C VOLATILE STRICT;
