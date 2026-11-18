\echo Use "CREATE EXTENSION pg_vector" to load this file. \quit

-- -----------------------------------------------------------------------
-- pg_vector: vector type and IVFFlat approximate nearest-neighbor search
--
-- IVFFlat (Inverted File with Flat quantization):
--   - Training: Lloyd's k-means algorithm (S. P. Lloyd, 1957 Bell Labs memo;
--     published 1982 IEEE Trans. Inf. Theory).  Extensive prior art.
--   - Search:   inverted file index, prior art to 1960s (Salton, 1968).
--   Neither algorithm is covered by any current patent.
--
-- HNSW (Malkov & Yashunin, patent US10380099, ~2037 expiry) is NOT
-- implemented.  IVFFlat achieves comparable recall at lower index-build
-- cost.
-- -----------------------------------------------------------------------

-- The vector type itself is built into AlohaDB core (pg_catalog.vector,
-- with I/O, vector_dims() and the <->, <=>, <#>, <+> operators).  This
-- extension used to declare its own public.vector, which cannot coexist
-- with the core type: "vector" always resolves to pg_catalog.vector first,
-- so CREATE EXTENSION failed.  Both types share the same binary layout
-- (varlena header, int16 dim, int16 pad, float4[]), so the functions below
-- operate directly on the core type.

-- L2 (Euclidean) distance — prior art: Euclid, ~300 BCE
CREATE FUNCTION vector_l2_distance(a vector, b vector)
RETURNS float8
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
AS 'MODULE_PATHNAME', 'vector_l2_distance';

-- Cosine distance (1 − cosine_similarity) — prior art: Salton & McGill, 1983
CREATE FUNCTION vector_cosine_distance(a vector, b vector)
RETURNS float8
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
AS 'MODULE_PATHNAME', 'vector_cosine_distance';

-- Negative inner product (for max-IP search)
CREATE FUNCTION vector_neg_dot_product(a vector, b vector)
RETURNS float8
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
AS 'MODULE_PATHNAME', 'vector_neg_dot_product';

-- Distance operators (<->, <=>, <#>) are provided by core.

-- -----------------------------------------------------------------------
-- IVFFlat index catalog (table-based, not a native index AM)
-- -----------------------------------------------------------------------

-- Index metadata
CREATE TABLE pg_vector_indexes (
    index_name      text        NOT NULL PRIMARY KEY,
    source_table    text        NOT NULL,
    vector_column   text        NOT NULL,
    id_column       text        NOT NULL DEFAULT 'id',
    ndim            int         NOT NULL,
    n_lists         int         NOT NULL DEFAULT 100,
    n_probes        int         NOT NULL DEFAULT 10,
    distance_metric text        NOT NULL DEFAULT 'l2',  -- 'l2' | 'cosine' | 'ip'
    trained         boolean     NOT NULL DEFAULT false,
    vector_count    bigint      NOT NULL DEFAULT 0,
    created_at      timestamptz NOT NULL DEFAULT now(),
    last_trained_at timestamptz
);

-- Centroids (k-means cluster centres)
CREATE TABLE pg_vector_centroids (
    index_name  text    NOT NULL,
    centroid_id int     NOT NULL,
    centroid    vector  NOT NULL,
    PRIMARY KEY (index_name, centroid_id)
);

-- Per-vector cluster assignments
CREATE TABLE pg_vector_entries (
    index_name  text    NOT NULL,
    centroid_id int     NOT NULL,
    entry_id    text    NOT NULL,   -- cast of the source table's id column
    vec         vector  NOT NULL,
    PRIMARY KEY (index_name, entry_id)
);

CREATE INDEX pg_vector_entries_centroid_idx
    ON pg_vector_entries (index_name, centroid_id);

-- Index data must survive pg_dump / pg_dumpall
SELECT pg_catalog.pg_extension_config_dump('pg_vector_indexes', '');
SELECT pg_catalog.pg_extension_config_dump('pg_vector_centroids', '');
SELECT pg_catalog.pg_extension_config_dump('pg_vector_entries', '');

-- -----------------------------------------------------------------------
-- Index management API
-- -----------------------------------------------------------------------

-- Register a new index (does not train immediately)
CREATE FUNCTION vector_index_create(
    p_index_name    text,
    p_source_table  text,
    p_vector_column text,
    p_id_column     text    DEFAULT 'id',
    p_n_lists       int     DEFAULT 100,
    p_n_probes      int     DEFAULT 10,
    p_metric        text    DEFAULT 'l2'
)
RETURNS void
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    v_ndim int;
BEGIN
    -- Infer dimension from a sample row
    EXECUTE format(
        'SELECT vector_dims(%I) FROM %I WHERE %I IS NOT NULL LIMIT 1',
        p_vector_column, p_source_table, p_vector_column
    ) INTO v_ndim;

    IF v_ndim IS NULL THEN
        RAISE EXCEPTION 'vector_index_create: no non-null vectors found in %.%',
                        p_source_table, p_vector_column;
    END IF;

    INSERT INTO pg_vector_indexes
        (index_name, source_table, vector_column, id_column,
         ndim, n_lists, n_probes, distance_metric)
    VALUES
        (p_index_name, p_source_table, p_vector_column, p_id_column,
         v_ndim, p_n_lists, p_n_probes, p_metric);
END;
$$;

-- Insert a single vector into an existing trained index
CREATE FUNCTION vector_index_insert(
    p_index_name text,
    p_id         text,
    p_vec        vector
)
RETURNS void
LANGUAGE C VOLATILE STRICT
AS 'MODULE_PATHNAME', 'vector_index_insert';

-- ANN search: returns (entry_id, distance) for the k nearest neighbours
CREATE FUNCTION vector_index_search(
    p_index_name text,
    p_query      vector,
    p_k          int DEFAULT 10,
    p_n_probes   int DEFAULT NULL  -- NULL = use index default
)
RETURNS TABLE(entry_id text, distance float8)
LANGUAGE C VOLATILE  -- not STRICT: p_n_probes defaults to NULL
AS 'MODULE_PATHNAME', 'vector_index_search';

-- Rebuild (re-train) the index from the source table
CREATE FUNCTION vector_index_rebuild(p_index_name text)
RETURNS void
LANGUAGE C VOLATILE STRICT
AS 'MODULE_PATHNAME', 'vector_index_rebuild';

-- Index statistics
CREATE FUNCTION vector_index_stats(p_index_name text DEFAULT NULL)
RETURNS TABLE(
    index_name      text,
    source_table    text,
    ndim            int,
    n_lists         int,
    n_probes        int,
    distance_metric text,
    trained         boolean,
    vector_count    bigint,
    last_trained_at timestamptz
)
LANGUAGE sql STABLE
AS $$
    SELECT index_name, source_table, ndim, n_lists, n_probes,
           distance_metric, trained, vector_count, last_trained_at
    FROM   pg_vector_indexes
    WHERE  p_index_name IS NULL OR index_name = p_index_name
    ORDER BY index_name;
$$;
