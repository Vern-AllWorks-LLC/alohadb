-- alohadb_vector regression test
-- Tests: type I/O, distance operators, IVFFlat index management

CREATE EXTENSION alohadb_vector;

-- -----------------------------------------------------------------------
-- Type I/O
-- -----------------------------------------------------------------------

SELECT '[1,2,3]'::vector;
SELECT '[1.5, -2.0, 0.75]'::vector;
SELECT vector_dims('[10,20,30,40]'::vector);

-- Round-trip
SELECT v::text FROM (SELECT '[1,2,3]'::vector AS v) t;

-- Error: wrong format
\set ON_ERROR_STOP 0
SELECT 'not-a-vector'::vector;
\set ON_ERROR_STOP 1

-- -----------------------------------------------------------------------
-- Distance operators
-- -----------------------------------------------------------------------

-- L2 distance (Euclidean)
SELECT '[1,0,0]'::vector <-> '[0,1,0]'::vector AS l2_dist;
SELECT '[3,4]'::vector <-> '[0,0]'::vector AS l2_5;

-- Cosine distance
SELECT '[1,0,0]'::vector <=> '[1,0,0]'::vector AS cos_same;
SELECT '[1,0,0]'::vector <=> '[0,1,0]'::vector AS cos_ortho;

-- Negative dot product
SELECT '[1,2,3]'::vector <#> '[1,2,3]'::vector AS neg_dot;

-- Scalar functions
SELECT vector_l2_distance('[0,3,4]'::vector, '[0,0,0]'::vector) AS l2_5;
SELECT vector_cosine_distance('[1,0]'::vector, '[0,1]'::vector) AS cos_1;
SELECT vector_neg_dot_product('[1,2,3]'::vector, '[4,5,6]'::vector) AS neg_dot_32;

-- -----------------------------------------------------------------------
-- IVFFlat index lifecycle
-- -----------------------------------------------------------------------

-- Source table
CREATE TABLE vec_items (
    id   bigserial PRIMARY KEY,
    name text,
    emb  vector
);

INSERT INTO vec_items (name, emb) VALUES
    ('alpha',   '[1.0, 0.0, 0.0]'),
    ('beta',    '[0.0, 1.0, 0.0]'),
    ('gamma',   '[0.0, 0.0, 1.0]'),
    ('delta',   '[0.7, 0.7, 0.0]'),
    ('epsilon', '[0.5, 0.5, 0.5]'),
    ('zeta',    '[0.9, 0.1, 0.1]'),
    ('eta',     '[0.1, 0.9, 0.1]'),
    ('theta',   '[0.1, 0.1, 0.9]');

-- Register index (does not train yet)
SELECT vector_index_create(
    'items_emb_idx',
    'vec_items',
    'emb',
    'id',
    3,      -- n_lists
    2,      -- n_probes
    'l2'    -- metric
);

-- Check metadata before training
SELECT index_name, ndim, n_lists, n_probes, distance_metric, trained
FROM   vector_index_stats('items_emb_idx');

-- Train (runs Lloyd's k-means)
SELECT vector_index_rebuild('items_emb_idx');

-- Verify trained state and vector count
SELECT trained, vector_count
FROM   vector_index_stats('items_emb_idx');

-- ANN search
SELECT entry_id, round(distance::numeric, 4) AS dist
FROM   vector_index_search(
           'items_emb_idx',
           '[1.0, 0.0, 0.0]'::vector,
           3   -- k
       )
ORDER BY dist;

-- Single insert into trained index
INSERT INTO vec_items (name, emb) VALUES ('iota', '[0.8, 0.1, 0.1]');
SELECT vector_index_insert('items_emb_idx', (SELECT id::text FROM vec_items WHERE name = 'iota' LIMIT 1), '[0.8, 0.1, 0.1]'::vector);

-- Stats should show incremented count
SELECT vector_count FROM vector_index_stats('items_emb_idx');

-- Cosine metric
SELECT vector_index_create(
    'items_cos_idx',
    'vec_items',
    'emb',
    'id',
    3,
    2,
    'cosine'
);
SELECT vector_index_rebuild('items_cos_idx');
SELECT trained FROM vector_index_stats('items_cos_idx');

-- List all indexes
SELECT index_name, ndim, distance_metric, trained
FROM   vector_index_stats()
ORDER BY index_name;

-- Cleanup
DROP TABLE vec_items CASCADE;
DROP EXTENSION alohadb_vector;
