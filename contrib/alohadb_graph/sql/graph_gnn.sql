--
-- alohadb_graph: GNN neighbor sampling and neighbor feature aggregation
--
CREATE EXTENSION IF NOT EXISTS alohadb_graph;
SET timezone = 'UTC';

CREATE TABLE n (id bigint PRIMARY KEY, feat float8, emb float8[], vec text);
INSERT INTO n SELECT i, CASE WHEN i = 5 THEN NULL ELSE i END,
                     ARRAY[i, 10 * i], format('[%s,%s]', i, -i)
                FROM generate_series(1, 10) i;
CREATE TABLE ge (s bigint, d bigint, vf timestamptz);
INSERT INTO ge VALUES
    (1, 2, '2020-01-01'), (1, 3, '2020-01-01'), (1, 4, '2020-01-01'),
    (1, 5, '2020-01-01'), (1, 6, '2020-01-01'),
    (1, 7, '2025-01-01'),                         -- a "future" edge
    (2, 8, '2020-01-01'), (2, 8, '2021-01-01'),   -- multi-edge
    (2, 9, '2020-01-01'), (3, 8, '2020-01-01'), (3, 10, '2020-01-01'),
    (4, 9, '2020-01-01');
SELECT graph_create('gnn');
SELECT graph_add_vertex_label('gnn', 'n', 'n');
SELECT graph_add_edge_label('gnn', 'ge', 'ge', 's', 'n', 'd', 'n', valid_from_column => 'vf');

-- ---------------- uniform neighbor sampling ----------------
CREATE TEMP TABLE s42 AS
  SELECT * FROM graph_sample_neighbors('gnn', ARRAY[1], ARRAY[3, 2], seed => 42);

-- hop 1: exactly 3 distinct neighbors of 1 (it has 6)
SELECT count(*), count(DISTINCT dst), bool_and(src = 1) FROM s42 WHERE hop = 1;
-- hop 2 expands exactly the vertices sampled in hop 1, at most 2 distinct each
SELECT (SELECT array_agg(DISTINCT src ORDER BY src) FROM s42 WHERE hop = 2)
       <@ (SELECT array_agg(dst) FROM s42 WHERE hop = 1) AS hop2_from_hop1,
       (SELECT max(c) FROM (SELECT count(DISTINCT dst) AS c FROM s42
                             WHERE hop = 2 GROUP BY src) x) <= 2 AS fanout_ok;
-- every hop-1 vertex with neighbors got min(2, #distinct neighbors) samples
SELECT h1.dst AS v,
       (SELECT count(DISTINCT d) FROM ge WHERE s = h1.dst) AS n_neighbors,
       (SELECT count(*) FROM s42 WHERE hop = 2 AND src = h1.dst) AS sampled
  FROM s42 h1 WHERE hop = 1
 ORDER BY 1;
-- every sampled edge exists
SELECT count(*) AS nonexistent
  FROM s42 WHERE NOT EXISTS (SELECT 1 FROM ge WHERE ge.s = s42.src AND ge.d = s42.dst);
-- deterministic for a fixed seed
SELECT (SELECT array_agg((hop, src, dst)::text ORDER BY hop, src, dst) FROM s42) =
       (SELECT array_agg((hop, src, dst)::text ORDER BY hop, src, dst)
          FROM graph_sample_neighbors('gnn', ARRAY[1], ARRAY[3, 2], seed => 42) x)
       AS same_sample;
-- fanout -1 (or larger than the degree) takes all distinct neighbors
SELECT hop, src, array_agg(dst ORDER BY dst)
  FROM graph_sample_neighbors('gnn', ARRAY[1, 2, 1], ARRAY[-1], seed => 1)
 GROUP BY hop, src ORDER BY hop, src;
SELECT count(*) FROM graph_sample_neighbors('gnn', ARRAY[2], ARRAY[10], seed => 1);
-- point in time: the 2025 edge 1->7 is never sampled as of 2024
SELECT count(*) AS n, count(*) FILTER (WHERE dst = 7) AS future_edges
  FROM graph_sample_neighbors('gnn', ARRAY[1], ARRAY[-1], as_of => '2024-01-01', seed => 7);
-- uniformity: over 300 seeds each of the 6 neighbors of 1 is picked about
-- 300 * 3/6 = 150 times (sd ~ 8.7)
SELECT dst, count(*) BETWEEN 110 AND 190 AS plausible
  FROM generate_series(1, 300) sd,
       LATERAL graph_sample_neighbors('gnn', ARRAY[1], ARRAY[3], seed => sd)
 GROUP BY dst ORDER BY dst;
-- unseeded sampling still respects the structure
SELECT count(*) FROM graph_sample_neighbors('gnn', ARRAY[1], ARRAY[4]);
-- edge cases
SELECT count(*) FROM graph_sample_neighbors('gnn', ARRAY[]::bigint[], ARRAY[2]);
SELECT count(*) FROM graph_sample_neighbors('gnn', ARRAY[42], ARRAY[2]);
SELECT count(*) FROM graph_sample_neighbors('gnn', ARRAY[1], ARRAY[0, 5]);
SELECT * FROM graph_sample_neighbors('gnn', ARRAY[1], NULL);       -- error
SELECT * FROM graph_sample_neighbors('gnn', ARRAY[1], ARRAY[-2]);  -- error

-- ---------------- neighbor feature aggregation ----------------
-- 1: neighbors 2..7, feat of 5 is NULL -> mean(2,3,4,6,7) = 4.4
-- 2: two parallel edges to 8 and one to 9 -> degree 3, mean(8,9) = 8.5
-- 3: mean(8,10) = 9; 10: no out-neighbors
SELECT * FROM graph_node_features('gnn', ARRAY[1, 2, 3, 10], 'feat');
-- as of 2024 the edge 1->7 does not exist yet: mean(2,3,4,6) = 3.75
SELECT * FROM graph_node_features('gnn', ARRAY[1], 'feat', as_of => '2024-01-01');
-- numeric arrays: mean of [k, 10k] for k = 2..7 = [4.5, 45]
SELECT * FROM graph_node_features('gnn', ARRAY[1], 'emb');
-- vector-like text '[k,-k]' -> [4.5, -4.5]
SELECT * FROM graph_node_features('gnn', ARRAY[1], 'vec');
-- incoming neighbors of 8 are 2 and 3
SELECT * FROM graph_node_features('gnn', ARRAY[8], 'feat', direction => 'in');
SELECT * FROM graph_node_features('gnn', ARRAY[1], 'nosuch');      -- error

-- point-in-time training rows: sampled neighborhood + features, one query
SELECT count(*) AS rows, bool_and(s.dst BETWEEN 2 AND 6) AS no_future,
       bool_and(f.vertex = s.dst AND f.degree >= 0) AS joined
  FROM graph_sample_neighbors('gnn', ARRAY[1], ARRAY[2], as_of => '2024-01-01', seed => 3) s
  CROSS JOIN LATERAL graph_node_features('gnn', ARRAY[s.dst], 'feat',
                                         as_of => '2024-01-01') f;

SELECT graph_drop('gnn');
DROP TABLE ge, n;
