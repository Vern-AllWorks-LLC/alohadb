--
-- alohadb_graph: PageRank, connected components, degree
--
CREATE EXTENSION IF NOT EXISTS alohadb_graph;
SET timezone = 'UTC';

-- ---------------- PageRank ----------------
-- 1->2, 1->3, 2->3, 3->1, 3->4; vertex 4 is dangling.
CREATE TABLE pv (id bigint PRIMARY KEY);
INSERT INTO pv VALUES (1), (2), (3), (4);
CREATE TABLE pe (s bigint, d bigint);
INSERT INTO pe VALUES (1, 2), (1, 3), (2, 3), (3, 1), (3, 4);
SELECT graph_create('pr');
SELECT graph_add_vertex_label('pr', 'pv', 'pv');
SELECT graph_add_edge_label('pr', 'pe', 'pe', 's', 'pv', 'd', 'pv');

-- One iteration by hand (d = 0.85, N = 4, r0 = 0.25, dangling mass 0.25):
--   base = 0.15/4 + 0.85*0.25/4 = 0.090625
--   r(1) = base + 0.85*(0.25/2)          = 0.196875
--   r(2) = base + 0.85*(0.25/2)          = 0.196875
--   r(3) = base + 0.85*(0.25/2 + 0.25)   = 0.409375
--   r(4) = base + 0.85*(0.25/2)          = 0.196875
SELECT vertex, round(rank::numeric, 6) FROM graph_pagerank('pr', 0.85, 1);

-- Converged values equal the exact stationary solution of the linear system
-- x = (1-d)/N + d*(sum_in x_u/outdeg(u) + x_4/N), solved with fractions:
-- x = (1429, 1140, 2109, 1429) / 6107
SELECT p.vertex, round(p.rank::numeric, 4) AS rank,
       abs(p.rank - x.exact) < 1e-9 AS matches_exact
  FROM graph_pagerank('pr', iterations => 100) p
  JOIN (VALUES (1, 1429/6107.0), (2, 1140/6107.0), (3, 2109/6107.0), (4, 1429/6107.0))
       AS x(vertex, exact) USING (vertex)
 ORDER BY vertex;
-- the default 20 iterations already agree to 4 decimals; ranks sum to 1
SELECT vertex, round(rank::numeric, 4) FROM graph_pagerank('pr');
SELECT round(sum(rank)::numeric, 10) FROM graph_pagerank('pr');
-- 0 iterations / damping 0 give the uniform distribution
SELECT vertex, rank FROM graph_pagerank('pr', 0.85, 0);
SELECT vertex, rank FROM graph_pagerank('pr', 0.0, 5);
SELECT * FROM graph_pagerank('pr', 1.5);                           -- error

-- symmetric 3-cycle: every vertex has rank exactly 1/3
CREATE TABLE cyv (id bigint PRIMARY KEY);
INSERT INTO cyv VALUES (1), (2), (3);
CREATE TABLE cyc (s bigint, d bigint);
INSERT INTO cyc VALUES (1, 2), (2, 3), (3, 1);
SELECT graph_create('cycle');
SELECT graph_add_vertex_label('cycle', 'cyv', 'cyv');
SELECT graph_add_edge_label('cycle', 'cyc', 'cyc', 's', 'cyv', 'd', 'cyv');
SELECT vertex, round(rank::numeric, 10) FROM graph_pagerank('cycle');

-- ---------------- components and degree ----------------
-- 1->2, 2->3 (valid from 2024-06-01), 4->5, 6->6, undirected 7-4, 8 isolated
CREATE TABLE cv (id bigint PRIMARY KEY);
INSERT INTO cv SELECT generate_series(1, 8);
CREATE TABLE ce (s bigint, d bigint, since timestamptz);
INSERT INTO ce VALUES (1, 2, NULL), (2, 3, '2024-06-01'), (4, 5, NULL), (6, 6, NULL);
CREATE TABLE cl (a bigint, b bigint);
INSERT INTO cl VALUES (7, 4);
SELECT graph_create('cc');
SELECT graph_add_vertex_label('cc', 'cv', 'cv');
SELECT graph_add_edge_label('cc', 'ce', 'ce', 's', 'cv', 'd', 'cv', valid_from_column => 'since');
SELECT graph_add_edge_label('cc', 'cl', 'cl', 'a', 'cv', 'b', 'cv', directed => false);

SELECT * FROM graph_connected_components('cc');
-- before 2024-06-01 vertex 3 is on its own
SELECT * FROM graph_connected_components('cc', as_of => '2024-01-01');
-- without the undirected link, 7 is alone
SELECT component, array_agg(vertex ORDER BY vertex)
  FROM graph_connected_components('cc', ARRAY['ce'])
 GROUP BY component ORDER BY component;

SELECT * FROM graph_degree('cc');
SELECT * FROM graph_degree('cc', ARRAY['ce'], '2024-01-01');

-- composes with SQL: top vertex by PageRank joined to its degree
SELECT p.vertex, d.degree
  FROM graph_pagerank('pr', edge_labels => ARRAY['pe']) p
  JOIN graph_degree('pr', ARRAY['pe']) d USING (vertex)
 ORDER BY p.rank DESC LIMIT 1;

SELECT graph_drop('pr');
SELECT graph_drop('cc');
SELECT graph_drop('cycle');
DROP TABLE pe, pv, cyc, cyv, ce, cl, cv;
