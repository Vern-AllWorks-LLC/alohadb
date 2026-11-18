--
-- pg_graph: neighbors, BFS, shortest paths, as-of and temporal paths
--
CREATE EXTENSION IF NOT EXISTS pg_graph;
SET timezone = 'UTC';

CREATE TABLE v (id bigint PRIMARY KEY);
INSERT INTO v SELECT generate_series(1, 9);

-- weighted, temporal, directed road network
CREATE TABLE road (src bigint, dst bigint, w float8, vf timestamptz, vt timestamptz);
INSERT INTO road VALUES
    (1, 2, 1, '2024-01-01', NULL),
    (1, 3, 4, '2024-01-02', NULL),
    (2, 3, 1, '2024-01-05', NULL),
    (3, 4, 1, '2024-01-03', NULL),
    (2, 4, 5, '2024-01-04', '2024-01-10'),
    (4, 5, 3, '2024-02-01', NULL),
    (5, 1, 1, '2024-03-01', NULL),
    (6, 7, 2, NULL, NULL);
-- unweighted, undirected, non-temporal links
CREATE TABLE link (a bigint, b bigint);
INSERT INTO link VALUES (1, 8), (8, 5);

SELECT graph_create('g');
SELECT graph_add_vertex_label('g', 'v', 'v');
SELECT graph_add_edge_label('g', 'road', 'road', 'src', 'v', 'dst', 'v',
                            weight_column => 'w', valid_from_column => 'vf',
                            valid_to_column => 'vt');
SELECT graph_add_edge_label('g', 'link', 'link', 'a', 'v', 'b', 'v', directed => false);

-- ---------------- neighbors ----------------
SELECT * FROM graph_neighbors('g', 2, ARRAY['road']);
SELECT * FROM graph_neighbors('g', 2, ARRAY['road'], 'in');
SELECT neighbor, edge_label, edge_direction FROM graph_neighbors('g', 1, direction => 'both');
-- as of 2024-01-04 12:00 only 2->4 exists (2->3 starts 01-05)
SELECT neighbor, weight FROM graph_neighbors('g', 2, as_of => '2024-01-04 12:00');
-- as of 2024-01-11 the 2->4 edge has expired
SELECT neighbor, weight FROM graph_neighbors('g', 2, as_of => '2024-01-11');
SELECT * FROM graph_neighbors('g', 1, ARRAY['nosuch']);            -- error
SELECT * FROM graph_neighbors('g', 1, direction => 'sideways');    -- error
SELECT * FROM graph_neighbors('g', NULL);                          -- empty

-- ---------------- BFS ----------------
SELECT * FROM graph_bfs('g', 1, edge_labels => ARRAY['road']);
SELECT * FROM graph_bfs('g', 1, 1, ARRAY['road']);
-- both labels: the undirected link 1-8-5 reaches 5 at depth 2
SELECT * FROM graph_bfs('g', 1, 2);
-- point in time: before 01-04 the only way to 4 is via 3
SELECT * FROM graph_bfs('g', 1, 3, ARRAY['road'], '2024-01-03 12:00');
-- after 2->4 expired (01-10), 4 is again reached via 3; 5 not yet (02-01)
SELECT * FROM graph_bfs('g', 1, 3, ARRAY['road'], '2024-01-11');
-- reverse direction
SELECT * FROM graph_bfs('g', 4, 3, ARRAY['road'], direction => 'in');
SELECT * FROM graph_bfs('g', 7, 3, ARRAY['road'], direction => 'both');
-- unlimited depth, start without edges
SELECT * FROM graph_bfs('g', 1, NULL, ARRAY['road']);
SELECT * FROM graph_bfs('g', 9, 3);
SELECT * FROM graph_bfs('g', 1, -1);                               -- error
-- composes with SQL: vertices per depth
SELECT depth, count(*), array_agg(vertex ORDER BY vertex)
  FROM graph_bfs('g', 1, 5, ARRAY['road']) GROUP BY depth ORDER BY depth;

-- ---------------- shortest paths ----------------
-- 1-2-3-4-5 costs 1+1+1+3 = 6 (1-3-4-5 = 8, 1-2-4-5 = 9)
SELECT * FROM graph_shortest_path('g', 1, 5, ARRAY['road']);
-- with the unweighted link (weight 1 per hop) 1-8-5 costs 2
SELECT * FROM graph_shortest_path('g', 1, 5);
-- hop count only
SELECT * FROM graph_shortest_path('g', 1, 5, ARRAY['link']);
-- as of 01-04 12:00: 1-3-4 = 5 beats 1-2-4 = 6 (2-3 not yet valid)
SELECT * FROM graph_shortest_path('g', 1, 4, ARRAY['road'], '2024-01-04 12:00');
-- reverse direction: 5 <- 4 <- 3 <- 2 <- 1
SELECT * FROM graph_shortest_path('g', 5, 1, ARRAY['road'], direction => 'in');
-- unreachable, unknown vertex, trivial
SELECT * FROM graph_shortest_path('g', 5, 6, ARRAY['road']);
SELECT * FROM graph_shortest_path('g', 1, 42);
SELECT * FROM graph_shortest_path('g', 3, 3);
-- negative weights are rejected
BEGIN;
INSERT INTO road VALUES (3, 5, -1, NULL, NULL);
SELECT * FROM graph_shortest_path('g', 1, 5, ARRAY['road']);       -- error
ROLLBACK;

-- ---------------- time-respecting paths ----------------
-- from 1 at 2024-01-01: 3 is reached at 01-02 directly (1-2-3 would be 01-05),
-- 4 at 01-03 via 3 (statically shortest 1-2-4 arrives later, 01-04)
SELECT * FROM graph_temporal_paths('g', 1, '2024-01-01', 10, ARRAY['road']);
-- starting after the 1->2 contact makes 2 unreachable
SELECT * FROM graph_temporal_paths('g', 1, '2024-01-01 12:00', 10, ARRAY['road']);
-- from 2: 3 at 01-05, but 3->4 (01-03) is earlier, so 4 only via 2->4 at 01-04;
-- 1 is reached at 03-01 via 5, and cannot continue (its edges are older)
SELECT * FROM graph_temporal_paths('g', 2, '2024-01-01', 10, ARRAY['road']);
-- hop bound
SELECT * FROM graph_temporal_paths('g', 2, '2024-01-01', 1, ARRAY['road']);
-- edges without a time (links, 6->7) do not take part
SELECT * FROM graph_temporal_paths('g', 6, '2000-01-01');
-- every reported path is time-respecting: edge times are non-decreasing
SELECT t.vertex, bool_and(r.vf >= coalesce(prev_vf, '-infinity')) AS ok
  FROM graph_temporal_paths('g', 2, '2024-01-01', 10, ARRAY['road']) t,
       LATERAL (SELECT r.vf, lag(r.vf) OVER (ORDER BY i) AS prev_vf
                  FROM generate_series(1, t.hops) i
                  JOIN road r ON r.src = t.path[i] AND r.dst = t.path[i + 1]) r
 GROUP BY t.vertex ORDER BY t.vertex;
SELECT * FROM graph_temporal_paths('g', 1, NULL);                  -- error

-- ---------------- row-level security applies ----------------
CREATE ROLE regress_graph_reader;
GRANT SELECT ON v, road, link TO regress_graph_reader;
ALTER TABLE road ENABLE ROW LEVEL SECURITY;
CREATE POLICY hide_two ON road FOR SELECT USING (src <> 2 AND dst <> 2);
SET ROLE regress_graph_reader;
SELECT * FROM graph_bfs('g', 1, 3, ARRAY['road']);
SELECT * FROM graph_shortest_path('g', 1, 5, ARRAY['road']);
RESET ROLE;
DROP POLICY hide_two ON road;
ALTER TABLE road DISABLE ROW LEVEL SECURITY;
REVOKE ALL ON v, road, link FROM regress_graph_reader;
DROP ROLE regress_graph_reader;

SELECT graph_drop('g');
DROP TABLE road, link, v;
