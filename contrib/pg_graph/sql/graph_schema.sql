--
-- pg_graph: graph definition, enforced schema, validation
--
CREATE EXTENSION pg_graph;

CREATE TABLE person (id bigint PRIMARY KEY, name text, age int);
CREATE TABLE company (id bigint PRIMARY KEY, name text);
CREATE TABLE knows (src bigint, dst bigint, w float8,
                    since timestamptz, until timestamptz);
CREATE TABLE works_at (person_id bigint, company_id bigint);
CREATE TABLE no_pk (id bigint);

INSERT INTO person VALUES (1, 'ann', 30), (2, 'bob', 40), (3, 'cy', 25);
INSERT INTO company VALUES (101, 'acme'), (102, 'globex');
INSERT INTO knows VALUES (1, 2, 1.0, '2024-01-01', NULL), (2, 3, 2.0, '2024-01-02', '2024-06-01');
INSERT INTO works_at VALUES (1, 101), (2, 102), (3, 102);

SELECT graph_create('social');
SELECT graph_create('social');                                     -- error: exists
SELECT graph_add_vertex_label('social', 'person', 'person');
SELECT graph_add_vertex_label('social', 'company', 'company', 'id');
SELECT graph_add_vertex_label('social', 'bad', 'no_pk');           -- error: no PK
SELECT graph_add_vertex_label('social', 'person', 'company');      -- error: dup label
SELECT graph_add_vertex_label('social', 'x', 'person', 'name');    -- error: key type
SELECT graph_add_vertex_label('nosuch', 'x', 'person');            -- error: no graph

SELECT graph_add_edge_label('social', 'knows', 'knows', 'src', 'person', 'dst', 'person',
                            weight_column => 'w', valid_from_column => 'since',
                            valid_to_column => 'until');
SELECT graph_add_edge_label('social', 'works_at', 'works_at', 'person_id', 'person',
                            'company_id', 'company', on_delete_cascade => true);
SELECT graph_add_edge_label('social', 'oops', 'knows', 'src', 'person', 'nope', 'person'); -- error
SELECT graph_add_edge_label('social', 'oops', 'knows', 'src', 'robot', 'dst', 'person');   -- error

SELECT kind, label, table_name, definition FROM graph_describe('social');

-- installed constraints
SELECT conname, contype, pg_get_constraintdef(oid)
  FROM pg_constraint WHERE conrelid IN ('knows'::regclass, 'works_at'::regclass)
 ORDER BY conname;

-- enforced schema: an edge can never point to a missing vertex
INSERT INTO knows VALUES (1, 99, 1, '2024-01-01', NULL);           -- error: FK
INSERT INTO knows VALUES (NULL, 2, 1, '2024-01-01', NULL);         -- error: NOT NULL
INSERT INTO knows VALUES (1, 3, 1, '2024-05-01', '2024-01-01');    -- error: interval
INSERT INTO works_at VALUES (1, 103);                              -- error: FK
DELETE FROM person WHERE id = 3;                                   -- error: referenced
-- ON DELETE CASCADE removes the edges of a deleted company
DELETE FROM company WHERE id = 102;
SELECT * FROM works_at ORDER BY person_id;

-- property rules
SELECT graph_add_property_rule('social', 'person', 'name');
SELECT graph_add_property_rule('social', 'person', 'age', false, 'age >= 0');
SELECT graph_add_property_rule('social', 'person', 'age', false, 'true; DROP TABLE person'); -- error
SELECT graph_add_property_rule('social', 'nolabel', 'age');        -- error
INSERT INTO person VALUES (4, NULL, 20);                           -- error: required
INSERT INTO person VALUES (4, 'dee', -1);                          -- error: check
INSERT INTO person VALUES (4, 'dee', 50);

-- clean data validates
SELECT * FROM graph_validate('social');

-- ------------------------------------------------------------------
-- validation of a non-enforced graph over messy data
-- ------------------------------------------------------------------
CREATE TABLE thing (id bigint PRIMARY KEY);
INSERT INTO thing VALUES (1), (500);
CREATE TABLE loose_edges (a bigint, b bigint, w float8, t0 timestamptz, t1 timestamptz);
INSERT INTO loose_edges VALUES
    (1, 2, 1, '2024-01-01', '2024-02-01'),    -- fine
    (1, 101, 1, NULL, NULL),                  -- dst is a company: wrong label
    (1, 999, 1, NULL, NULL),                  -- dangling dst
    (777, 2, -3, NULL, NULL),                 -- dangling src, negative weight
    (2, NULL, 1, NULL, NULL),                 -- NULL dst
    (2, 3, 1, '2024-03-01', '2024-02-01');    -- invalid interval
SELECT graph_create('messy');
SELECT graph_add_vertex_label('messy', 'person', 'person');
SELECT graph_add_vertex_label('messy', 'company', 'company');
SELECT graph_add_vertex_label('messy', 'thing', 'thing');
SELECT graph_add_edge_label('messy', 'rel', 'loose_edges', 'a', 'person', 'b', 'person',
                            weight_column => 'w', valid_from_column => 't0',
                            valid_to_column => 't1', enforce => false);
-- no constraints were installed
SELECT count(*) FROM pg_constraint WHERE conrelid = 'loose_edges'::regclass;
SELECT graph_add_property_rule('messy', 'thing', 'id', true, 'id < 1000');
ALTER TABLE thing DROP CONSTRAINT graph_messy_thing_id_ck;
INSERT INTO thing VALUES (1500);
SELECT graph_add_property_rule('messy', 'company', 'name');
ALTER TABLE company ALTER COLUMN name DROP NOT NULL;
INSERT INTO company VALUES (103, NULL);
SELECT * FROM graph_validate('messy');

-- ------------------------------------------------------------------
-- graph_drop removes the constraints it installed, not the data
-- ------------------------------------------------------------------
SELECT graph_drop('social');
SELECT conname FROM pg_constraint
 WHERE conrelid IN ('knows'::regclass, 'works_at'::regclass, 'person'::regclass)
   AND contype IN ('f', 'c');
SELECT count(*) FROM knows;
SELECT graph_drop('social');                                       -- error
SELECT graph_neighbors('social', 1);                               -- error
SELECT graph_drop('messy');
SELECT count(*) FROM pg_graph_graphs;

-- extension can be dropped cleanly
DROP EXTENSION pg_graph;
CREATE EXTENSION pg_graph;
DROP TABLE person, company, knows, works_at, no_pk, thing, loose_edges;
