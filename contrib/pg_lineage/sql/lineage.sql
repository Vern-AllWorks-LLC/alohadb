--
-- pg_lineage regression test (pg_temporal system-versioned tables)
--
CREATE EXTENSION pg_temporal;
CREATE EXTENSION pg_lineage;

CREATE TABLE customers (id int PRIMARY KEY, name text NOT NULL, segment text);
SELECT pg_enable_system_versioning('customers');
INSERT INTO customers VALUES (1, 'Ada', 'smb'), (2, 'Bob', 'ent'), (3, 'Cy', NULL);

-- unversioned tables are rejected
CREATE TABLE plain (id int PRIMARY KEY);
SELECT * FROM lineage_snapshot('bad', ARRAY['plain'::regclass]);
SELECT * FROM lineage_snapshot('bad', ARRAY[]::regclass[]);

-- ---------------------------------------------------------------
-- Snapshot, mutate, read back, verify
-- ---------------------------------------------------------------
SELECT * FROM lineage_snapshot('train_v3', ARRAY['customers'::regclass], 'training set for churn v3');
SELECT * FROM lineage_snapshot('train_v3', ARRAY['customers'::regclass]);

UPDATE customers SET name = 'Ada L.' WHERE id = 1;
DELETE FROM customers WHERE id = 2;
INSERT INTO customers VALUES (4, 'Dee', 'smb');

SELECT id, name, segment FROM customers ORDER BY id;
-- the snapshot still reads the old data
SELECT id, name, segment FROM lineage_read_snapshot(NULL::customers, 'train_v3') ORDER BY id;
-- ... and verifies
SELECT table_name, row_count_then, row_count_now, hash_then = hash_now AS same_hash, matches
  FROM lineage_verify('train_v3');

-- a later snapshot sees the new state and has a different fingerprint
SELECT table_name, kind, row_count FROM lineage_snapshot('after_edit', ARRAY['customers'::regclass]);
SELECT a.content_hash <> b.content_hash AS hash_differs
  FROM lineage_snapshot_tables a, lineage_snapshot_tables b
 WHERE a.snapshot_name = 'train_v3' AND b.snapshot_name = 'after_edit';

-- the hash is order independent and content sensitive
CREATE TABLE h1 (k int PRIMARY KEY, v text);
CREATE TABLE h2 (k int PRIMARY KEY, v text);
SELECT pg_enable_system_versioning('h1');
SELECT pg_enable_system_versioning('h2');
INSERT INTO h1 SELECT g, 'v' || g FROM generate_series(1, 50) g;
INSERT INTO h2 SELECT g, 'v' || g FROM generate_series(50, 1, -1) g;
SELECT count(DISTINCT content_hash) AS distinct_hashes, sum(row_count) AS rows
  FROM lineage_snapshot('h_same', ARRAY['h1'::regclass, 'h2'::regclass]);
UPDATE h2 SET v = 'changed' WHERE k = 7;
SELECT count(DISTINCT content_hash) AS distinct_hashes
  FROM lineage_snapshot('h_diff', ARRAY['h1'::regclass, 'h2'::regclass]);

-- tampering with the history is detected
UPDATE customers_history SET name = 'Bobby' WHERE id = 2;
SELECT table_name, matches FROM lineage_verify('train_v3');
UPDATE customers_history SET name = 'Bob' WHERE id = 2;
SELECT table_name, matches FROM lineage_verify('train_v3');
SELECT lineage_verify('nope');

-- ---------------------------------------------------------------
-- Model linkage
-- ---------------------------------------------------------------
SELECT lineage_link_model('churn', 'v3', 'train_v3', '{"auc": 0.91}');
SELECT lineage_link_model('churn', 'v3', 'train_v3', '{"auc": 0.92}');   -- metadata refresh
SELECT lineage_link_model('churn', 'v3', 'after_edit');                    -- refused
SELECT lineage_link_model('churn', 'v4', 'after_edit');
SELECT lineage_link_model('churn', 'v5', 'missing');
SELECT model, version, snapshot_name, metadata FROM lineage_model_links ORDER BY 1, 2;
SELECT lineage_model_snapshot('churn', 'v3') AS v3, lineage_model_snapshot('churn', 'v4') AS v4,
       lineage_model_snapshot('churn', 'v9') AS v9;
-- "what did the dataset look like when model v3 was trained?"
SELECT r FROM lineage_read('customers', 'churn', 'v3') r ORDER BY r ->> 'id';
SELECT r FROM lineage_read('customers', 'churn', 'v4') r ORDER BY r ->> 'id';
SELECT id, name FROM lineage_read_snapshot(NULL::customers, lineage_model_snapshot('churn', 'v3'))
 ORDER BY id;
SELECT * FROM lineage_read('customers', 'churn', 'v9');
SELECT snapshot_name, note, tables, total_rows, models FROM lineage_snapshots();
SELECT lineage_snapshot_drop('train_v3');
SELECT lineage_snapshot_drop('h_diff');
SELECT lineage_unlink_model('churn', 'v4'), lineage_unlink_model('churn', 'v4');

-- ---------------------------------------------------------------
-- Zero-copy branches
-- ---------------------------------------------------------------
SELECT lineage_branch_create('exp1', 'plain');
CREATE TABLE nopk (id int);
SELECT pg_enable_system_versioning('nopk');
SELECT lineage_branch_create('exp1', 'nopk');

-- base now: 1 Ada L., 3 Cy, 4 Dee
SELECT lineage_branch_create('exp1', 'customers');
SELECT lineage_branch_create('exp1', 'customers');
-- the branch copies nothing
SELECT count(*) AS delta_rows FROM customers__exp1__delta;

INSERT INTO customers__exp1 (id, name, segment) VALUES (10, 'Branchy', 'smb');
UPDATE customers__exp1 SET segment = 'vip' WHERE id = 3;
DELETE FROM customers__exp1 WHERE id = 4;
INSERT INTO customers__exp1 (id, name) VALUES (1, 'dup');       -- duplicate key
INSERT INTO customers__exp1 (id, name) VALUES (11, 'Temp');
UPDATE customers__exp1 SET id = 12 WHERE id = 11;               -- key change
DELETE FROM customers__exp1 WHERE id = 12;                      -- branch-only row removed

SELECT id, name, segment FROM customers__exp1 ORDER BY id;
-- base unchanged by branch writes
SELECT id, name, segment FROM customers ORDER BY id;

-- base changes after the branch point are not visible in the branch
UPDATE customers SET name = 'Ada Lovelace' WHERE id = 1;
INSERT INTO customers VALUES (5, 'Eve', 'ent');
SELECT id, name, segment FROM customers__exp1 ORDER BY id;

SELECT change, key, base_row, branch_row FROM lineage_branch_diff('exp1', 'customers');
SELECT branch, base_table, view_name, from_snapshot, status, delta_rows, tombstones
  FROM lineage_branches();

-- merge: base row 1 changed since the branch point, but exp1 did not touch it
SELECT * FROM lineage_branch_merge('exp1', 'customers');
SELECT id, name, segment FROM customers ORDER BY id;
-- merged changes went through system versioning (old versions in history)
SELECT id, name, segment FROM customers_history WHERE id IN (3, 4) ORDER BY id;
-- merged branch is read-only and cannot be merged twice
INSERT INTO customers__exp1 (id, name) VALUES (20, 'late');
SELECT * FROM lineage_branch_merge('exp1', 'customers');

-- ---------------------------------------------------------------
-- Branch from a snapshot + conflict detection
-- ---------------------------------------------------------------
SELECT lineage_branch_create('fromsnap', 'customers', 'train_v3');
SELECT id, name, segment FROM customers__fromsnap ORDER BY id;     -- data as of train_v3
UPDATE customers__fromsnap SET segment = 'gold' WHERE id IN (1, 3);
INSERT INTO customers__fromsnap (id, name) VALUES (30, 'Fresh');
SELECT change, key FROM lineage_branch_diff('fromsnap', 'customers');
-- ids 1 and 3 changed in the base since train_v3 -> conflict
SELECT * FROM lineage_branch_merge('fromsnap', 'customers');
SELECT id, name, segment FROM customers ORDER BY id;               -- untouched
SELECT * FROM lineage_branch_merge('fromsnap', 'customers', force => true);
SELECT id, name, segment FROM customers ORDER BY id;

-- dropping branches removes view, delta and tombstone tables
SELECT lineage_branch_drop('exp1', 'customers');
SELECT lineage_branch_drop('fromsnap', 'customers');
SELECT lineage_branch_drop('fromsnap', 'customers');
SELECT count(*) AS leftover FROM pg_class WHERE relname LIKE 'customers\_\_%';
SELECT count(*) AS branches FROM lineage_branches();

-- the training snapshot still verifies after all of this
SELECT table_name, matches FROM lineage_verify('train_v3');
