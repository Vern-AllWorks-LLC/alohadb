-- System-versioned tables: repeated updates of a keyed row, time travel,
-- and the 1.0 -> 1.1 upgrade that repairs history tables.
CREATE EXTENSION alohadb_temporal VERSION '1.0';

-- A table versioned under 1.0 got the base table's primary key on its history.
CREATE TABLE legacy (id int PRIMARY KEY, v int);
SELECT alohadb_enable_system_versioning('legacy');
-- (the fixed library no longer copies it, so recreate what a 1.0 install left behind)
ALTER TABLE legacy_history ADD PRIMARY KEY (id);
SELECT count(*) AS unique_constraints FROM pg_constraint
 WHERE conrelid = 'legacy_history'::regclass AND contype IN ('p', 'u');

ALTER EXTENSION alohadb_temporal UPDATE TO '1.1';
SELECT count(*) AS unique_constraints FROM pg_constraint
 WHERE conrelid = 'legacy_history'::regclass AND contype IN ('p', 'u');
INSERT INTO legacy VALUES (1, 1);
UPDATE legacy SET v = 2;
UPDATE legacy SET v = 3;
SELECT count(*) AS history_versions FROM legacy_history WHERE id = 1;

-- New tables (1.1): history has no keys; identity/generated columns archive verbatim.
CREATE TABLE item (
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL,
    price numeric,
    price_x2 numeric GENERATED ALWAYS AS (price * 2) STORED
);
SELECT alohadb_enable_system_versioning('item');
SELECT count(*) AS unique_constraints FROM pg_constraint
 WHERE conrelid = 'item_history'::regclass AND contype IN ('p', 'u');

INSERT INTO item (name, price) VALUES ('widget', 10);
SELECT pg_sleep(0.01);
SELECT clock_timestamp() AS t1 \gset
SELECT pg_sleep(0.01);
UPDATE item SET price = 11;
SELECT clock_timestamp() AS t2 \gset
SELECT pg_sleep(0.01);
UPDATE item SET price = 12;
UPDATE item SET price = 13;
DELETE FROM item;

SELECT name, price, price_x2 FROM item_history ORDER BY row_start;
SELECT name, price FROM alohadb_as_of('item', :'t1') AS t(id int, name text, price numeric, price_x2 numeric, row_start timestamptz, row_end timestamptz);
SELECT name, price FROM alohadb_as_of('item', :'t2') AS t(id int, name text, price numeric, price_x2 numeric, row_start timestamptz, row_end timestamptz);
SELECT count(*) AS current_rows FROM item;
