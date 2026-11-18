--
-- pg_bitemporal regression test
--
CREATE EXTENSION pg_bitemporal;
SET timezone = 'UTC';
SET datestyle = 'ISO, YMD';

CREATE TABLE price (
    product_id int  NOT NULL,
    price      numeric NOT NULL,
    note       text
);

-- unique constraint on the key alone is rejected
CREATE TABLE bad_pk (id int PRIMARY KEY, v int);
SELECT bitemporal_enable('bad_pk', ARRAY['id']);
-- unknown key column
SELECT bitemporal_enable('price', ARRAY['nope']);

SELECT bitemporal_enable('price', ARRAY['product_id']);
SELECT table_name, history_table, key_columns, valid_from_col, valid_to_col
  FROM bitemporal_tables;
\d price

-- ---------------------------------------------------------------
-- 1. Initial load, recorded at system time 2024-01-10
-- ---------------------------------------------------------------
SET pg_bitemporal.system_time = '2024-01-10 00:00:00+00';
INSERT INTO price (product_id, price, valid_from) VALUES
    (1, 100, '2024-01-01'),
    (2,  50, '2024-01-01');
SELECT product_id, price, valid_from, valid_to, sys_from, sys_to
  FROM price ORDER BY product_id, valid_from;

-- overlapping valid period for the same key is rejected
INSERT INTO price (product_id, price, valid_from, valid_to)
    VALUES (1, 999, '2024-06-01', '2024-07-01');
-- empty valid period is rejected
INSERT INTO price (product_id, price, valid_from, valid_to)
    VALUES (3, 1, '2024-06-01', '2024-06-01');

-- ---------------------------------------------------------------
-- 2. Retroactive correction, recorded 2024-02-15: the price of
--    product 1 had really been 110 since 2024-02-01.
-- ---------------------------------------------------------------
SET pg_bitemporal.system_time = '2024-02-15 00:00:00+00';
SELECT bitemporal_update('price', '{"product_id": 1}', '2024-02-01', NULL,
                         '{"price": 110, "note": "correction"}');
SELECT product_id, price, note, valid_from, valid_to, sys_from, sys_to
  FROM price ORDER BY product_id, valid_from;

-- ---------------------------------------------------------------
-- 3. FOR PORTION OF split, recorded 2024-03-20: promotion price 90
--    for product 1 during [2024-03-01, 2024-03-15).
-- ---------------------------------------------------------------
SET pg_bitemporal.system_time = '2024-03-20 00:00:00+00';
SELECT bitemporal_update('price', '{"product_id": "1"}', '2024-03-01', '2024-03-15',
                         '{"price": 90, "note": "promo"}');
SELECT product_id, price, note, valid_from, valid_to, sys_from, sys_to
  FROM price ORDER BY product_id, valid_from;

-- ---------------------------------------------------------------
-- 4. Sequenced delete, recorded 2024-04-01: product 2 discontinued
--    from 2024-04-01 on.
-- ---------------------------------------------------------------
SET pg_bitemporal.system_time = '2024-04-01 00:00:00+00';
SELECT bitemporal_delete('price', '{"product_id": 2}', '2024-04-01', NULL);
-- a non-overlapping new period is fine
INSERT INTO price (product_id, price, valid_from) VALUES (2, 55, '2024-05-01');
SELECT product_id, price, valid_from, valid_to, sys_from
  FROM price ORDER BY product_id, valid_from;

-- history: every superseded version with its system period closed
SELECT product_id, price, note, valid_from, valid_to, sys_from, sys_to
  FROM price_history ORDER BY product_id, sys_from, valid_from;

RESET pg_bitemporal.system_time;

-- ---------------------------------------------------------------
-- 5. "What did we believe on date X about date Y?"
-- ---------------------------------------------------------------
-- before anything was recorded
SELECT product_id, price FROM bitemporal_as_of(NULL::price, '2024-02-10', '2024-01-05');
-- believed on 2024-01-20 about 2024-02-10: original price 100
SELECT product_id, price FROM bitemporal_as_of(NULL::price, '2024-02-10', '2024-01-20')
 ORDER BY product_id;
-- believed on 2024-02-20 about 2024-02-10: corrected price 110
SELECT product_id, price FROM bitemporal_as_of(NULL::price, '2024-02-10', '2024-02-20')
 ORDER BY product_id;
-- believed on 2024-02-20 about 2024-03-05: 110 (promo not yet known)
SELECT product_id, price, note FROM bitemporal_as_of(NULL::price, '2024-03-05', '2024-02-20')
 ORDER BY product_id;
-- believed on 2024-03-25 about 2024-03-05: promo price 90
SELECT product_id, price, note FROM bitemporal_as_of(NULL::price, '2024-03-05', '2024-03-25')
 ORDER BY product_id;
-- believed on 2024-03-25 about 2024-01-15: 100 (before the correction window)
SELECT product_id, price FROM bitemporal_as_of(NULL::price, '2024-01-15', '2024-03-25')
 ORDER BY product_id;
-- believed now about 2024-04-15: product 2 has no valid price
SELECT product_id, price FROM bitemporal_as_of(NULL::price, '2024-04-15')
 ORDER BY product_id;
-- believed now about 2024-06-01
SELECT product_id, price FROM bitemporal_as_of(NULL::price, '2024-06-01')
 ORDER BY product_id;
-- believed on 2024-03-25 about 2024-06-01: product 2 still at 50
SELECT product_id, price FROM bitemporal_as_of(NULL::price, '2024-06-01', '2024-03-25')
 ORDER BY product_id;
-- whole valid timeline of product 1 as known on 2024-02-20
SELECT product_id, price, valid_from, valid_to
  FROM bitemporal_as_of(NULL::price, NULL, '2024-02-20')
 WHERE product_id = 1 ORDER BY valid_from;

-- full bitemporal history of one key
SELECT entity_key, valid_from, valid_to, sys_from, sys_to, is_current, row_data
  FROM bitemporal_history('price', '{"product_id": 1}');
SELECT count(*) AS versions_all_keys FROM bitemporal_history('price');

-- ---------------------------------------------------------------
-- 6. Plain (non-sequenced) DML is versioned too; system time defaults
--    to the transaction timestamp.
-- ---------------------------------------------------------------
BEGIN;
UPDATE price SET note = 'restocked' WHERE product_id = 2 AND valid_from = '2024-05-01';
SELECT note, sys_from = now() AS sys_from_is_now, sys_to = 'infinity' AS open
  FROM price WHERE product_id = 2 AND valid_from = '2024-05-01';
-- second change in the same transaction: no zero-length history version
UPDATE price SET note = 'restocked twice' WHERE product_id = 2 AND valid_from = '2024-05-01';
SELECT count(*) AS hist_rows_closed_now FROM price_history WHERE sys_to = now();
COMMIT;
SELECT price, note FROM bitemporal_as_of(NULL::price, '2024-06-01')
 WHERE product_id = 2;
SELECT price, note FROM bitemporal_as_of(NULL::price, '2024-06-01', '2024-04-02')
 WHERE product_id = 2;

-- direct UPDATE that creates an overlap is rejected
UPDATE price SET valid_to = '2024-06-01' WHERE product_id = 2 AND valid_from = '2024-01-01';

-- system time may not go backwards for a row version
SET pg_bitemporal.system_time = '2023-01-01 00:00:00+00';
DELETE FROM price WHERE product_id = 1;
RESET pg_bitemporal.system_time;

-- TRUNCATE is refused (would lose history)
TRUNCATE price;

-- ---------------------------------------------------------------
-- 7. Error handling of the sequenced functions
-- ---------------------------------------------------------------
SELECT bitemporal_update('price', '{"product_id": 1}', NULL, NULL, '{"product_id": 7}');
SELECT bitemporal_update('price', '{"product_id": 1}', NULL, NULL, '{"nosuch": 7}');
SELECT bitemporal_update('price', '{"id": 1}', NULL, NULL, '{"price": 7}');
SELECT bitemporal_update('price', '{}', NULL, NULL, '{"price": 7}');
SELECT bitemporal_delete('price', '{"product_id": 1}', '2024-05-01', '2024-04-01');
SELECT bitemporal_update('bad_pk', '{"id": 1}', NULL, NULL, '{"v": 1}');
SELECT * FROM bitemporal_as_of(NULL::bad_pk, now());
-- no matching version: nothing happens
SELECT bitemporal_delete('price', '{"product_id": 42}', NULL, NULL);

-- ---------------------------------------------------------------
-- 8. Composite key, custom period column names, pre-existing rows
-- ---------------------------------------------------------------
CREATE TABLE emp_dept (
    company text,
    emp_no  int,
    dept    text,
    eff_from timestamptz,
    eff_to   timestamptz
);
INSERT INTO emp_dept VALUES ('acme', 1, 'R&D', '2020-01-01', NULL);
SET pg_bitemporal.system_time = '2024-01-01 00:00:00+00';
SELECT bitemporal_enable('emp_dept', ARRAY['company', 'emp_no'], 'eff_from', 'eff_to');
SELECT company, emp_no, dept, eff_from, eff_to, sys_from, sys_to FROM emp_dept;
SET pg_bitemporal.system_time = '2024-02-01 00:00:00+00';
-- employee was in Sales during 2021 (split into three versions)
SELECT bitemporal_update('emp_dept', '{"company": "acme", "emp_no": 1}',
                         '2021-01-01', '2022-01-01', '{"dept": "Sales"}');
RESET pg_bitemporal.system_time;
SELECT company, emp_no, dept, eff_from, eff_to FROM emp_dept ORDER BY eff_from;
SELECT dept FROM bitemporal_as_of(NULL::emp_dept, '2021-06-01', '2024-01-15');
SELECT dept FROM bitemporal_as_of(NULL::emp_dept, '2021-06-01', '2024-02-15');

SELECT bitemporal_disable('emp_dept', true);
SELECT to_regclass('emp_dept_history') IS NULL AS history_dropped;
SELECT table_name FROM bitemporal_tables ORDER BY 1;
SELECT bitemporal_enable('price', ARRAY['product_id']);

DROP TABLE emp_dept, bad_pk;
