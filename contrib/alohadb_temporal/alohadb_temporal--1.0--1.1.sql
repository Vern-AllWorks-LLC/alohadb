/* contrib/alohadb_temporal/alohadb_temporal--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION alohadb_temporal UPDATE TO '1.1'" to load this file. \quit

-- 1.0 created history tables with LIKE ... INCLUDING ALL, which copied the
-- base table's PRIMARY KEY / UNIQUE constraints and indexes.  A history table
-- must hold many versions of a key, so with those in place the second UPDATE
-- of a row failed with a unique violation.  Remove them from every registered
-- history table (CHECK and NOT NULL constraints are left alone).
DO $$
DECLARE
    t record;
    hist regclass;
    c record;
BEGIN
    FOR t IN SELECT table_name, history_table FROM alohadb_temporal_tables LOOP
        hist := to_regclass(format('%I.%I',
                    (SELECT n.nspname FROM pg_class r JOIN pg_namespace n ON n.oid = r.relnamespace
                      WHERE r.oid = t.table_name), t.history_table));
        CONTINUE WHEN hist IS NULL;
        FOR c IN SELECT conname FROM pg_constraint
                  WHERE conrelid = hist AND contype IN ('p', 'u', 'x') LOOP
            EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', hist, c.conname);
        END LOOP;
        FOR c IN SELECT i.indexrelid::regclass AS idx FROM pg_index i
                  WHERE i.indrelid = hist AND i.indisunique LOOP
            EXECUTE format('DROP INDEX %s', c.idx);
        END LOOP;
    END LOOP;
END
$$;
