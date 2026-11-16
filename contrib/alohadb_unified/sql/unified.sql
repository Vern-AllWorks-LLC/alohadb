-- alohadb_unified regression test
-- Tests: unified streaming + batch view, ingest, flush, purge

CREATE EXTENSION alohadb_unified;

-- -----------------------------------------------------------------------
-- Batch table (the "cold" side of the unified view)
-- -----------------------------------------------------------------------

CREATE TABLE orders (
    id       bigserial PRIMARY KEY,
    customer text     NOT NULL,
    amount   numeric  NOT NULL,
    status   text     NOT NULL DEFAULT 'pending'
);

INSERT INTO orders (customer, amount, status) VALUES
    ('alice', 99.00,  'completed'),
    ('bob',   149.50, 'completed'),
    ('carol', 200.00, 'completed');

-- -----------------------------------------------------------------------
-- Register unified source
-- -----------------------------------------------------------------------

SELECT unified_register(
    'orders_stream',
    'orders',
    'public',
    'orders_unified'
) AS registered;

-- -----------------------------------------------------------------------
-- Ingest streaming events into the buffer
-- -----------------------------------------------------------------------

SELECT unified_ingest('orders_stream', '{"id": 4, "customer": "dave",  "amount": 75.00,  "status": "pending"}'::jsonb);
SELECT unified_ingest('orders_stream', '{"id": 5, "customer": "eve",   "amount": 320.00, "status": "pending"}'::jsonb);
SELECT unified_ingest('orders_stream', '{"id": 6, "customer": "frank", "amount": 45.50,  "status": "pending"}'::jsonb);

-- -----------------------------------------------------------------------
-- Query buffered stream events
-- -----------------------------------------------------------------------

SELECT count(*) AS buffered_events
FROM   unified_stream_events('orders_stream');

-- -----------------------------------------------------------------------
-- Unified view: should show 3 batch rows + 3 buffered rows = 6 total
-- -----------------------------------------------------------------------

SELECT count(*) AS total_rows FROM orders_unified;

SELECT customer, amount
FROM   orders_unified
ORDER BY id;

-- -----------------------------------------------------------------------
-- Flush: move buffer into batch table
-- -----------------------------------------------------------------------

SELECT unified_flush('orders_stream') AS rows_flushed;

-- After flush, buffer should be empty
SELECT count(*) AS buffered_after_flush
FROM   unified_stream_events('orders_stream');

-- Batch table now has all 6 rows
SELECT count(*) AS batch_after_flush FROM orders;

-- Unified view still correct
SELECT count(*) AS total_after_flush FROM orders_unified;

-- -----------------------------------------------------------------------
-- Ingest more and purge
-- -----------------------------------------------------------------------

SELECT unified_ingest('orders_stream', '{"id": 7, "customer": "grace", "amount": 10.00, "status": "pending"}'::jsonb);
SELECT unified_purge('orders_stream') AS purged_events;

SELECT count(*) AS buffered_after_purge
FROM   unified_stream_events('orders_stream');

-- -----------------------------------------------------------------------
-- Source list and stats
-- -----------------------------------------------------------------------

SELECT source_name, batch_table, view_name
FROM   unified_list();

SELECT source_name, total_ingested, total_flushed
FROM   unified_stats('orders_stream');

-- -----------------------------------------------------------------------
-- Drop unified source (removes view, not the batch table)
-- -----------------------------------------------------------------------

SELECT unified_drop('orders_stream');

SELECT count(*) AS sources_after_drop FROM alohadb_unified_sources;

-- Batch table still intact
SELECT count(*) AS orders_intact FROM orders;

-- -----------------------------------------------------------------------
-- Cleanup
-- -----------------------------------------------------------------------

DROP TABLE orders CASCADE;
DROP EXTENSION alohadb_unified;
