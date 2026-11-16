-- alohadb_intent regression test

CREATE EXTENSION alohadb_intent;
SELECT extversion FROM pg_extension WHERE extname = 'alohadb_intent';

SELECT intent_register('customer_service', 'Customer service interaction', ARRAY['help', 'support']);
SELECT intent_register('purchase',  'Purchase intent',  ARRAY['buy', 'order', 'price', 'checkout'], 'customer_service');
SELECT intent_register('complaint', 'Complaint',        ARRAY['broken', 'damaged', 'refund', 'return'], 'customer_service');
SELECT intent_register('billing_dispute', 'Billing dispute', ARRAY['invoice', 'charged twice', 'refund'], 'complaint');
SELECT intent_register('fifty_percent', 'Discount talk', ARRAY['50%', 'half_off']);

-- Cycles in the hierarchy are rejected
SELECT intent_register('customer_service', 'Customer service interaction', ARRAY['help'], 'billing_dispute');
SELECT intent_register('purchase', 'Purchase intent', ARRAY['buy'], 'purchase');

SELECT * FROM intent_ancestors('billing_dispute');

-- Manual tagging
SELECT intent_tag('t1', 'tickets', 'purchase', 0.9);
SELECT intent_tag('t2', 'tickets', 'complaint');
SELECT intent_tag('t2', 'tickets', 'billing_dispute', 0.8, '{"src":"manual"}');
SELECT intent_name, confidence, parent_intent, metadata FROM intent_of('t2', 'tickets');

-- intent_find with its default (NULL) source table
SELECT doc_id, source_table, confidence FROM intent_find('purchase');
SELECT doc_id FROM intent_find('complaint', 'tickets');
SELECT count(*) FROM intent_find('complaint', 'emails');

-- Inference: whole words only; LIKE wildcards in keywords are literal
SELECT intent_name, hit_count, round(confidence::numeric, 3) AS conf
FROM   intent_infer('I was charged twice on my invoice and want a refund');
SELECT intent_name FROM intent_infer('the reorder button is broken');   -- "reorder" is not "order"
SELECT intent_name FROM intent_infer('get 50% off today');
SELECT intent_name FROM intent_infer('get 50 percent off today');        -- "%" is not a wildcard
SELECT intent_name FROM intent_infer('halfXoff deal');                   -- "_" is not a wildcard

-- Auto-tag with default (NULL) metadata
SELECT intent_auto_tag('t3', 'tickets', 'I want to buy it, what is the price at checkout?');
SELECT intent_name, round(confidence::numeric, 2) FROM intent_of('t3', 'tickets');

-- Stats (previously failed on chr(0))
SELECT registered_intents, tagged_documents, round(avg_confidence::numeric, 3)
FROM   intent_stats();

SELECT intent_untag('t1', 'tickets', 'purchase') AS untagged;
SELECT count(*) FROM intent_of('t1', 'tickets');

SELECT c.relname
FROM   pg_extension e, unnest(e.extconfig) AS cfg(relid)
JOIN   pg_class c ON c.oid = cfg.relid
WHERE  e.extname = 'alohadb_intent'
ORDER BY 1;

DROP EXTENSION alohadb_intent;

-- Upgrade path 1.0 -> 1.1
CREATE EXTENSION alohadb_intent VERSION '1.0';
SELECT intent_register('a', 'A', ARRAY['alpha']);
SELECT intent_tag('d1', 't', 'a');
ALTER EXTENSION alohadb_intent UPDATE TO '1.1';
SELECT * FROM intent_stats();
SELECT intent_auto_tag('d2', 't', 'alpha release');
SELECT intent_register('a', 'A', ARRAY['alpha'], 'a');
DROP EXTENSION alohadb_intent;
