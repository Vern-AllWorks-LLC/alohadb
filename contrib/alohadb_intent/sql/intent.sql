-- alohadb_intent regression test
-- Tests: intent registry, document tagging, inference, hierarchy

CREATE EXTENSION alohadb_intent;

-- -----------------------------------------------------------------------
-- Register intent hierarchy
-- -----------------------------------------------------------------------

-- Root intents
SELECT intent_register('customer_service', 'General customer service interactions', ARRAY['help','support','service'], NULL);
SELECT intent_register('purchase',         'Purchase-related intent',               ARRAY['buy','order','purchase','checkout','price','cost'], 'customer_service');
SELECT intent_register('complaint',        'Complaint or dissatisfaction',          ARRAY['broken','damaged','wrong','bad','terrible','refund','return'], 'customer_service');
SELECT intent_register('inquiry',          'General information inquiry',           ARRAY['what','how','when','where','tell me','explain'], 'customer_service');
SELECT intent_register('compliment',       'Positive feedback',                     ARRAY['great','love','excellent','amazing','thank'], 'customer_service');

-- -----------------------------------------------------------------------
-- Tag documents explicitly
-- -----------------------------------------------------------------------

SELECT intent_tag(1, 'I want to buy the new laptop, what is the price?', 'purchase');
SELECT intent_tag(2, 'My order arrived broken, I want a refund',         'complaint');
SELECT intent_tag(3, 'How do I track my order?',                         'inquiry');
SELECT intent_tag(4, 'This product is amazing, thank you!',              'compliment');

-- -----------------------------------------------------------------------
-- Query document intents
-- -----------------------------------------------------------------------

SELECT doc_id, intent_name
FROM   intent_of(1);

SELECT doc_id, intent_name
FROM   intent_of(2);

-- -----------------------------------------------------------------------
-- Find documents by intent
-- -----------------------------------------------------------------------

SELECT doc_id
FROM   intent_find('purchase')
ORDER BY doc_id;

SELECT doc_id
FROM   intent_find('complaint')
ORDER BY doc_id;

-- -----------------------------------------------------------------------
-- Intent inference (keyword scoring)
-- -----------------------------------------------------------------------

SELECT intent_name, round(score::numeric, 3) AS score
FROM   intent_infer('I need to return this broken item and get my money back')
WHERE  score > 0
ORDER BY score DESC, intent_name
LIMIT 5;

SELECT intent_name, round(score::numeric, 3) AS score
FROM   intent_infer('What time does the store close?')
WHERE  score > 0
ORDER BY score DESC, intent_name
LIMIT 5;

-- -----------------------------------------------------------------------
-- Auto-tag
-- -----------------------------------------------------------------------

SELECT intent_auto_tag(5, 'I love this product, great quality!');
SELECT intent_name FROM intent_of(5);

-- -----------------------------------------------------------------------
-- Intent hierarchy ancestors
-- -----------------------------------------------------------------------

SELECT ancestor, depth
FROM   intent_ancestors('purchase')
ORDER BY depth;

SELECT ancestor, depth
FROM   intent_ancestors('complaint')
ORDER BY depth;

-- -----------------------------------------------------------------------
-- Statistics
-- -----------------------------------------------------------------------

SELECT total_intents, total_documents, total_tags
FROM   intent_stats();

-- -----------------------------------------------------------------------
-- Untag
-- -----------------------------------------------------------------------

SELECT intent_untag(1, 'purchase');

SELECT count(*) AS tags_after_untag
FROM   intent_of(1);

-- -----------------------------------------------------------------------
-- Cleanup
-- -----------------------------------------------------------------------

TRUNCATE alohadb_intent_registry, alohadb_intent_documents CASCADE;
DROP EXTENSION alohadb_intent;
