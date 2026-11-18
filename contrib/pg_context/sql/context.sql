-- pg_context regression test

CREATE EXTENSION pg_ontology;
CREATE EXTENSION pg_context;
SELECT extversion FROM pg_extension WHERE extname = 'pg_context';

-- Word/phrase matching
SELECT context_mentions('Apple shipped a new iPhone', 'apple')  AS t1,
       context_mentions('pineapple juice', 'apple')             AS f1,
       context_mentions('Ask Apple (fruit) growers', 'Apple (fruit)') AS t2,
       context_mentions('price is $5.00', '5.00')               AS t3,
       context_mentions('price is 5x00', '5.00')                AS f2;

-- Two senses of "Apple": our customer, and the produce we sell
SELECT context_register('Apple', 'customer:42', 'customer',
                        'Apple Inc. account', 100, 'sales',
                        ARRAY['invoice', 'contract', 'account']);
SELECT context_register('Apple', 'produce:apple', 'product',
                        'Fresh fruit SKU', 150, 'grocery',
                        ARRAY['fruit', 'orchard', 'crate', 'ripe']);
SELECT context_register('Mercury', 'planet:mercury', 'planet');
SELECT context_alias('AAPL', 'customer:42');
SELECT context_alias('Apple Computer', 'customer:42');

-- Ontology knowledge also serves as context
SELECT ontology_assert('customer:42', 'headquartersIn', 'Cupertino');
SELECT ontology_type_assert('produce:apple', 'Fruit');

-- No context: priority decides
SELECT canonical_entity, resolution_path FROM context_resolve('Apple');
-- Context decides, regardless of priority
SELECT canonical_entity, score FROM context_resolve('Apple', 'Renew the Apple contract and send the invoice');
SELECT canonical_entity, score FROM context_resolve('apple', 'a crate of ripe apple from the orchard');
SELECT canonical_entity FROM context_resolve('Apple', 'meeting with Apple in Cupertino');
SELECT canonical_entity FROM context_resolve('apple', 'is an apple a fruit?');
-- All candidates, ranked
SELECT canonical_entity, resolution_path, score
FROM   context_resolve('Apple', 'ripe fruit', 5);
-- Case and alias handling
SELECT canonical_entity, resolution_path FROM context_resolve('APPLE');
SELECT canonical_entity, resolution_path FROM context_resolve('aapl');
SELECT canonical_entity, resolution_path FROM context_resolve('Apple Computer');
SELECT count(*) AS unknown FROM context_resolve('Banana');

SELECT * FROM context_disambiguate(ARRAY['Apple', 'Mercury', 'Banana', 'AAPL'],
                                   'the apple orchard');

SELECT * FROM context_extract('AAPL and Apple met near Mercury; pineapple') ORDER BY 1;

-- Reranking vector-search hits: the 0.94 hit about fruit must lose to the
-- account hit when the query is about the customer
SELECT id, original_score, round(final_score::numeric, 3) AS final, matched_entity
FROM   context_rerank(
         '[{"id":"d1","text":"Apple orchards report a ripe fruit harvest","score":0.94},
           {"id":"d2","text":"Apple renewed its contract; invoice attached","score":0.81},
           {"id":"d3","text":"Quarterly AAPL account review","score":0.70},
           {"id":"d4","text":"Unrelated weather report","score":0.60}]',
         'status of the Apple contract invoice');
-- ...and the reverse for a produce query
SELECT id, round(final_score::numeric, 3) AS final, matched_entity
FROM   context_rerank(
         '[{"id":"d1","text":"Apple orchards report a ripe fruit harvest","score":0.70},
           {"id":"d2","text":"Apple renewed its contract; invoice attached","score":0.90}]',
         'how many crates of apple fruit are ripe');
-- Query without registered terms leaves scores unchanged
SELECT id, final_score FROM context_rerank('[{"id":"x","text":"Apple","score":0.5}]', 'weather');
SELECT * FROM context_rerank('{"id":1}', 'Apple');   -- error: not an array

-- Listing, history, retraction
SELECT term, canonical_entity, entity_type, domain, keywords, priority FROM context_list();
SELECT event_type, new_value->>'domain' AS domain FROM context_history('produce:apple');
SELECT context_register('Apple', 'produce:apple', 'product', 'Fresh fruit SKU', 50, 'grocery',
                        ARRAY['fruit']);
SELECT event_type, old_value->>'priority' AS old_p, new_value->>'priority' AS new_p
FROM   context_history('produce:apple');
SELECT context_retract('Apple', 'produce:apple') AS retracted;
SELECT canonical_entity FROM context_resolve('Apple', 'ripe fruit');
SELECT context_retract('Apple') AS retracted_all;
SELECT count(*) FROM context_resolve('Apple');
SELECT event_type FROM context_history('customer:42');

DROP EXTENSION pg_context;

-- Upgrade path 1.0 -> 1.1 keeps data and allows multiple senses
CREATE EXTENSION pg_context VERSION '1.0';
SELECT context_register('Apple', 'customer:42', 'customer');
ALTER EXTENSION pg_context UPDATE TO '1.1';
SELECT context_register('Apple', 'produce:apple', 'product', NULL, 100, 'grocery', ARRAY['fruit']);
SELECT canonical_entity FROM context_resolve('Apple', 'fresh fruit');
SELECT term, canonical_entity, keywords FROM context_list();
DROP EXTENSION pg_context;
DROP EXTENSION pg_ontology;
