-- alohadb_context regression test
-- Tests: entity disambiguation, alias resolution, context-aware lookup

CREATE EXTENSION alohadb_context;

-- -----------------------------------------------------------------------
-- Register context profiles
-- -----------------------------------------------------------------------

-- "Apple" is ambiguous: company in tech context, fruit in food context
SELECT context_register('apple',  'Apple Inc.',    'technology', 0.95);
SELECT context_register('apple',  'Apple (fruit)', 'food',       0.90);
SELECT context_register('apple',  'Apple Inc.',    NULL,         0.70);

-- "Python" contexts
SELECT context_register('python', 'Python (programming language)', 'technology', 0.99);
SELECT context_register('python', 'Python (snake)',                'biology',    0.95);

-- "Mercury"
SELECT context_register('mercury', 'Mercury (planet)',  'astronomy',  0.97);
SELECT context_register('mercury', 'Mercury (element)', 'chemistry',  0.96);
SELECT context_register('mercury', 'Mercury (car brand)','automotive', 0.85);

-- -----------------------------------------------------------------------
-- Register aliases
-- -----------------------------------------------------------------------

SELECT context_alias('Apple Inc.',    'AAPL');
SELECT context_alias('Apple Inc.',    'Apple Computer');
SELECT context_alias('Apple (fruit)', 'apple fruit');

-- -----------------------------------------------------------------------
-- Resolve: exact + context
-- -----------------------------------------------------------------------

SELECT context_resolve('apple',  'technology') AS canonical;
SELECT context_resolve('apple',  'food')       AS canonical;
SELECT context_resolve('apple',  NULL)         AS canonical_default;
SELECT context_resolve('python', 'technology') AS canonical;

-- Alias resolution
SELECT context_resolve('AAPL', 'technology') AS canonical_from_alias;

-- Unknown term returns NULL
SELECT context_resolve('unknownterm', NULL) IS NULL AS returns_null;

-- -----------------------------------------------------------------------
-- Batch disambiguation
-- -----------------------------------------------------------------------

SELECT term, canonical_entity
FROM   context_disambiguate(ARRAY['apple', 'python', 'mercury', 'unknown'])
ORDER BY term;

-- -----------------------------------------------------------------------
-- Audit history
-- -----------------------------------------------------------------------

SELECT term, canonical_entity, context_domain, confidence
FROM   context_list()
ORDER BY term, context_domain NULLS LAST;

SELECT count(*) AS history_rows
FROM   context_history('apple');

-- -----------------------------------------------------------------------
-- Retract
-- -----------------------------------------------------------------------

SELECT context_retract('apple', 'food');

SELECT context_resolve('apple', 'food') AS after_retract;

-- -----------------------------------------------------------------------
-- Cleanup
-- -----------------------------------------------------------------------

TRUNCATE alohadb_context_profiles, alohadb_context_aliases, alohadb_context_evolution;
DROP EXTENSION alohadb_context;
