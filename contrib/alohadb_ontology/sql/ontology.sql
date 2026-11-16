-- alohadb_ontology regression test
-- Tests: triple store, type hierarchy, graph traversal

CREATE EXTENSION alohadb_ontology;

-- -----------------------------------------------------------------------
-- Type hierarchy
-- -----------------------------------------------------------------------

SELECT ontology_type_assert('Organization', NULL);
SELECT ontology_type_assert('Company', 'Organization');
SELECT ontology_type_assert('TechCompany', 'Company');
SELECT ontology_type_assert('Product', NULL);
SELECT ontology_type_assert('SoftwareProduct', 'Product');
SELECT ontology_type_assert('Person', NULL);

SELECT ontology_subclass('TechCompany', 'Organization') AS is_subclass_true;
SELECT ontology_subclass('Product', 'TechCompany')      AS is_subclass_false;

-- -----------------------------------------------------------------------
-- Assert triples (S-P-O knowledge graph)
-- -----------------------------------------------------------------------

-- Apple Inc. facts
SELECT ontology_assert('Apple Inc.', 'rdf:type',     'TechCompany',  1.0);
SELECT ontology_assert('Apple Inc.', 'founded',      '1976-04-01',   1.0);
SELECT ontology_assert('Apple Inc.', 'foundedBy',    'Steve Jobs',   1.0);
SELECT ontology_assert('Apple Inc.', 'foundedBy',    'Steve Wozniak',1.0);
SELECT ontology_assert('Apple Inc.', 'headquartersIn','Cupertino',   1.0);
SELECT ontology_assert('Apple Inc.', 'makes',        'iPhone',       1.0);
SELECT ontology_assert('Apple Inc.', 'makes',        'Mac',          1.0);
SELECT ontology_assert('Apple Inc.', 'makes',        'iPad',         1.0);

-- Products
SELECT ontology_assert('iPhone',     'rdf:type',     'SoftwareProduct', 0.9);
SELECT ontology_assert('iPhone',     'madeBy',       'Apple Inc.',      1.0);
SELECT ontology_assert('Mac',        'rdf:type',     'SoftwareProduct', 0.9);
SELECT ontology_assert('Mac',        'madeBy',       'Apple Inc.',      1.0);

-- People
SELECT ontology_assert('Steve Jobs', 'rdf:type',     'Person',      1.0);
SELECT ontology_assert('Steve Jobs', 'workedAt',     'Apple Inc.',  1.0);
SELECT ontology_assert('Tim Cook',   'rdf:type',     'Person',      1.0);
SELECT ontology_assert('Tim Cook',   'workedAt',     'Apple Inc.',  1.0);

-- -----------------------------------------------------------------------
-- Query: wildcard matching
-- -----------------------------------------------------------------------

-- All things Apple makes
SELECT subject, predicate, object
FROM   ontology_query('Apple Inc.', 'makes', NULL)
ORDER BY object;

-- All founders
SELECT subject, predicate, object
FROM   ontology_query('Apple Inc.', 'foundedBy', NULL)
ORDER BY object;

-- Who works at Apple?
SELECT subject, predicate, object
FROM   ontology_query(NULL, 'workedAt', 'Apple Inc.')
ORDER BY subject;

-- Exact triple lookup
SELECT subject, predicate, object, confidence
FROM   ontology_query('Apple Inc.', 'rdf:type', 'TechCompany');

-- -----------------------------------------------------------------------
-- Graph traversal
-- -----------------------------------------------------------------------

SELECT related_entity, predicate, depth
FROM   ontology_related('Apple Inc.', 2)
ORDER BY depth, related_entity;

-- Ancestors of TechCompany
SELECT ancestor, depth
FROM   ontology_ancestors('TechCompany')
ORDER BY depth;

-- -----------------------------------------------------------------------
-- Statistics
-- -----------------------------------------------------------------------

SELECT total_triples, total_types, total_entities
FROM   ontology_stats();

-- -----------------------------------------------------------------------
-- Retract
-- -----------------------------------------------------------------------

SELECT ontology_retract('Apple Inc.', 'foundedBy', 'Steve Jobs');

SELECT count(*) AS founders_after_retract
FROM   ontology_query('Apple Inc.', 'foundedBy', NULL);

-- -----------------------------------------------------------------------
-- Cleanup
-- -----------------------------------------------------------------------

TRUNCATE alohadb_ontology_triples, alohadb_ontology_types, alohadb_ontology_hierarchy;
DROP EXTENSION alohadb_ontology;
