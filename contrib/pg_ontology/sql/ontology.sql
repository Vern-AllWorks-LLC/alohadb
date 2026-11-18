-- pg_ontology regression test

CREATE EXTENSION pg_ontology;
SELECT extversion FROM pg_extension WHERE extname = 'pg_ontology';

-- Types and hierarchy
SELECT ontology_type_assert('Apple Inc.', 'TechCompany', 'Consumer electronics maker');
SELECT ontology_type_assert('Apple Inc.', 'Customer');
SELECT ontology_subclass('TechCompany', 'Company');
SELECT ontology_subclass('Company', 'Organization');
SELECT ontology_subclass('Customer', 'Organization');

SELECT * FROM ontology_ancestors('TechCompany');
SELECT * FROM ontology_descendants('Organization');
SELECT ontology_is_a('TechCompany', 'Organization') AS t,
       ontology_is_a('Organization', 'TechCompany') AS f,
       ontology_is_a('Company', 'Company')          AS self;
SELECT * FROM ontology_entity_types('Apple Inc.');

-- Cycles are rejected
SELECT ontology_subclass('Organization', 'TechCompany');
SELECT ontology_subclass('Company', 'Company');

-- Triples
SELECT ontology_assert('Apple Inc.', 'makes',          'iPhone');
SELECT ontology_assert('Apple Inc.', 'makes',          'Mac');
SELECT ontology_assert('Apple Inc.', 'headquartersIn', 'Cupertino');
SELECT ontology_assert('Steve Jobs', 'founded',        'Apple Inc.');
SELECT ontology_assert('Cupertino',  'locatedIn',      'California');
SELECT ontology_assert('Mac',        'runs',           'macOS', 'default', 0.4);
SELECT ontology_assert('Apple Inc.', 'makes', 'iPhone');   -- idempotent
SELECT ontology_assert(NULL, 'makes', 'x');                -- error

SELECT subject, predicate, object FROM ontology_query('Apple Inc.', 'makes');
SELECT subject, predicate, object FROM ontology_query(NULL, NULL, 'Apple Inc.');

-- Traversal follows both directions, is cycle-safe, shortest hop wins,
-- low-confidence edges are skipped
SELECT entity, via_predicate, hops FROM ontology_related('Apple Inc.', 1) ORDER BY hops, entity;
SELECT entity, hops, path FROM ontology_related('Steve Jobs', 3) ORDER BY hops, entity;
SELECT entity, hops FROM ontology_related('Mac', 1, 0.0) ORDER BY entity;
SELECT ontology_assert('California', 'home of', 'Steve Jobs');  -- creates a cycle
SELECT entity, hops FROM ontology_related('Steve Jobs', 5) ORDER BY hops, entity;

-- Stats (entities counted once whether subject or object)
SELECT * FROM ontology_stats();

-- Retraction and history
SELECT ontology_retract('Apple Inc.', 'makes', 'Mac') AS retracted;
SELECT ontology_retract('Apple Inc.', 'makes', 'Mac') AS retracted_again;
SELECT ontology_assert('Mac', 'runs', 'macOS', 'default', 0.9);
SELECT operation, subject, predicate, object, old_confidence, new_confidence
FROM   ontology_history('Mac');

-- Extension data is included in dumps
SELECT c.relname
FROM   pg_extension e, unnest(e.extconfig) AS cfg(relid)
JOIN   pg_class c ON c.oid = cfg.relid
WHERE  e.extname = 'pg_ontology'
ORDER BY 1;

DROP EXTENSION pg_ontology;

-- Upgrade path 1.0 -> 1.1 yields a working 1.1
CREATE EXTENSION pg_ontology VERSION '1.0';
SELECT ontology_assert('a', 'p', 'b');
SELECT ontology_assert('b', 'p', 'c');
ALTER EXTENSION pg_ontology UPDATE TO '1.1';
SELECT entity, hops FROM ontology_related('c', 2) ORDER BY entity;
SELECT ontology_retract('a', 'p', 'b');
SELECT operation, subject, object FROM ontology_history('b');
DROP EXTENSION pg_ontology;
