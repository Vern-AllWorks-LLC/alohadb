\echo Use "CREATE EXTENSION pg_ontology" to load this file. \quit

-- Entity type registry
CREATE TABLE pg_ontology_types (
    entity        text        NOT NULL,
    type          text        NOT NULL,
    description   text,
    registered_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (entity, type)
);

-- Core triple store: subject – predicate – object
-- Foundation: W3C RDF standard (1997), extensive prior art, not patentable
CREATE TABLE pg_ontology_triples (
    id          bigserial   PRIMARY KEY,
    subject     text        NOT NULL,
    predicate   text        NOT NULL,
    object      text        NOT NULL,
    graph       text        NOT NULL DEFAULT 'default',
    confidence  float8      NOT NULL DEFAULT 1.0
                            CHECK (confidence BETWEEN 0.0 AND 1.0),
    asserted_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX pg_ontology_triples_spo_idx
    ON pg_ontology_triples (subject, predicate, object, graph);
CREATE INDEX pg_ontology_triples_subject_idx
    ON pg_ontology_triples (subject);
CREATE INDEX pg_ontology_triples_object_idx
    ON pg_ontology_triples (object);
CREATE INDEX pg_ontology_triples_predicate_idx
    ON pg_ontology_triples (predicate);

-- Concept hierarchy: is-a relationships (subclass / superclass)
CREATE TABLE pg_ontology_hierarchy (
    concept text NOT NULL,
    parent  text NOT NULL,
    PRIMARY KEY (concept, parent)
);

-- Assert a triple (idempotent upsert)
CREATE FUNCTION ontology_assert(
    p_subject    text,
    p_predicate  text,
    p_object     text,
    p_graph      text   DEFAULT 'default',
    p_confidence float8 DEFAULT 1.0
)
RETURNS void
LANGUAGE plpgsql VOLATILE
AS $$
BEGIN
    INSERT INTO pg_ontology_triples
        (subject, predicate, object, graph, confidence)
    VALUES
        (p_subject, p_predicate, p_object, p_graph, p_confidence)
    ON CONFLICT (subject, predicate, object, graph)
    DO UPDATE SET
        confidence  = EXCLUDED.confidence,
        asserted_at = now();
END;
$$;

-- Retract a triple
CREATE FUNCTION ontology_retract(
    p_subject   text,
    p_predicate text,
    p_object    text,
    p_graph     text DEFAULT 'default'
)
RETURNS boolean
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    deleted_count int;
BEGIN
    DELETE FROM pg_ontology_triples
    WHERE subject   = p_subject
      AND predicate = p_predicate
      AND object    = p_object
      AND graph     = p_graph;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count > 0;
END;
$$;

-- Pattern-match query: NULL = wildcard (matches any value)
CREATE FUNCTION ontology_query(
    p_subject   text DEFAULT NULL,
    p_predicate text DEFAULT NULL,
    p_object    text DEFAULT NULL,
    p_graph     text DEFAULT NULL
)
RETURNS TABLE(subject text, predicate text, object text, graph text, confidence float8)
LANGUAGE sql STABLE
AS $$
    SELECT t.subject, t.predicate, t.object, t.graph, t.confidence
    FROM   pg_ontology_triples t
    WHERE  (p_subject   IS NULL OR t.subject   = p_subject)
      AND  (p_predicate IS NULL OR t.predicate = p_predicate)
      AND  (p_object    IS NULL OR t.object    = p_object)
      AND  (p_graph     IS NULL OR t.graph     = p_graph)
    ORDER BY t.confidence DESC, t.asserted_at DESC;
$$;

-- Register an entity type
CREATE FUNCTION ontology_type_assert(
    p_entity      text,
    p_type        text,
    p_description text DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql VOLATILE
AS $$
BEGIN
    INSERT INTO pg_ontology_types (entity, type, description)
    VALUES (p_entity, p_type, p_description)
    ON CONFLICT (entity, type)
    DO UPDATE SET description = EXCLUDED.description;
END;
$$;

-- Get all types for an entity
CREATE FUNCTION ontology_entity_types(p_entity text)
RETURNS TABLE(type text, description text)
LANGUAGE sql STABLE STRICT
AS $$
    SELECT type, description
    FROM   pg_ontology_types
    WHERE  entity = p_entity
    ORDER BY type;
$$;

-- Declare concept is-a relationship in the hierarchy
CREATE FUNCTION ontology_subclass(
    p_concept text,
    p_parent  text
)
RETURNS void
LANGUAGE plpgsql VOLATILE
AS $$
BEGIN
    INSERT INTO pg_ontology_hierarchy (concept, parent)
    VALUES (p_concept, p_parent)
    ON CONFLICT (concept, parent) DO NOTHING;

    PERFORM ontology_assert(p_concept, 'is-a', p_parent);
END;
$$;

-- Find all entities related to a given entity within N hops.
-- Uses recursive SQL CTEs (ISO SQL:1999 standard) — no proprietary graph patents.
CREATE FUNCTION ontology_related(
    p_entity text,
    p_depth  int DEFAULT 2
)
RETURNS TABLE(entity text, via_predicate text, hops int)
LANGUAGE sql STABLE STRICT
AS $$
    WITH RECURSIVE graph_walk(entity, via_predicate, hops) AS (
        -- Direct outgoing neighbors
        SELECT t.object,
               t.predicate,
               1
        FROM   pg_ontology_triples t
        WHERE  t.subject   = p_entity
          AND  t.confidence >= 0.5

        UNION ALL

        -- Direct incoming neighbors
        SELECT t.subject,
               t.predicate,
               1
        FROM   pg_ontology_triples t
        WHERE  t.object    = p_entity
          AND  t.confidence >= 0.5

        UNION ALL

        -- Recurse outward
        SELECT t.object,
               t.predicate,
               gw.hops + 1
        FROM   pg_ontology_triples t
        JOIN   graph_walk gw ON gw.entity = t.subject
        WHERE  gw.hops < p_depth
          AND  t.confidence >= 0.5
    )
    SELECT DISTINCT entity,
                    via_predicate,
                    MIN(hops) AS hops
    FROM   graph_walk
    WHERE  entity <> p_entity
    GROUP BY entity, via_predicate
    ORDER BY hops, entity;
$$;

-- Return all ancestors of a concept via the hierarchy
CREATE FUNCTION ontology_ancestors(p_concept text)
RETURNS TABLE(ancestor text, depth int)
LANGUAGE sql STABLE STRICT
AS $$
    WITH RECURSIVE hier(ancestor, depth) AS (
        SELECT parent, 1
        FROM   pg_ontology_hierarchy
        WHERE  concept = p_concept

        UNION ALL

        SELECT h.parent, hier.depth + 1
        FROM   pg_ontology_hierarchy h
        JOIN   hier ON hier.ancestor = h.concept
    )
    SELECT ancestor, MIN(depth)
    FROM   hier
    GROUP BY ancestor
    ORDER BY MIN(depth);
$$;

-- Summary statistics
CREATE FUNCTION ontology_stats(
    OUT triple_count    bigint,
    OUT entity_count    bigint,
    OUT predicate_count bigint,
    OUT type_count      bigint,
    OUT graph_count     bigint
)
RETURNS record
LANGUAGE sql STABLE
AS $$
    SELECT
        (SELECT count(*)                          FROM pg_ontology_triples),
        (SELECT count(DISTINCT subject)
               + count(DISTINCT object)           FROM pg_ontology_triples),
        (SELECT count(DISTINCT predicate)         FROM pg_ontology_triples),
        (SELECT count(*)                          FROM pg_ontology_types),
        (SELECT count(DISTINCT graph)             FROM pg_ontology_triples);
$$;
