/* contrib/pg_ontology/pg_ontology--1.1.sql */

\echo Use "CREATE EXTENSION pg_ontology" to load this file. \quit

-- Design note: everything here is built from long-established, openly
-- published techniques -- RDF-style subject/predicate/object triples (W3C,
-- 1999/2004), is-a concept hierarchies, and SQL:1999 recursive queries.  No
-- vendor-specific or recently-claimed graph algorithms are used.  This is an
-- engineering design choice, not a legal freedom-to-operate opinion.

-- Entity type registry
CREATE TABLE pg_ontology_types (
    entity        text        NOT NULL,
    type          text        NOT NULL,
    description   text,
    registered_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (entity, type)
);

-- Core triple store: subject - predicate - object
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
CREATE INDEX pg_ontology_triples_object_idx
    ON pg_ontology_triples (object);
CREATE INDEX pg_ontology_triples_predicate_idx
    ON pg_ontology_triples (predicate);

-- Concept hierarchy: is-a relationships (subclass -> superclass)
CREATE TABLE pg_ontology_hierarchy (
    concept text NOT NULL,
    parent  text NOT NULL,
    PRIMARY KEY (concept, parent),
    CHECK (concept <> parent)
);

CREATE INDEX pg_ontology_hierarchy_parent_idx
    ON pg_ontology_hierarchy (parent);

-- Change log: how the knowledge graph evolves over time
CREATE TABLE pg_ontology_history (
    id          bigserial   PRIMARY KEY,
    operation   text        NOT NULL CHECK (operation IN ('assert','update','retract')),
    subject     text        NOT NULL,
    predicate   text        NOT NULL,
    object      text        NOT NULL,
    graph       text        NOT NULL,
    old_confidence float8,
    new_confidence float8,
    changed_at  timestamptz NOT NULL DEFAULT now(),
    changed_by  text        NOT NULL DEFAULT current_user
);

CREATE INDEX pg_ontology_history_subject_idx
    ON pg_ontology_history (subject, changed_at);

-- User data in extension tables must survive pg_dump / pg_dumpall.
SELECT pg_catalog.pg_extension_config_dump('pg_ontology_types', '');
SELECT pg_catalog.pg_extension_config_dump('pg_ontology_triples', '');
SELECT pg_catalog.pg_extension_config_dump('pg_ontology_triples_id_seq', '');
SELECT pg_catalog.pg_extension_config_dump('pg_ontology_hierarchy', '');
SELECT pg_catalog.pg_extension_config_dump('pg_ontology_history', '');
SELECT pg_catalog.pg_extension_config_dump('pg_ontology_history_id_seq', '');

CREATE FUNCTION pg_ontology_log_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO pg_ontology_history
            (operation, subject, predicate, object, graph, new_confidence)
        VALUES ('assert', NEW.subject, NEW.predicate, NEW.object, NEW.graph,
                NEW.confidence);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        IF NEW.confidence IS DISTINCT FROM OLD.confidence THEN
            INSERT INTO pg_ontology_history
                (operation, subject, predicate, object, graph,
                 old_confidence, new_confidence)
            VALUES ('update', NEW.subject, NEW.predicate, NEW.object, NEW.graph,
                    OLD.confidence, NEW.confidence);
        END IF;
        RETURN NEW;
    ELSE
        INSERT INTO pg_ontology_history
            (operation, subject, predicate, object, graph, old_confidence)
        VALUES ('retract', OLD.subject, OLD.predicate, OLD.object, OLD.graph,
                OLD.confidence);
        RETURN OLD;
    END IF;
END;
$$;

CREATE TRIGGER pg_ontology_triples_history
    AFTER INSERT OR UPDATE OR DELETE ON pg_ontology_triples
    FOR EACH ROW EXECUTE FUNCTION pg_ontology_log_change();

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
    IF p_subject IS NULL OR p_predicate IS NULL OR p_object IS NULL THEN
        RAISE EXCEPTION 'ontology_assert: subject, predicate and object must not be NULL';
    END IF;

    INSERT INTO pg_ontology_triples
        (subject, predicate, object, graph, confidence)
    VALUES
        (p_subject, p_predicate, p_object,
         coalesce(p_graph, 'default'), coalesce(p_confidence, 1.0))
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
      AND graph     = coalesce(p_graph, 'default');
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
    ORDER BY t.confidence DESC, t.asserted_at DESC, t.id;
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
    IF p_entity IS NULL OR p_type IS NULL THEN
        RAISE EXCEPTION 'ontology_type_assert: entity and type must not be NULL';
    END IF;

    INSERT INTO pg_ontology_types (entity, type, description)
    VALUES (p_entity, p_type, p_description)
    ON CONFLICT (entity, type)
    DO UPDATE SET description = EXCLUDED.description;
END;
$$;

-- Declare a concept is-a relationship in the hierarchy.  Cycles are rejected.
CREATE FUNCTION ontology_subclass(
    p_concept text,
    p_parent  text
)
RETURNS void
LANGUAGE plpgsql VOLATILE STRICT
AS $$
BEGIN
    IF p_concept = p_parent
       OR EXISTS (SELECT 1 FROM ontology_ancestors(p_parent)
                  WHERE ancestor = p_concept) THEN
        RAISE EXCEPTION 'ontology_subclass: "%" is-a "%" would create a cycle',
            p_concept, p_parent;
    END IF;

    INSERT INTO pg_ontology_hierarchy (concept, parent)
    VALUES (p_concept, p_parent)
    ON CONFLICT (concept, parent) DO NOTHING;

    PERFORM ontology_assert(p_concept, 'is-a', p_parent);
END;
$$;

-- Find all entities related to a given entity within N hops, following
-- edges in both directions.  Cycle-safe; each entity is reported once at its
-- shortest distance.  Edges below p_min_confidence are ignored.
CREATE FUNCTION ontology_related(
    p_entity         text,
    p_depth          int    DEFAULT 2,
    p_min_confidence float8 DEFAULT 0.5
)
RETURNS TABLE(entity text, via_predicate text, hops int, path text[])
LANGUAGE sql STABLE STRICT
AS $$
    WITH RECURSIVE edges(src, dst, predicate) AS (
        SELECT t.subject, t.object, t.predicate
        FROM   pg_ontology_triples t
        WHERE  t.confidence >= p_min_confidence
        UNION ALL
        SELECT t.object, t.subject, t.predicate
        FROM   pg_ontology_triples t
        WHERE  t.confidence >= p_min_confidence
    ),
    walk(entity, via_predicate, hops, path) AS (
        SELECT e.dst, e.predicate, 1, ARRAY[p_entity, e.dst]
        FROM   edges e
        WHERE  e.src = p_entity
          AND  e.dst <> p_entity
        UNION ALL
        SELECT e.dst, e.predicate, w.hops + 1, w.path || e.dst
        FROM   walk w
        JOIN   edges e ON e.src = w.entity
        WHERE  w.hops < p_depth
          AND  e.dst <> ALL (w.path)
    )
    SELECT DISTINCT ON (w.entity) w.entity, w.via_predicate, w.hops, w.path
    FROM   walk w
    ORDER BY w.entity, w.hops, w.via_predicate;
$$;

-- All ancestors of a concept via the hierarchy (cycle-safe)
CREATE FUNCTION ontology_ancestors(p_concept text)
RETURNS TABLE(ancestor text, depth int)
LANGUAGE sql STABLE STRICT
AS $$
    WITH RECURSIVE hier(ancestor, depth, path) AS (
        SELECT h.parent, 1, ARRAY[p_concept, h.parent]
        FROM   pg_ontology_hierarchy h
        WHERE  h.concept = p_concept
        UNION ALL
        SELECT h.parent, hier.depth + 1, hier.path || h.parent
        FROM   pg_ontology_hierarchy h
        JOIN   hier ON hier.ancestor = h.concept
        WHERE  h.parent <> ALL (hier.path)
    )
    SELECT ancestor, min(depth)::int
    FROM   hier
    GROUP BY ancestor
    ORDER BY 2, 1;
$$;

-- All descendants of a concept via the hierarchy (cycle-safe)
CREATE FUNCTION ontology_descendants(p_concept text)
RETURNS TABLE(descendant text, depth int)
LANGUAGE sql STABLE STRICT
AS $$
    WITH RECURSIVE hier(descendant, depth, path) AS (
        SELECT h.concept, 1, ARRAY[p_concept, h.concept]
        FROM   pg_ontology_hierarchy h
        WHERE  h.parent = p_concept
        UNION ALL
        SELECT h.concept, hier.depth + 1, hier.path || h.concept
        FROM   pg_ontology_hierarchy h
        JOIN   hier ON hier.descendant = h.parent
        WHERE  h.concept <> ALL (hier.path)
    )
    SELECT descendant, min(depth)::int
    FROM   hier
    GROUP BY descendant
    ORDER BY 2, 1;
$$;

-- Is p_concept the same as, or a (transitive) subclass of, p_ancestor?
CREATE FUNCTION ontology_is_a(p_concept text, p_ancestor text)
RETURNS boolean
LANGUAGE sql STABLE STRICT
AS $$
    SELECT p_concept = p_ancestor
        OR EXISTS (SELECT 1 FROM ontology_ancestors(p_concept)
                   WHERE ancestor = p_ancestor);
$$;

-- Change history for an entity (as subject or object)
CREATE FUNCTION ontology_history(
    p_entity text,
    p_since  timestamptz DEFAULT '-infinity'
)
RETURNS TABLE(operation text, subject text, predicate text, object text,
              graph text, old_confidence float8, new_confidence float8,
              changed_at timestamptz, changed_by text)
LANGUAGE sql STABLE STRICT
AS $$
    SELECT h.operation, h.subject, h.predicate, h.object, h.graph,
           h.old_confidence, h.new_confidence, h.changed_at, h.changed_by
    FROM   pg_ontology_history h
    WHERE  (h.subject = p_entity OR h.object = p_entity)
      AND  h.changed_at >= p_since
    ORDER BY h.id;
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
        (SELECT count(*) FROM pg_ontology_triples),
        (SELECT count(*) FROM (SELECT subject FROM pg_ontology_triples
                               UNION
                               SELECT object  FROM pg_ontology_triples) e),
        (SELECT count(DISTINCT predicate) FROM pg_ontology_triples),
        (SELECT count(*) FROM pg_ontology_types),
        (SELECT count(DISTINCT graph) FROM pg_ontology_triples);
$$;

-- Get all types for an entity, including types inherited via the hierarchy
CREATE FUNCTION ontology_entity_types(p_entity text)
RETURNS TABLE(type text, description text, inherited boolean)
LANGUAGE sql STABLE STRICT
AS $$
    SELECT t.type, t.description, false
    FROM   pg_ontology_types t
    WHERE  t.entity = p_entity
    UNION
    SELECT a.ancestor, NULL::text, true
    FROM   pg_ontology_types t,
           LATERAL ontology_ancestors(t.type) a
    WHERE  t.entity = p_entity
      AND  NOT EXISTS (SELECT 1 FROM pg_ontology_types t2
                       WHERE t2.entity = p_entity AND t2.type = a.ancestor)
    ORDER BY 3, 1;
$$;
