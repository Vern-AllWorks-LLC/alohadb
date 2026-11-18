/* contrib/pg_intent/pg_intent--1.0--1.1.sql */

\echo Use "ALTER EXTENSION pg_intent UPDATE TO '1.1'" to load this file. \quit

SELECT pg_catalog.pg_extension_config_dump('pg_intent_registry', '');

SELECT pg_catalog.pg_extension_config_dump('pg_intent_documents', '');

SELECT pg_catalog.pg_extension_config_dump('pg_intent_documents_id_seq', '');


-- Does p_text contain p_phrase as a whole word/phrase (case-insensitive)?
CREATE FUNCTION intent_mentions(p_text text, p_phrase text)
RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
AS $$
    SELECT p_phrase <> ''
       AND lower(p_text) ~ ('(^|[^[:alnum:]_])'
                            || regexp_replace(lower(p_phrase),
                                              '([.^$*+?()\[\]{}|\\-])', '\\\1', 'g')
                            || '($|[^[:alnum:]_])');
$$;

-- Get all ancestors of an intent through the hierarchy
CREATE OR REPLACE FUNCTION intent_ancestors(p_intent_name text)
RETURNS TABLE(intent_name text, depth int)
LANGUAGE sql STABLE STRICT
AS $$
    WITH RECURSIVE hier(intent_name, depth, path) AS (
        SELECT parent_intent, 1, ARRAY[intent_name, parent_intent]
        FROM   pg_intent_registry
        WHERE  intent_name   = p_intent_name
          AND  parent_intent IS NOT NULL

        UNION ALL

        SELECT r.parent_intent, h.depth + 1, h.path || r.parent_intent
        FROM   pg_intent_registry r
        JOIN   hier h ON h.intent_name = r.intent_name
        WHERE  r.parent_intent IS NOT NULL
          AND  r.parent_intent <> ALL (h.path)
    )
    SELECT intent_name, depth
    FROM   hier
    ORDER BY depth;
$$;

-- Reject parent links that would make the intent hierarchy cyclic
CREATE FUNCTION pg_intent_check_cycle()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.parent_intent IS NOT NULL
       AND (NEW.parent_intent = NEW.intent_name
            OR EXISTS (SELECT 1 FROM intent_ancestors(NEW.parent_intent)
                       WHERE intent_name = NEW.intent_name)) THEN
        RAISE EXCEPTION 'intent "%" cannot have parent "%": cycle in intent hierarchy',
            NEW.intent_name, NEW.parent_intent;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER pg_intent_registry_no_cycle
    BEFORE INSERT OR UPDATE OF parent_intent ON pg_intent_registry
    FOR EACH ROW EXECUTE FUNCTION pg_intent_check_cycle();

-- Find all documents tagged with a given intent
CREATE OR REPLACE FUNCTION intent_find(
    p_intent_name    text,
    p_source_table   text   DEFAULT NULL,
    p_min_confidence float8 DEFAULT 0.5
)
RETURNS TABLE(
    doc_id       text,
    source_table text,
    confidence   float8,
    metadata     jsonb,
    indexed_at   timestamptz
)
LANGUAGE sql STABLE
AS $$
    SELECT d.doc_id, d.source_table, d.confidence, d.metadata, d.indexed_at
    FROM   pg_intent_documents d
    WHERE  d.intent_name  = p_intent_name
      AND  (p_source_table IS NULL OR d.source_table = p_source_table)
      AND  d.confidence   >= p_min_confidence
    ORDER BY d.confidence DESC, d.indexed_at DESC;
$$;

-- Keyword-based intent inference.
-- Given a document text, returns matching intents ranked by keyword hit ratio.
-- Plain whole-word keyword matching over the registered keyword lists
-- (a long-published technique; no learned or proprietary NLP models).
CREATE OR REPLACE FUNCTION intent_infer(
    p_document_text  text,
    p_min_hits       int  DEFAULT 1,
    p_top_k          int  DEFAULT 5
)
RETURNS TABLE(
    intent_name text,
    description text,
    hit_count   int,
    confidence  float8
)
LANGUAGE sql STABLE STRICT
AS $$
    WITH hits AS (
        SELECT r.intent_name,
               r.description,
               (
                   SELECT count(*)::int
                   FROM   unnest(r.keywords) kw
                   WHERE  intent_mentions(p_document_text, kw)
               )                          AS hit_count,
               array_length(r.keywords, 1) AS total_keywords
        FROM   pg_intent_registry r
        WHERE  array_length(r.keywords, 1) > 0
    )
    SELECT h.intent_name,
           h.description,
           h.hit_count,
           CASE
               WHEN h.total_keywords = 0 THEN 0.0
               ELSE h.hit_count::float8 / h.total_keywords::float8
           END AS confidence
    FROM   hits h
    WHERE  h.hit_count >= p_min_hits
    ORDER BY h.hit_count DESC, 4 DESC, h.intent_name
    LIMIT  p_top_k;
$$;

-- Auto-tag a document by running intent inference and persisting the results
CREATE OR REPLACE FUNCTION intent_auto_tag(
    p_doc_id         text,
    p_source_table   text,
    p_document_text  text,
    p_min_confidence float8 DEFAULT 0.3,
    p_metadata       jsonb  DEFAULT NULL
)
RETURNS int  -- number of intents tagged
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    tagged_count int := 0;
    rec          record;
BEGIN
    FOR rec IN
        SELECT intent_name, confidence
        FROM   intent_infer(p_document_text)
        WHERE  confidence >= p_min_confidence
    LOOP
        PERFORM intent_tag(
            p_doc_id,
            p_source_table,
            rec.intent_name,
            rec.confidence,
            p_metadata
        );
        tagged_count := tagged_count + 1;
    END LOOP;
    RETURN tagged_count;
END;
$$;

-- Statistics
CREATE OR REPLACE FUNCTION intent_stats(
    OUT registered_intents bigint,
    OUT tagged_documents   bigint,
    OUT avg_confidence     float8
)
RETURNS record
LANGUAGE sql STABLE
AS $$
    SELECT
        (SELECT count(*)
         FROM   pg_intent_registry),
        (SELECT count(*)
         FROM   (SELECT DISTINCT doc_id, source_table
                 FROM pg_intent_documents) d),
        (SELECT avg(confidence)
         FROM   pg_intent_documents);
$$;
