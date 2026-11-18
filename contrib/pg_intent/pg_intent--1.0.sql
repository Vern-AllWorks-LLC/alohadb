\echo Use "CREATE EXTENSION pg_intent" to load this file. \quit

-- Intent type registry: defines what intents exist and how to recognize them.
-- Addresses HydraDB's "understands the 'why' behind documents."
CREATE TABLE pg_intent_registry (
    intent_name   text    NOT NULL PRIMARY KEY,
    description   text    NOT NULL,
    keywords      text[]  NOT NULL DEFAULT '{}',
    parent_intent text    REFERENCES pg_intent_registry(intent_name)
                          ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED,
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX pg_intent_registry_parent_idx
    ON pg_intent_registry (parent_intent);

-- Document intent assignments: tags a document row with its intent
CREATE TABLE pg_intent_documents (
    id           bigserial   PRIMARY KEY,
    doc_id       text        NOT NULL,
    source_table text        NOT NULL,
    intent_name  text        NOT NULL
                             REFERENCES pg_intent_registry(intent_name)
                             ON DELETE CASCADE,
    confidence   float8      NOT NULL DEFAULT 1.0
                             CHECK (confidence BETWEEN 0.0 AND 1.0),
    metadata     jsonb,
    indexed_at   timestamptz NOT NULL DEFAULT now(),
    indexed_by   text        NOT NULL DEFAULT current_user,
    UNIQUE (doc_id, source_table, intent_name)
);

CREATE INDEX pg_intent_documents_doc_idx
    ON pg_intent_documents (doc_id, source_table);
CREATE INDEX pg_intent_documents_intent_idx
    ON pg_intent_documents (intent_name);
CREATE INDEX pg_intent_documents_confidence_idx
    ON pg_intent_documents (confidence DESC);

-- Register an intent type
CREATE FUNCTION intent_register(
    p_intent_name text,
    p_description text,
    p_keywords    text[] DEFAULT '{}',
    p_parent      text   DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql VOLATILE
AS $$
BEGIN
    INSERT INTO pg_intent_registry
        (intent_name, description, keywords, parent_intent)
    VALUES
        (p_intent_name, p_description, p_keywords, p_parent)
    ON CONFLICT (intent_name)
    DO UPDATE SET
        description   = EXCLUDED.description,
        keywords      = EXCLUDED.keywords,
        parent_intent = EXCLUDED.parent_intent;
END;
$$;

-- Tag a document with an intent (manual or programmatic)
CREATE FUNCTION intent_tag(
    p_doc_id       text,
    p_source_table text,
    p_intent_name  text,
    p_confidence   float8 DEFAULT 1.0,
    p_metadata     jsonb  DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql VOLATILE
AS $$
BEGIN
    INSERT INTO pg_intent_documents
        (doc_id, source_table, intent_name, confidence, metadata)
    VALUES
        (p_doc_id, p_source_table, p_intent_name, p_confidence, p_metadata)
    ON CONFLICT (doc_id, source_table, intent_name)
    DO UPDATE SET
        confidence = EXCLUDED.confidence,
        metadata   = EXCLUDED.metadata,
        indexed_at = now();
END;
$$;

-- Remove an intent tag from a document
CREATE FUNCTION intent_untag(
    p_doc_id       text,
    p_source_table text,
    p_intent_name  text
)
RETURNS boolean
LANGUAGE plpgsql VOLATILE STRICT
AS $$
DECLARE
    deleted_count int;
BEGIN
    DELETE FROM pg_intent_documents
    WHERE doc_id       = p_doc_id
      AND source_table = p_source_table
      AND intent_name  = p_intent_name;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count > 0;
END;
$$;

-- Get the intent(s) of a specific document
CREATE FUNCTION intent_of(
    p_doc_id       text,
    p_source_table text
)
RETURNS TABLE(
    intent_name   text,
    description   text,
    confidence    float8,
    parent_intent text,
    metadata      jsonb,
    indexed_at    timestamptz
)
LANGUAGE sql STABLE STRICT
AS $$
    SELECT d.intent_name,
           r.description,
           d.confidence,
           r.parent_intent,
           d.metadata,
           d.indexed_at
    FROM   pg_intent_documents d
    JOIN   pg_intent_registry  r USING (intent_name)
    WHERE  d.doc_id       = p_doc_id
      AND  d.source_table = p_source_table
    ORDER BY d.confidence DESC;
$$;

-- Find all documents tagged with a given intent
CREATE FUNCTION intent_find(
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
LANGUAGE sql STABLE STRICT
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
-- Uses simple term-frequency over the registered keyword lists.
-- No patented NLP algorithms — plain substring matching with open prior art.
CREATE FUNCTION intent_infer(
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
                   WHERE  lower(p_document_text) LIKE '%' || lower(kw) || '%'
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
    ORDER BY h.hit_count DESC, h.intent_name
    LIMIT  p_top_k;
$$;

-- Auto-tag a document by running intent inference and persisting the results
CREATE FUNCTION intent_auto_tag(
    p_doc_id         text,
    p_source_table   text,
    p_document_text  text,
    p_min_confidence float8 DEFAULT 0.3,
    p_metadata       jsonb  DEFAULT NULL
)
RETURNS int  -- number of intents tagged
LANGUAGE plpgsql VOLATILE STRICT
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

-- Get all ancestors of an intent through the hierarchy
CREATE FUNCTION intent_ancestors(p_intent_name text)
RETURNS TABLE(intent_name text, depth int)
LANGUAGE sql STABLE STRICT
AS $$
    WITH RECURSIVE hier(intent_name, depth) AS (
        SELECT parent_intent, 1
        FROM   pg_intent_registry
        WHERE  intent_name   = p_intent_name
          AND  parent_intent IS NOT NULL

        UNION ALL

        SELECT r.parent_intent, h.depth + 1
        FROM   pg_intent_registry r
        JOIN   hier h ON h.intent_name = r.intent_name
        WHERE  r.parent_intent IS NOT NULL
    )
    SELECT intent_name, depth
    FROM   hier
    ORDER BY depth;
$$;

-- Statistics
CREATE FUNCTION intent_stats(
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
        (SELECT count(DISTINCT doc_id || chr(0) || source_table)
         FROM   pg_intent_documents),
        (SELECT avg(confidence)
         FROM   pg_intent_documents);
$$;
