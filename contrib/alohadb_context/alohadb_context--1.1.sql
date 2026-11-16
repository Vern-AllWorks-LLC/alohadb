/* contrib/alohadb_context/alohadb_context--1.1.sql */

\echo Use "CREATE EXTENSION alohadb_context" to load this file. \quit

-- Business-context entity disambiguation.
--
-- A term (e.g. 'Apple') may have several registered senses
-- ('customer:42' the client company, 'produce:apple' the fruit).  Each sense
-- carries context cues: its domain, entity type, keywords, and -- via
-- alohadb_ontology -- its graph neighbours.  Resolving a term against a piece
-- of surrounding text counts which cues appear in that text and picks the
-- best-supported sense; registered priority only breaks ties.
--
-- Design note: the methods are deliberately plain and long-published --
-- dictionary lookup, alias tables, cue-word counting (Lesk-style overlap,
-- 1986) and multiplicative score adjustment.  No learned or proprietary
-- ranking models are used.  This is an engineering choice, not a legal
-- freedom-to-operate opinion.

CREATE TABLE alohadb_context_profiles (
    id               bigserial   PRIMARY KEY,
    term             text        NOT NULL,
    canonical_entity text        NOT NULL,
    entity_type      text        NOT NULL,
    domain           text,
    keywords         text[]      NOT NULL DEFAULT '{}',
    context_hint     text,
    priority         int         NOT NULL DEFAULT 100,
    registered_by    text        NOT NULL DEFAULT current_user,
    registered_at    timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    UNIQUE (term, canonical_entity)
);

CREATE INDEX alohadb_context_profiles_lterm_idx
    ON alohadb_context_profiles (lower(term));
CREATE INDEX alohadb_context_profiles_entity_idx
    ON alohadb_context_profiles (canonical_entity);
CREATE INDEX alohadb_context_profiles_type_idx
    ON alohadb_context_profiles (entity_type);

-- Alternate names that unambiguously identify one canonical entity
CREATE TABLE alohadb_context_aliases (
    alias            text        NOT NULL PRIMARY KEY,
    canonical_entity text        NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX alohadb_context_aliases_canonical_idx
    ON alohadb_context_aliases (canonical_entity);
CREATE INDEX alohadb_context_aliases_lalias_idx
    ON alohadb_context_aliases (lower(alias));

-- Evolution log: how entity contexts change over time
CREATE TABLE alohadb_context_evolution (
    id               bigserial   PRIMARY KEY,
    canonical_entity text        NOT NULL,
    event_type       text        NOT NULL
                     CHECK (event_type IN ('registered','updated','aliased','retracted')),
    old_value        jsonb,
    new_value        jsonb,
    recorded_at      timestamptz NOT NULL DEFAULT now(),
    recorded_by      text        NOT NULL DEFAULT current_user
);

CREATE INDEX alohadb_context_evolution_entity_idx
    ON alohadb_context_evolution (canonical_entity);
CREATE INDEX alohadb_context_evolution_time_idx
    ON alohadb_context_evolution (recorded_at);

SELECT pg_catalog.pg_extension_config_dump('alohadb_context_profiles', '');
SELECT pg_catalog.pg_extension_config_dump('alohadb_context_profiles_id_seq', '');
SELECT pg_catalog.pg_extension_config_dump('alohadb_context_aliases', '');
SELECT pg_catalog.pg_extension_config_dump('alohadb_context_evolution', '');
SELECT pg_catalog.pg_extension_config_dump('alohadb_context_evolution_id_seq', '');

-- Does p_text mention p_phrase as a whole word/phrase (case-insensitive)?
CREATE FUNCTION context_mentions(p_text text, p_phrase text)
RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
AS $$
    SELECT p_phrase <> ''
       AND lower(p_text) ~ ('(^|[^[:alnum:]_])'
                            || regexp_replace(lower(p_phrase),
                                              '([.^$*+?()\[\]{}|\\-])', '\\\1', 'g')
                            || '($|[^[:alnum:]_])');
$$;

-- Register (or update) one sense of a term
CREATE FUNCTION context_register(
    p_term         text,
    p_canonical    text,
    p_entity_type  text,
    p_context_hint text   DEFAULT NULL,
    p_priority     int    DEFAULT 100,
    p_domain       text   DEFAULT NULL,
    p_keywords     text[] DEFAULT '{}'
)
RETURNS void
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    old_rec alohadb_context_profiles%ROWTYPE;
BEGIN
    IF p_term IS NULL OR p_canonical IS NULL OR p_entity_type IS NULL THEN
        RAISE EXCEPTION 'context_register: term, canonical entity and entity type must not be NULL';
    END IF;

    SELECT * INTO old_rec
    FROM   alohadb_context_profiles
    WHERE  term = p_term AND canonical_entity = p_canonical;

    INSERT INTO alohadb_context_profiles
        (term, canonical_entity, entity_type, context_hint, priority,
         domain, keywords)
    VALUES
        (p_term, p_canonical, p_entity_type, p_context_hint,
         coalesce(p_priority, 100), p_domain, coalesce(p_keywords, '{}'))
    ON CONFLICT (term, canonical_entity)
    DO UPDATE SET
        entity_type  = EXCLUDED.entity_type,
        context_hint = EXCLUDED.context_hint,
        priority     = EXCLUDED.priority,
        domain       = EXCLUDED.domain,
        keywords     = EXCLUDED.keywords,
        updated_at   = now();

    INSERT INTO alohadb_context_evolution
        (canonical_entity, event_type, old_value, new_value)
    VALUES (
        p_canonical,
        CASE WHEN old_rec.id IS NULL THEN 'registered' ELSE 'updated' END,
        CASE WHEN old_rec.id IS NOT NULL THEN
            jsonb_build_object(
                'term',         old_rec.term,
                'entity_type',  old_rec.entity_type,
                'domain',       old_rec.domain,
                'keywords',     to_jsonb(old_rec.keywords),
                'context_hint', old_rec.context_hint,
                'priority',     old_rec.priority)
        END,
        jsonb_build_object(
            'term',         p_term,
            'entity_type',  p_entity_type,
            'domain',       p_domain,
            'keywords',     to_jsonb(coalesce(p_keywords, '{}')),
            'context_hint', p_context_hint,
            'priority',     coalesce(p_priority, 100))
    );
END;
$$;

-- Register an alias for a canonical entity
CREATE FUNCTION context_alias(
    p_alias     text,
    p_canonical text
)
RETURNS void
LANGUAGE plpgsql VOLATILE STRICT
AS $$
BEGIN
    INSERT INTO alohadb_context_aliases (alias, canonical_entity)
    VALUES (p_alias, p_canonical)
    ON CONFLICT (alias)
    DO UPDATE SET canonical_entity = EXCLUDED.canonical_entity;

    INSERT INTO alohadb_context_evolution
        (canonical_entity, event_type, new_value)
    VALUES (p_canonical, 'aliased',
            jsonb_build_object('alias', p_alias, 'canonical_entity', p_canonical));
END;
$$;

-- Count how many of an entity's context cues appear in p_context.
-- Cues: domain, entity type, keywords, ontology types and 1-hop neighbours.
CREATE FUNCTION context_cue_hits(
    p_canonical   text,
    p_entity_type text,
    p_domain      text,
    p_keywords    text[],
    p_context     text
)
RETURNS int
LANGUAGE sql STABLE
AS $$
    SELECT count(DISTINCT lower(cue))::int
    FROM (
        SELECT p_domain AS cue
        UNION ALL SELECT p_entity_type
        UNION ALL SELECT unnest(p_keywords)
        UNION ALL SELECT r.entity
                  FROM   ontology_related(p_canonical, 1) r
        UNION ALL SELECT t.type
                  FROM   ontology_entity_types(p_canonical) t
    ) cues
    WHERE p_context IS NOT NULL
      AND cue IS NOT NULL
      AND lower(cue) <> lower(p_canonical)
      AND context_mentions(p_context, cue);
$$;

-- Resolve a term to its canonical entity, optionally using surrounding text
-- (p_context) to choose between several registered senses.
--
-- Candidates come from exact term match, case-insensitive term match, and
-- alias match.  Score = number of context cues found in p_context
-- + priority / 1000 (tie-break).  An alias match identifies its entity
-- outright and scores above any profile match.  Returns the best p_limit
-- candidates, best first (empty set if the term is unknown).
CREATE FUNCTION context_resolve(
    p_term    text,
    p_context text DEFAULT NULL,
    p_limit   int  DEFAULT 1
)
RETURNS TABLE(
    canonical_entity text,
    entity_type      text,
    context_hint     text,
    resolution_path  text,   -- 'direct' | 'direct_ci' | 'alias'
    score            float8
)
LANGUAGE sql STABLE
AS $$
    WITH cand AS (
        SELECT cp.canonical_entity, cp.entity_type, cp.context_hint,
               CASE WHEN cp.term = p_term THEN 'direct' ELSE 'direct_ci' END AS path,
               CASE WHEN cp.term = p_term THEN 1 ELSE 2 END AS path_rank,
               context_cue_hits(cp.canonical_entity, cp.entity_type, cp.domain,
                                cp.keywords, p_context)
                 + cp.priority / 1000.0 AS score
        FROM   alohadb_context_profiles cp
        WHERE  lower(cp.term) = lower(p_term)
        UNION ALL
        SELECT ca.canonical_entity, cp.entity_type, cp.context_hint,
               'alias', 0,
               1000000.0 + coalesce(cp.priority, 0) / 1000.0
        FROM   alohadb_context_aliases ca
        LEFT JOIN LATERAL (
               SELECT p.entity_type, p.context_hint, p.priority
               FROM   alohadb_context_profiles p
               WHERE  p.canonical_entity = ca.canonical_entity
               ORDER BY p.priority DESC, p.id
               LIMIT 1) cp ON true
        WHERE  lower(ca.alias) = lower(p_term)
    ),
    best AS (
        SELECT DISTINCT ON (c.canonical_entity) c.*
        FROM   cand c
        ORDER BY c.canonical_entity, c.score DESC, c.path_rank
    )
    SELECT b.canonical_entity, b.entity_type, b.context_hint, b.path, b.score
    FROM   best b
    WHERE  p_term IS NOT NULL
    ORDER BY b.score DESC, b.path_rank, b.canonical_entity
    LIMIT  greatest(coalesce(p_limit, 1), 1);
$$;

-- Batch disambiguation: resolve several terms against one context
CREATE FUNCTION context_disambiguate(
    p_terms   text[],
    p_context text DEFAULT NULL
)
RETURNS TABLE(
    input_term       text,
    canonical_entity text,
    entity_type      text,
    context_hint     text,
    resolution_path  text
)
LANGUAGE sql STABLE
AS $$
    SELECT t.term,
           r.canonical_entity,
           r.entity_type,
           r.context_hint,
           coalesce(r.resolution_path, 'not_found')
    FROM   unnest(p_terms) WITH ORDINALITY AS t(term, ord)
    LEFT JOIN LATERAL context_resolve(t.term, p_context) r ON true
    ORDER BY t.ord;
$$;

-- Registered terms and aliases mentioned in a piece of text
CREATE FUNCTION context_extract(p_text text)
RETURNS TABLE(term text, is_alias boolean)
LANGUAGE sql STABLE STRICT
AS $$
    SELECT DISTINCT lower(cp.term), false
    FROM   alohadb_context_profiles cp
    WHERE  context_mentions(p_text, cp.term)
    UNION
    SELECT DISTINCT lower(ca.alias), true
    FROM   alohadb_context_aliases ca
    WHERE  context_mentions(p_text, ca.alias);
$$;

-- Context-aware reranking of retrieval results (e.g. vector search hits).
--
-- p_candidates: jsonb array of {"id": ..., "text": ..., "score": ...}.
-- Every registered term/alias in p_query is resolved using the query as
-- context.  Each candidate's mentions of those same terms are resolved using
-- the candidate's own text as context.  A candidate that refers to the
-- query's entity gets score * (1 + p_boost); one that only refers to a
-- different sense of the query's terms gets score * p_penalty; others keep
-- their score.  Results are returned best first.
CREATE FUNCTION context_rerank(
    p_candidates jsonb,
    p_query      text,
    p_boost      float8 DEFAULT 0.3,
    p_penalty    float8 DEFAULT 0.5
)
RETURNS TABLE(
    id             text,
    original_score float8,
    context_score  float8,
    final_score    float8,
    matched_entity text
)
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    q_entities text[];
    q_terms    text[];
BEGIN
    IF p_candidates IS NULL OR jsonb_typeof(p_candidates) <> 'array' THEN
        RAISE EXCEPTION 'context_rerank: candidates must be a jsonb array';
    END IF;

    -- Entities the query refers to, and the terms it used to refer to them
    SELECT array_agg(DISTINCT r.canonical_entity),
           array_agg(DISTINCT x.term)
    INTO   q_entities, q_terms
    FROM   context_extract(coalesce(p_query, '')) x
    CROSS JOIN LATERAL context_resolve(x.term, p_query) r;

    IF q_entities IS NULL THEN
        -- No registered term inside the query: try the whole query string
        SELECT array_agg(r.canonical_entity), ARRAY[lower(p_query)]
        INTO   q_entities, q_terms
        FROM   context_resolve(p_query, p_query) r;
    END IF;

    RETURN QUERY
    WITH c AS (
        SELECT e->>'id' AS cid,
               coalesce(e->>'text', '') AS ctext,
               coalesce((e->>'score')::float8, 0.0) AS cscore,
               ord
        FROM   jsonb_array_elements(p_candidates) WITH ORDINALITY AS j(e, ord)
    ),
    -- Entities each candidate refers to, via the query's terms or any
    -- alias/term naming one of the query's entities
    mentions AS (
        SELECT c.cid, c.ord, r.canonical_entity AS ent
        FROM   c
        CROSS JOIN LATERAL context_extract(c.ctext) x
        CROSS JOIN LATERAL context_resolve(x.term, c.ctext) r
        WHERE  x.term = ANY (q_terms)
           OR  r.canonical_entity = ANY (q_entities)
    ),
    verdict AS (
        SELECT c.cid, c.ord, c.cscore,
               (SELECT min(m.ent) FROM mentions m
                WHERE m.ord = c.ord AND m.ent = ANY (q_entities)) AS hit,
               EXISTS (SELECT 1 FROM mentions m
                       WHERE m.ord = c.ord
                         AND NOT (m.ent = ANY (q_entities))) AS conflict
        FROM   c
    )
    SELECT v.cid,
           v.cscore,
           s.adj,
           s.adj,
           v.hit
    FROM   verdict v
    CROSS JOIN LATERAL (
        SELECT CASE
                 WHEN q_entities IS NULL THEN v.cscore
                 WHEN v.hit IS NOT NULL  THEN v.cscore * (1.0 + p_boost)
                 WHEN v.conflict         THEN v.cscore * p_penalty
                 ELSE v.cscore
               END AS adj) s
    ORDER BY s.adj DESC, v.ord;
END;
$$;

-- Retract one sense of a term (p_canonical given) or every sense of it
CREATE FUNCTION context_retract(
    p_term      text,
    p_canonical text DEFAULT NULL
)
RETURNS boolean
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    deleted_count int;
BEGIN
    WITH gone AS (
        DELETE FROM alohadb_context_profiles
        WHERE  term = p_term
          AND  (p_canonical IS NULL OR canonical_entity = p_canonical)
        RETURNING canonical_entity, term, entity_type
    ), logged AS (
        INSERT INTO alohadb_context_evolution
            (canonical_entity, event_type, old_value)
        SELECT canonical_entity, 'retracted',
               jsonb_build_object('term', term, 'entity_type', entity_type)
        FROM   gone
        RETURNING 1
    )
    SELECT count(*) INTO deleted_count FROM logged;

    RETURN deleted_count > 0;
END;
$$;

-- Evolution history for an entity
CREATE FUNCTION context_history(
    p_entity text,
    p_since  timestamptz DEFAULT '-infinity'
)
RETURNS TABLE(
    event_type  text,
    old_value   jsonb,
    new_value   jsonb,
    recorded_at timestamptz,
    recorded_by text
)
LANGUAGE sql STABLE STRICT
AS $$
    SELECT event_type, old_value, new_value, recorded_at, recorded_by
    FROM   alohadb_context_evolution
    WHERE  canonical_entity = p_entity
      AND  recorded_at      >= p_since
    ORDER BY id;
$$;

-- List registered context profiles
CREATE FUNCTION context_list(p_entity_type text DEFAULT NULL)
RETURNS TABLE(
    term             text,
    canonical_entity text,
    entity_type      text,
    domain           text,
    keywords         text[],
    context_hint     text,
    priority         int,
    registered_at    timestamptz
)
LANGUAGE sql STABLE
AS $$
    SELECT term, canonical_entity, entity_type, domain, keywords,
           context_hint, priority, registered_at
    FROM   alohadb_context_profiles
    WHERE  p_entity_type IS NULL OR entity_type = p_entity_type
    ORDER BY term, priority DESC, canonical_entity;
$$;
