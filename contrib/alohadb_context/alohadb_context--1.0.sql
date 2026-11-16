\echo Use "CREATE EXTENSION alohadb_context" to load this file. \quit

-- Context profiles: maps ambiguous terms to their canonical entity in this
-- database's specific business context.
-- e.g. term='Apple' -> canonical_entity='customer:42', entity_type='customer'
-- so even when a vector DB returns 0.94 similarity for the fruit, the context
-- layer overrides with the correct entity.
CREATE TABLE alohadb_context_profiles (
    term             text        NOT NULL PRIMARY KEY,
    canonical_entity text        NOT NULL,
    entity_type      text        NOT NULL,
    context_hint     text,
    priority         int         NOT NULL DEFAULT 100,
    registered_by    text        NOT NULL DEFAULT current_user,
    registered_at    timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX alohadb_context_profiles_entity_idx
    ON alohadb_context_profiles (canonical_entity);
CREATE INDEX alohadb_context_profiles_type_idx
    ON alohadb_context_profiles (entity_type);

-- Alternate names that map to a canonical entity
CREATE TABLE alohadb_context_aliases (
    alias            text        NOT NULL PRIMARY KEY,
    canonical_entity text        NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX alohadb_context_aliases_canonical_idx
    ON alohadb_context_aliases (canonical_entity);

-- Evolution log: tracks how entity contexts change over time.
-- Addresses HydraDB's "tracks how information evolves over time."
CREATE TABLE alohadb_context_evolution (
    id               bigserial   PRIMARY KEY,
    canonical_entity text        NOT NULL,
    event_type       text        NOT NULL,  -- 'registered','updated','aliased','retracted'
    old_value        jsonb,
    new_value        jsonb,
    recorded_at      timestamptz NOT NULL DEFAULT now(),
    recorded_by      text        NOT NULL DEFAULT current_user
);

CREATE INDEX alohadb_context_evolution_entity_idx
    ON alohadb_context_evolution (canonical_entity);
CREATE INDEX alohadb_context_evolution_time_idx
    ON alohadb_context_evolution (recorded_at);

-- Register an entity context profile
CREATE FUNCTION context_register(
    p_term         text,
    p_canonical    text,
    p_entity_type  text,
    p_context_hint text DEFAULT NULL,
    p_priority     int  DEFAULT 100
)
RETURNS void
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    old_rec alohadb_context_profiles%ROWTYPE;
BEGIN
    SELECT * INTO old_rec
    FROM   alohadb_context_profiles
    WHERE  term = p_term;

    INSERT INTO alohadb_context_profiles
        (term, canonical_entity, entity_type, context_hint, priority)
    VALUES
        (p_term, p_canonical, p_entity_type, p_context_hint, p_priority)
    ON CONFLICT (term)
    DO UPDATE SET
        canonical_entity = EXCLUDED.canonical_entity,
        entity_type      = EXCLUDED.entity_type,
        context_hint     = EXCLUDED.context_hint,
        priority         = EXCLUDED.priority,
        updated_at       = now();

    INSERT INTO alohadb_context_evolution
        (canonical_entity, event_type, old_value, new_value)
    VALUES (
        p_canonical,
        CASE WHEN old_rec.term IS NULL THEN 'registered' ELSE 'updated' END,
        CASE WHEN old_rec.term IS NOT NULL THEN
            jsonb_build_object(
                'term',             old_rec.term,
                'canonical_entity', old_rec.canonical_entity,
                'entity_type',      old_rec.entity_type,
                'context_hint',     old_rec.context_hint
            )
        ELSE NULL END,
        jsonb_build_object(
            'term',             p_term,
            'canonical_entity', p_canonical,
            'entity_type',      p_entity_type,
            'context_hint',     p_context_hint
        )
    );
END;
$$;

-- Register an alias for a canonical entity
CREATE FUNCTION context_alias(
    p_alias     text,
    p_canonical text
)
RETURNS void
LANGUAGE plpgsql VOLATILE
AS $$
BEGIN
    INSERT INTO alohadb_context_aliases (alias, canonical_entity)
    VALUES (p_alias, p_canonical)
    ON CONFLICT (alias)
    DO UPDATE SET canonical_entity = EXCLUDED.canonical_entity;

    INSERT INTO alohadb_context_evolution
        (canonical_entity, event_type, new_value)
    VALUES (
        p_canonical,
        'aliased',
        jsonb_build_object('alias', p_alias, 'canonical_entity', p_canonical)
    );
END;
$$;

-- Resolve a term to its canonical entity in this database's business context.
-- Resolution order:
--   1. Exact match against profiles
--   2. Case-insensitive match against profiles
--   3. Alias match (exact, case-insensitive)
-- Returns empty set when term has no registered context.
CREATE FUNCTION context_resolve(p_term text)
RETURNS TABLE(
    canonical_entity text,
    entity_type      text,
    context_hint     text,
    resolution_path  text   -- 'direct' | 'direct_ci' | 'alias'
)
LANGUAGE plpgsql STABLE STRICT
AS $$
BEGIN
    -- 1. Exact match
    RETURN QUERY
    SELECT cp.canonical_entity,
           cp.entity_type,
           cp.context_hint,
           'direct'::text
    FROM   alohadb_context_profiles cp
    WHERE  cp.term = p_term
    LIMIT  1;

    IF FOUND THEN RETURN; END IF;

    -- 2. Case-insensitive match
    RETURN QUERY
    SELECT cp.canonical_entity,
           cp.entity_type,
           cp.context_hint,
           'direct_ci'::text
    FROM   alohadb_context_profiles cp
    WHERE  lower(cp.term) = lower(p_term)
    ORDER BY cp.priority DESC
    LIMIT  1;

    IF FOUND THEN RETURN; END IF;

    -- 3. Alias match
    RETURN QUERY
    SELECT ca.canonical_entity,
           cp.entity_type,
           cp.context_hint,
           'alias'::text
    FROM   alohadb_context_aliases ca
    LEFT JOIN alohadb_context_profiles cp
           ON cp.canonical_entity = ca.canonical_entity
    WHERE  lower(ca.alias) = lower(p_term)
    LIMIT  1;
END;
$$;

-- Batch disambiguation: resolve multiple terms at once
CREATE FUNCTION context_disambiguate(p_terms text[])
RETURNS TABLE(
    input_term       text,
    canonical_entity text,
    entity_type      text,
    context_hint     text,
    resolution_path  text
)
LANGUAGE sql STABLE STRICT
AS $$
    SELECT t.term                                     AS input_term,
           r.canonical_entity,
           r.entity_type,
           r.context_hint,
           COALESCE(r.resolution_path, 'not_found')  AS resolution_path
    FROM   unnest(p_terms) AS t(term)
    LEFT JOIN LATERAL context_resolve(t.term) r ON true;
$$;

-- Context-aware search reranking.
-- Given a query string and a JSONB array of candidates [{id, text, score}],
-- boosts candidates whose text mentions the same canonical entity as the query,
-- and penalises candidates that mention a different entity for the same term.
-- Algorithm: additive multiplier on the original score — no patented ranking
-- methods, just plain score arithmetic on a lookup result.
CREATE FUNCTION context_rerank(
    p_candidates jsonb,          -- array of {id text, text text, score float8}
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
LANGUAGE plpgsql STABLE STRICT
AS $$
DECLARE
    candidate       jsonb;
    cand_id         text;
    cand_text       text;
    cand_score      float8;
    ctx_score       float8;
    matched         text;
    word            text;
    resolved_entity text;
    query_entity    text;
BEGIN
    -- Resolve the query term once
    SELECT canonical_entity INTO query_entity
    FROM   context_resolve(p_query)
    LIMIT  1;

    FOR candidate IN SELECT jsonb_array_elements(p_candidates)
    LOOP
        cand_id    := candidate->>'id';
        cand_text  := candidate->>'text';
        cand_score := (candidate->>'score')::float8;
        ctx_score  := cand_score;
        matched    := NULL;

        FOR word IN SELECT regexp_split_to_table(cand_text, '\s+')
        LOOP
            SELECT canonical_entity INTO resolved_entity
            FROM   context_resolve(word)
            LIMIT  1;

            IF resolved_entity IS NOT NULL THEN
                IF query_entity IS NOT NULL
                   AND resolved_entity = query_entity THEN
                    ctx_score := cand_score * (1.0 + p_boost);
                    matched   := resolved_entity;
                ELSIF query_entity IS NOT NULL
                      AND resolved_entity <> query_entity THEN
                    ctx_score := cand_score * p_penalty;
                END IF;
            END IF;
        END LOOP;

        RETURN QUERY SELECT cand_id, cand_score, ctx_score, ctx_score, matched;
    END LOOP;
END;
$$;

-- Retract a context profile
CREATE FUNCTION context_retract(p_term text)
RETURNS boolean
LANGUAGE plpgsql VOLATILE STRICT
AS $$
DECLARE
    deleted_count int;
    old_canonical text;
BEGIN
    SELECT canonical_entity INTO old_canonical
    FROM   alohadb_context_profiles
    WHERE  term = p_term;

    DELETE FROM alohadb_context_profiles WHERE term = p_term;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;

    IF deleted_count > 0 THEN
        INSERT INTO alohadb_context_evolution
            (canonical_entity, event_type, old_value)
        VALUES (
            old_canonical,
            'retracted',
            jsonb_build_object('term', p_term)
        );
    END IF;

    RETURN deleted_count > 0;
END;
$$;

-- Show evolution history for an entity
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
    ORDER BY recorded_at;
$$;

-- List all registered context profiles
CREATE FUNCTION context_list(p_entity_type text DEFAULT NULL)
RETURNS TABLE(
    term             text,
    canonical_entity text,
    entity_type      text,
    context_hint     text,
    priority         int,
    registered_at    timestamptz
)
LANGUAGE sql STABLE
AS $$
    SELECT term, canonical_entity, entity_type, context_hint,
           priority, registered_at
    FROM   alohadb_context_profiles
    WHERE  p_entity_type IS NULL OR entity_type = p_entity_type
    ORDER BY priority DESC, term;
$$;
