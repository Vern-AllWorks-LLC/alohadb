/* contrib/pg_embeddings/pg_embeddings--1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_embeddings" to load this file. \quit

/*
 * pg_embeddings -- embedding lifecycle management and hybrid retrieval
 *
 * Model registry (name@version, dims, metric, pluggable SQL embed function),
 * tracked sources with change-detecting triggers and a work queue, batch /
 * background re-embedding with several model versions side by side, exact
 * and ANN-indexable vector search, hybrid vector + full-text search fused
 * with Reciprocal Rank Fusion, and binary / int8 quantization helpers.
 *
 * Prior art / patent notes
 * ------------------------
 * Only long-published, prior-art techniques are implemented here:
 *   - Reciprocal Rank Fusion: Cormack, Clarke & Buettcher, "Reciprocal Rank
 *     Fusion outperforms Condorcet and individual rank learning methods",
 *     SIGIR 2009.  score(d) = sum over lists of 1 / (rrf_k + rank(d)).
 *   - Sign (1-bit) quantization and Hamming-distance prefiltering: classic
 *     random-projection / sign hashing (Charikar, STOC 2002) and earlier
 *     locality-sensitive hashing literature; candidates are re-ranked with
 *     the exact metric (generic two-stage retrieval).
 *   - Scalar int8 quantization: plain symmetric max-abs linear scaling.
 *   - Full-text ranking uses PostgreSQL's own to_tsvector /
 *     websearch_to_tsquery / ts_rank_cd.
 *   - The trigger -> work queue -> worker (FOR UPDATE SKIP LOCKED) pattern
 *     is a generic database job-queue idiom, not any vendor's specific
 *     claimed design.
 * Deliberately NOT implemented: product quantization, anisotropic (ScaNN)
 * quantization, RaBitQ, DiskANN / Vamana / filtered-DiskANN graph indexes.
 * ANN indexes use only the core server's existing hnsw / ivfflat AMs.
 */

------------------------------------------------------------------------------
-- Catalog tables
------------------------------------------------------------------------------

CREATE TABLE embedding_model (
    model_id    serial PRIMARY KEY,
    name        text NOT NULL CHECK (name <> '' AND position('@' IN name) = 0),
    version     text NOT NULL CHECK (version <> ''),
    dims        int  NOT NULL CHECK (dims > 0),
    metric      text NOT NULL CHECK (metric IN ('cosine', 'l2', 'ip', 'l1')),
    -- schema-qualified signature (text, not regprocedure, so that
    -- pg_upgrade can carry the table across major versions)
    embed_fn    text,
    batch       boolean NOT NULL DEFAULT false,
    metadata    jsonb NOT NULL DEFAULT '{}',
    created_at  timestamptz NOT NULL DEFAULT now(),
    retired_at  timestamptz,
    UNIQUE (name, version)
);

CREATE TABLE embedding_source (
    source_id       serial PRIMARY KEY,
    source          regclass NOT NULL UNIQUE,
    key_column      name NOT NULL,
    key_type        regtype NOT NULL,
    content_expr    text NOT NULL,
    store           regclass NOT NULL UNIQUE,
    index_method    text NOT NULL CHECK (index_method IN ('hnsw', 'ivfflat', 'none')),
    active_model_id int NOT NULL REFERENCES embedding_model (model_id)
);

CREATE TABLE embedding_source_model (
    source_id   int NOT NULL REFERENCES embedding_source (source_id) ON DELETE CASCADE,
    model_id    int NOT NULL REFERENCES embedding_model (model_id),
    index_name  name,
    PRIMARY KEY (source_id, model_id)
);

CREATE TABLE embedding_queue (
    id          bigserial PRIMARY KEY,
    source_id   int NOT NULL REFERENCES embedding_source (source_id) ON DELETE CASCADE,
    model_id    int NOT NULL REFERENCES embedding_model (model_id),
    key         text NOT NULL,
    enqueued_at timestamptz NOT NULL DEFAULT now(),
    attempts    int NOT NULL DEFAULT 0,
    last_error  text,
    UNIQUE (source_id, model_id, key)
);

SELECT pg_catalog.pg_extension_config_dump('embedding_model', '');
SELECT pg_catalog.pg_extension_config_dump('embedding_model_model_id_seq', '');
SELECT pg_catalog.pg_extension_config_dump('embedding_source', '');
SELECT pg_catalog.pg_extension_config_dump('embedding_source_source_id_seq', '');
SELECT pg_catalog.pg_extension_config_dump('embedding_source_model', '');
SELECT pg_catalog.pg_extension_config_dump('embedding_queue', '');
SELECT pg_catalog.pg_extension_config_dump('embedding_queue_id_seq', '');

GRANT SELECT ON embedding_model, embedding_source, embedding_source_model,
                embedding_queue TO PUBLIC;

------------------------------------------------------------------------------
-- Quantization helpers
------------------------------------------------------------------------------

-- 1 bit per dimension: 1 if the component is > 0, else 0 (dimension 1 is the
-- leftmost bit).
CREATE FUNCTION embedding_binary_quantize(v vector)
RETURNS bit
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
AS $$
    SELECT pg_catalog.string_agg(CASE WHEN x > 0 THEN '1' ELSE '0' END, ''
                                 ORDER BY o)::varbit
      FROM pg_catalog.unnest(pg_catalog.vector_to_array(v)) WITH ORDINALITY AS u(x, o)
$$;

-- Hamming distance = popcount(a XOR b).  Both codes must be equally long.
CREATE FUNCTION embedding_hamming(a bit, b bit)
RETURNS int
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
AS $$ SELECT pg_catalog.bit_count(a # b)::int $$;

-- Symmetric scalar quantization to [-127, 127].  scale defaults to
-- 127 / max(|x|); value_i = round(x_i * scale).  NOT STRICT because scale has
-- DEFAULT NULL; a NULL vector yields NULL.
CREATE FUNCTION embedding_int8_quantize(v vector, scale float8 DEFAULT NULL)
RETURNS smallint[]
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
AS $$
DECLARE
    arr   real[];
    s     float8 := scale;
    maxab float8;
BEGIN
    IF v IS NULL THEN
        RETURN NULL;
    END IF;
    arr := pg_catalog.vector_to_array(v);
    IF s IS NULL THEN
        SELECT max(abs(x)) INTO maxab FROM unnest(arr) AS x;
        s := CASE WHEN maxab > 0 THEN 127.0 / maxab ELSE 1.0 END;
    ELSIF s <= 0 THEN
        RAISE EXCEPTION 'embedding_int8_quantize: scale must be positive, got %', s;
    END IF;
    RETURN ARRAY(SELECT greatest(-127, least(127, round(x * s)))::smallint
                   FROM unnest(arr) WITH ORDINALITY AS u(x, o) ORDER BY o);
END;
$$;

-- Inverse of embedding_int8_quantize for a known scale.
CREATE FUNCTION embedding_int8_dequantize(q smallint[], scale float8)
RETURNS vector
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
AS $$
    SELECT pg_catalog.array_to_vector(ARRAY(
        SELECT (x / scale)::real FROM pg_catalog.unnest(q) WITH ORDINALITY AS u(x, o)
         ORDER BY o))
$$;

------------------------------------------------------------------------------
-- Internal helpers
------------------------------------------------------------------------------

-- Resolve a model.  Accepts (name, version) or ('name@version', NULL).
CREATE FUNCTION embedding__model_id(p_name text, p_version text DEFAULT NULL)
RETURNS int
LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    n   text := p_name;
    ver text := p_version;
    id  int;
BEGIN
    IF n IS NULL THEN
        RAISE EXCEPTION 'model name must not be NULL';
    END IF;
    IF ver IS NULL THEN
        IF position('@' IN n) = 0 THEN
            RAISE EXCEPTION 'model "%" needs a version (use name@version or pass version)', n;
        END IF;
        ver := split_part(n, '@', 2);
        n   := split_part(n, '@', 1);
    END IF;
    SELECT m.model_id INTO id FROM @extschema@.embedding_model m
     WHERE m.name = n AND m.version = ver;
    IF id IS NULL THEN
        RAISE EXCEPTION 'embedding model "%@%" is not registered', n, ver;
    END IF;
    RETURN id;
END;
$$;

CREATE FUNCTION embedding__source(p_source regclass)
RETURNS embedding_source
LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    s @extschema@.embedding_source;
BEGIN
    SELECT * INTO s FROM @extschema@.embedding_source WHERE source = p_source;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'table % is not tracked by pg_embeddings', p_source
            USING HINT = 'Call embedding_track() first.';
    END IF;
    RETURN s;
END;
$$;

CREATE FUNCTION embedding__distance_op(p_metric text)
RETURNS text
LANGUAGE sql IMMUTABLE STRICT
AS $$
    SELECT CASE p_metric WHEN 'cosine' THEN '<=>' WHEN 'l2' THEN '<->'
                         WHEN 'ip' THEN '<#>' WHEN 'l1' THEN '<+>' END
$$;

CREATE FUNCTION embedding__opclass(p_metric text)
RETURNS text
LANGUAGE sql IMMUTABLE STRICT
AS $$
    SELECT CASE p_metric WHEN 'cosine' THEN 'vector_cosine_ops'
                         WHEN 'l2' THEN 'vector_l2_ops'
                         WHEN 'ip' THEN 'vector_ip_ops' END
$$;

-- Fully qualified call text for a regprocedure (schema.name).
CREATE FUNCTION embedding__fn_call(p_fn regprocedure)
RETURNS text
LANGUAGE sql STABLE STRICT
AS $$
    SELECT pg_catalog.format('%I.%I', n.nspname, p.proname)
      FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
     WHERE p.oid = p_fn
$$;

-- Same, for the stored text form of embed_fn.
CREATE FUNCTION embedding__fn_call(p_fn text)
RETURNS text
LANGUAGE sql STABLE STRICT
AS $$ SELECT @extschema@.embedding__fn_call(p_fn::pg_catalog.regprocedure) $$;

-- Stored form of a function reference: fully schema-qualified signature.
CREATE FUNCTION embedding__fn_text(p_fn regprocedure)
RETURNS text
LANGUAGE sql STABLE STRICT
SET search_path = pg_catalog
AS $$ SELECT p_fn::text $$;

-- Content hash used for change detection.
CREATE FUNCTION embedding__hash(p_content text)
RETURNS bytea
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
AS $$ SELECT pg_catalog.sha256(pg_catalog.convert_to(p_content, 'UTF8')) $$;

-- Reject anything that is not a single SQL boolean expression fragment.
CREATE FUNCTION embedding__check_fragment(p_expr text, p_what text)
RETURNS void
LANGUAGE plpgsql IMMUTABLE
AS $$
BEGIN
    IF p_expr IS NULL THEN
        RETURN;
    END IF;
    IF p_expr ~ ';' OR position('--' IN p_expr) > 0 OR position('/*' IN p_expr) > 0 THEN
        RAISE EXCEPTION '% must be a single SQL expression (no ";" or comments)', p_what;
    END IF;
END;
$$;

------------------------------------------------------------------------------
-- Model registry
------------------------------------------------------------------------------

CREATE FUNCTION embedding_model_register(
    name     text,
    version  text,
    dims     int,
    metric   text DEFAULT 'cosine',
    embed_fn regprocedure DEFAULT NULL,
    metadata jsonb DEFAULT '{}')
RETURNS int
LANGUAGE plpgsql
AS $$
DECLARE
    arg1   oid;
    rett   oid;
    is_batch boolean := false;
    id     int;
BEGIN
    IF name IS NULL OR version IS NULL OR dims IS NULL THEN
        RAISE EXCEPTION 'name, version and dims must not be NULL';
    END IF;
    IF position('@' IN name) > 0 THEN
        RAISE EXCEPTION 'model name must not contain "@" (models are identified as name@version)';
    END IF;
    IF lower(coalesce(metric, 'cosine')) NOT IN ('cosine', 'l2', 'ip', 'l1') THEN
        RAISE EXCEPTION 'unknown metric "%" (expected cosine, l2, ip or l1)', metric;
    END IF;
    IF embed_fn IS NOT NULL THEN
        SELECT CASE WHEN p.pronargs = 1 THEN p.proargtypes[0] END, p.prorettype
          INTO arg1, rett
          FROM pg_catalog.pg_proc p WHERE p.oid = embed_fn;
        IF arg1 = 'text'::regtype AND rett = 'vector'::regtype THEN
            is_batch := false;
        ELSIF arg1 = 'text[]'::regtype AND rett = 'vector[]'::regtype THEN
            is_batch := true;
        ELSE
            RAISE EXCEPTION 'embed_fn % must be fn(text) RETURNS vector or fn(text[]) RETURNS vector[]',
                embed_fn;
        END IF;
    END IF;

    INSERT INTO @extschema@.embedding_model AS m
           (name, version, dims, metric, embed_fn, batch, metadata)
    VALUES (embedding_model_register.name, embedding_model_register.version,
            embedding_model_register.dims, lower(coalesce(metric, 'cosine')),
            @extschema@.embedding__fn_text(embed_fn), is_batch, coalesce(metadata, '{}'))
    RETURNING m.model_id INTO id;
    RETURN id;
EXCEPTION WHEN unique_violation THEN
    RAISE EXCEPTION 'embedding model "%@%" is already registered', name, version;
END;
$$;

CREATE FUNCTION embedding_models()
RETURNS TABLE (model_id int, model text, name text, version text, dims int,
               metric text, embed_fn regprocedure, batch boolean,
               metadata jsonb, created_at timestamptz, retired_at timestamptz)
LANGUAGE sql STABLE
AS $$
    SELECT m.model_id, m.name || '@' || m.version, m.name, m.version, m.dims,
           m.metric, m.embed_fn::pg_catalog.regprocedure, m.batch, m.metadata, m.created_at, m.retired_at
      FROM @extschema@.embedding_model m
     ORDER BY m.name, m.model_id
$$;

-- Retired models are no longer maintained (triggers stop enqueueing for them,
-- pending queue entries are dropped) and cannot be newly tracked or promoted.
-- Existing stored vectors stay queryable until embedding_untrack().
CREATE FUNCTION embedding_model_retire(name text, version text DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    mid int := @extschema@.embedding__model_id(name, version);
    src regclass;
BEGIN
    SELECT s.source INTO src FROM @extschema@.embedding_source s
     WHERE s.active_model_id = mid LIMIT 1;
    IF src IS NOT NULL THEN
        RAISE EXCEPTION 'model is the active model of %', src
            USING HINT = 'Promote another model with embedding_promote() first.';
    END IF;
    UPDATE @extschema@.embedding_model m SET retired_at = coalesce(m.retired_at, now())
     WHERE m.model_id = mid;
    DELETE FROM @extschema@.embedding_queue q WHERE q.model_id = mid;
END;
$$;

------------------------------------------------------------------------------
-- Store-side trigger: dimension enforcement + binary code maintenance
------------------------------------------------------------------------------

CREATE FUNCTION embedding__store_check()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    m @extschema@.embedding_model;
    d int;
BEGIN
    SELECT * INTO m FROM @extschema@.embedding_model WHERE model_id = NEW.model_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'embedding model id % is not registered', NEW.model_id;
    END IF;
    d := vector_dims(NEW.embedding);
    IF d <> m.dims THEN
        RAISE EXCEPTION 'embedding has % dimensions but model %@% requires %',
            d, m.name, m.version, m.dims
            USING ERRCODE = 'data_exception';
    END IF;
    NEW.binary_code := @extschema@.embedding_binary_quantize(NEW.embedding);
    RETURN NEW;
END;
$$;

------------------------------------------------------------------------------
-- Source-side triggers (change detection -> work queue)
------------------------------------------------------------------------------

-- The SECURITY DEFINER helpers below are callable by anyone (the source
-- triggers run as the writing user), so they only act for a session user that
-- may write the tracked table.
CREATE FUNCTION embedding__check_writer(p_source regclass)
RETURNS void
LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
    IF NOT has_table_privilege(session_user, p_source, 'INSERT, UPDATE, DELETE, TRUNCATE') THEN
        RAISE EXCEPTION 'permission denied: % may not write %', session_user, p_source;
    END IF;
END;
$$;

-- Mark the key's stored vectors stale (when the hash differs) and enqueue the
-- key for every maintained model.  SECURITY DEFINER so that writers of the
-- source table need no privileges on the queue / store; it takes only plain
-- values (no user SQL) and touches only catalog-registered tables.
CREATE FUNCTION embedding__enqueue(p_source_id int, p_key text, p_hash bytea)
RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    s  @extschema@.embedding_source;
    sm record;
    old_hash bytea;
BEGIN
    SELECT * INTO s FROM @extschema@.embedding_source WHERE source_id = p_source_id;
    IF NOT FOUND THEN
        RETURN;
    END IF;
    PERFORM @extschema@.embedding__check_writer(s.source);
    FOR sm IN SELECT x.model_id, m.embed_fn IS NOT NULL AS has_fn FROM @extschema@.embedding_source_model x
                JOIN @extschema@.embedding_model m USING (model_id)
               WHERE x.source_id = p_source_id AND m.retired_at IS NULL
    LOOP
        EXECUTE format('SELECT content_hash FROM %s WHERE key = $1::%s AND model_id = $2',
                       s.store, s.key_type)
           INTO old_hash USING p_key, sm.model_id;
        IF old_hash IS NOT DISTINCT FROM p_hash THEN
            -- unchanged (or reverted) content: the stored vector is current
            EXECUTE format('UPDATE %s SET status = ''ready'' WHERE key = $1::%s AND model_id = $2 AND status <> ''ready''',
                           s.store, s.key_type)
              USING p_key, sm.model_id;
            DELETE FROM @extschema@.embedding_queue q
             WHERE q.source_id = p_source_id AND q.model_id = sm.model_id AND q.key = p_key;
            CONTINUE;
        END IF;
        IF old_hash IS NOT NULL THEN
            EXECUTE format('UPDATE %s SET status = ''stale'' WHERE key = $1::%s AND model_id = $2',
                           s.store, s.key_type)
              USING p_key, sm.model_id;
        END IF;
        IF NOT sm.has_fn THEN
            CONTINUE;           -- externally embedded model: caller writes the store
        END IF;
        INSERT INTO @extschema@.embedding_queue (source_id, model_id, key)
        VALUES (p_source_id, sm.model_id, p_key)
        ON CONFLICT (source_id, model_id, key)
        DO UPDATE SET attempts = 0, last_error = NULL;
    END LOOP;
END;
$$;

-- Remove all embeddings and queue entries of a key (or of the whole source
-- when p_key IS NULL, used by TRUNCATE).
CREATE FUNCTION embedding__forget(p_source_id int, p_key text)
RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    s @extschema@.embedding_source;
BEGIN
    SELECT * INTO s FROM @extschema@.embedding_source WHERE source_id = p_source_id;
    IF NOT FOUND THEN
        RETURN;
    END IF;
    PERFORM @extschema@.embedding__check_writer(s.source);
    IF p_key IS NULL THEN
        EXECUTE format('DELETE FROM %s', s.store);
        DELETE FROM @extschema@.embedding_queue q WHERE q.source_id = p_source_id;
    ELSE
        EXECUTE format('DELETE FROM %s WHERE key = $1::%s', s.store, s.key_type) USING p_key;
        DELETE FROM @extschema@.embedding_queue q
         WHERE q.source_id = p_source_id AND q.key = p_key;
    END IF;
END;
$$;


-- Row trigger on tracked sources.  Runs as the writing user; content_expr is
-- evaluated here (SECURITY INVOKER), like any trigger installed by the table
-- owner.
CREATE FUNCTION embedding__source_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    s       @extschema@.embedding_source;
    sql     text;
    new_key text;
    old_key text;
    content text;
BEGIN
    SELECT * INTO s FROM @extschema@.embedding_source WHERE source_id = TG_ARGV[0]::int;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    IF TG_OP = 'TRUNCATE' THEN
        PERFORM @extschema@.embedding__forget(s.source_id, NULL);
        RETURN NULL;
    END IF;

    sql := format('SELECT (%I)::text, (%s)::text FROM (SELECT ($1::%s).*) AS src',
                  s.key_column, s.content_expr, s.source);
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        EXECUTE sql INTO old_key USING OLD;
    END IF;
    IF TG_OP IN ('INSERT', 'UPDATE') THEN
        EXECUTE sql INTO new_key, content USING NEW;
    END IF;

    IF old_key IS NOT NULL AND old_key IS DISTINCT FROM new_key THEN
        PERFORM @extschema@.embedding__forget(s.source_id, old_key);
    END IF;
    IF new_key IS NOT NULL THEN
        IF content IS NULL THEN
            PERFORM @extschema@.embedding__forget(s.source_id, new_key);
        ELSE
            PERFORM @extschema@.embedding__enqueue(s.source_id, new_key,
                                                   @extschema@.embedding__hash(content));
        END IF;
    END IF;
    RETURN NULL;
END;
$$;

------------------------------------------------------------------------------
-- Tracking
------------------------------------------------------------------------------

-- Create the partial ANN index of one model on a store (if the method/metric
-- allow it).  Returns the index name or NULL.
CREATE FUNCTION embedding__create_index(s embedding_source, m embedding_model)
RETURNS name
LANGUAGE plpgsql
AS $$
DECLARE
    opc  text := @extschema@.embedding__opclass(m.metric);
    nsp  name;
    rel  name;
    idx  name;
BEGIN
    IF s.index_method = 'none' THEN
        RETURN NULL;
    END IF;
    IF opc IS NULL THEN
        RAISE NOTICE 'no % operator class for metric "%"; model %@% is searched without an ANN index',
            s.index_method, m.metric, m.name, m.version;
        RETURN NULL;
    END IF;
    SELECT n.nspname, c.relname INTO nsp, rel
      FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     WHERE c.oid = s.store;
    idx := left(rel, 40) || '_m' || m.model_id || '_' || s.index_method || '_idx';
    EXECUTE format('CREATE INDEX %I ON %s USING %s ((embedding::vector(%s)) %s) WHERE model_id = %s',
                   idx, s.store, s.index_method, m.dims, opc, m.model_id);
    RETURN idx;
END;
$$;

CREATE FUNCTION embedding__enqueue_all(s embedding_source, p_model_id int)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
    n bigint;
BEGIN
    IF (SELECT m.embed_fn IS NULL FROM @extschema@.embedding_model m
         WHERE m.model_id = p_model_id) THEN
        RETURN 0;               -- externally embedded model: nothing to queue
    END IF;
    EXECUTE format(
        'INSERT INTO @extschema@.embedding_queue (source_id, model_id, key)
         SELECT $1, $2, (%I)::text FROM %s ORDER BY %I
         ON CONFLICT (source_id, model_id, key) DO NOTHING',
        s.key_column, s.source, s.key_column)
      USING s.source_id, p_model_id;
    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN n;
END;
$$;

CREATE FUNCTION embedding_track(
    source       regclass,
    key_column   text,
    content_expr text,
    model        text,
    version      text,
    index_method text DEFAULT 'hnsw')
RETURNS regclass
LANGUAGE plpgsql
AS $$
DECLARE
    mid     int;
    m       @extschema@.embedding_model;
    s       @extschema@.embedding_source;
    meth    text := lower(coalesce(index_method, 'hnsw'));
    katt    smallint;
    ktype   regtype;
    nsp     name;
    rel     name;
    storename name;
    store   regclass;
BEGIN
    IF source IS NULL OR key_column IS NULL OR content_expr IS NULL OR model IS NULL THEN
        RAISE EXCEPTION 'source, key_column, content_expr and model must not be NULL';
    END IF;
    IF meth NOT IN ('hnsw', 'ivfflat', 'none') THEN
        RAISE EXCEPTION 'index_method must be hnsw, ivfflat or none, got "%"', index_method;
    END IF;
    mid := @extschema@.embedding__model_id(model, version);
    SELECT * INTO m FROM @extschema@.embedding_model WHERE model_id = mid;
    IF m.retired_at IS NOT NULL THEN
        RAISE EXCEPTION 'embedding model %@% is retired', m.name, m.version;
    END IF;

    SELECT n.nspname, c.relname INTO nsp, rel
      FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     WHERE c.oid = source AND c.relkind IN ('r', 'p');
    IF rel IS NULL THEN
        RAISE EXCEPTION '% is not a table', source;
    END IF;
    IF NOT pg_catalog.pg_has_role((SELECT relowner FROM pg_catalog.pg_class WHERE oid = source),
                                  'USAGE') THEN
        RAISE EXCEPTION 'must be owner of table % to track it', source;
    END IF;

    SELECT a.attnum, a.atttypid::regtype INTO katt, ktype
      FROM pg_catalog.pg_attribute a
     WHERE a.attrelid = source AND a.attname = key_column AND a.attnum > 0
       AND NOT a.attisdropped;
    IF katt IS NULL THEN
        RAISE EXCEPTION 'column "%" does not exist in %', key_column, source;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_index i
                    WHERE i.indrelid = source AND i.indisunique AND i.indnkeyatts = 1
                      AND i.indkey[0] = katt AND i.indpred IS NULL) THEN
        RAISE EXCEPTION 'key column "%" of % must have a single-column unique index or primary key',
            key_column, source;
    END IF;

    PERFORM @extschema@.embedding__check_fragment(content_expr, 'content_expr');
    BEGIN
        EXECUTE format('SELECT (%s)::text FROM %s AS src LIMIT 0', content_expr, source);
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'invalid content_expr "%": %', content_expr, SQLERRM;
    END;

    SELECT * INTO s FROM @extschema@.embedding_source es WHERE es.source = embedding_track.source;
    IF FOUND THEN
        IF s.key_column <> key_column OR s.content_expr <> content_expr THEN
            RAISE EXCEPTION '% is already tracked with key_column "%" and content_expr "%"',
                source, s.key_column, s.content_expr
                USING HINT = 'Use embedding_untrack() first to change them.';
        END IF;
        IF EXISTS (SELECT 1 FROM @extschema@.embedding_source_model x
                    WHERE x.source_id = s.source_id AND x.model_id = mid) THEN
            RAISE EXCEPTION 'model %@% is already tracked for %', m.name, m.version, source;
        END IF;
    ELSE
        storename := left(rel, 52) || '_embeddings';
        EXECUTE format(
            'CREATE TABLE %I.%I (
                 key          %s NOT NULL,
                 model_id     int NOT NULL REFERENCES @extschema@.embedding_model (model_id),
                 embedding    vector NOT NULL,
                 binary_code  bit varying,
                 content_hash bytea NOT NULL,
                 embedded_at  timestamptz NOT NULL DEFAULT now(),
                 status       text NOT NULL DEFAULT ''ready''
                              CHECK (status IN (''ready'', ''stale'')),
                 PRIMARY KEY (key, model_id))',
            nsp, storename, ktype);
        store := format('%I.%I', nsp, storename)::regclass;
        EXECUTE format('CREATE INDEX %I ON %s (model_id, key)',
                       left(storename, 50) || '_model_idx', store);
        EXECUTE format('CREATE TRIGGER embedding_store_check BEFORE INSERT OR UPDATE OF embedding, model_id ON %s
                        FOR EACH ROW EXECUTE FUNCTION @extschema@.embedding__store_check()', store);

        INSERT INTO @extschema@.embedding_source
               (source, key_column, key_type, content_expr, store, index_method, active_model_id)
        VALUES (source, key_column, ktype, content_expr, store, meth, mid)
        RETURNING * INTO s;

        EXECUTE format('CREATE TRIGGER embedding_track_row AFTER INSERT OR UPDATE OR DELETE ON %s
                        FOR EACH ROW EXECUTE FUNCTION @extschema@.embedding__source_trigger(%L)',
                       source, s.source_id);
        EXECUTE format('CREATE TRIGGER embedding_track_truncate AFTER TRUNCATE ON %s
                        FOR EACH STATEMENT EXECUTE FUNCTION @extschema@.embedding__source_trigger(%L)',
                       source, s.source_id);
    END IF;

    INSERT INTO @extschema@.embedding_source_model (source_id, model_id, index_name)
    VALUES (s.source_id, mid, @extschema@.embedding__create_index(s, m));

    PERFORM @extschema@.embedding__enqueue_all(s, mid);
    RETURN s.store;
END;
$$;

-- Stop maintaining one model for a source (drops its vectors, index and queue
-- entries), or -- with model NULL -- stop tracking the source entirely (drops
-- triggers and the store table).
CREATE FUNCTION embedding_untrack(source regclass, model text DEFAULT NULL,
                                  version text DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    s   @extschema@.embedding_source := @extschema@.embedding__source(source);
    mid int;
    idx name;
    nsp name;
BEGIN
    IF model IS NULL THEN
        EXECUTE format('DROP TRIGGER IF EXISTS embedding_track_row ON %s', s.source);
        EXECUTE format('DROP TRIGGER IF EXISTS embedding_track_truncate ON %s', s.source);
        DELETE FROM @extschema@.embedding_source es WHERE es.source_id = s.source_id;
        EXECUTE format('DROP TABLE %s', s.store);
        RETURN;
    END IF;
    mid := @extschema@.embedding__model_id(model, version);
    IF mid = s.active_model_id THEN
        RAISE EXCEPTION 'cannot untrack the active model of %', source
            USING HINT = 'Promote another model first, or untrack the whole source.';
    END IF;
    DELETE FROM @extschema@.embedding_source_model x
     WHERE x.source_id = s.source_id AND x.model_id = mid
    RETURNING x.index_name INTO idx;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'model is not tracked for %', source;
    END IF;
    IF idx IS NOT NULL THEN
        SELECT n.nspname INTO nsp FROM pg_catalog.pg_class c
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = s.store;
        EXECUTE format('DROP INDEX IF EXISTS %I.%I', nsp, idx);
    END IF;
    EXECUTE format('DELETE FROM %s WHERE model_id = $1', s.store) USING mid;
    DELETE FROM @extschema@.embedding_queue q WHERE q.source_id = s.source_id AND q.model_id = mid;
END;
$$;

------------------------------------------------------------------------------
-- Processing (background / batch)
------------------------------------------------------------------------------

-- Upsert one computed vector.
CREATE FUNCTION embedding__store_put(s embedding_source, p_model_id int, p_key text,
                                     p_vec vector, p_hash bytea)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_vec IS NULL THEN
        RAISE EXCEPTION 'embed function returned NULL';
    END IF;
    EXECUTE format(
        'INSERT INTO %s AS st (key, model_id, embedding, content_hash, embedded_at, status)
         VALUES ($1::%s, $2, $3, $4, now(), ''ready'')
         ON CONFLICT (key, model_id) DO UPDATE
            SET embedding = EXCLUDED.embedding, content_hash = EXCLUDED.content_hash,
                embedded_at = EXCLUDED.embedded_at, status = ''ready''',
        s.store, s.key_type)
      USING p_key, p_model_id, p_vec, p_hash;
END;
$$;

-- Pull up to max_rows queue entries (FOR UPDATE SKIP LOCKED, so several
-- workers can run concurrently), compute the content, call the model's
-- embed_fn and upsert.  Returns the number of vectors written.  Entries whose
-- embedding fails are kept with attempts/last_error; after 5 attempts they are
-- skipped ("failed" in embedding_status()).  Models registered without an
-- embed_fn are never queued: write their vectors into the store table
-- directly (dimension check still applies).
--
-- Background scheduling (no dependency on pg_cronjobs; any scheduler that
-- runs SQL works), e.g. with pg_cronjobs / pg_cron-style syntax:
--   SELECT cron.schedule('embed-worker', '* * * * *',
--                        'SELECT embedding_process(500)');
-- Several workers may run concurrently; each call commits as one transaction,
-- so keep max_rows moderate when embed_fn is slow (e.g. a remote API).
CREATE FUNCTION embedding_process(max_rows int DEFAULT 100, source regclass DEFAULT NULL)
RETURNS int
LANGUAGE plpgsql
AS $$
DECLARE
    q        record;
    s        @extschema@.embedding_source;
    m        @extschema@.embedding_model;
    content  text;
    h        bytea;
    old_hash bytea;
    vec      vector;
    vecs     vector[];
    done     int := 0;
    -- pending batch (for fn(text[]) models)
    b_ids    bigint[] := '{}';
    b_keys   text[] := '{}';
    b_texts  text[] := '{}';
    b_hashes bytea[] := '{}';
    cur_src  int;
    cur_mod  int;
    i        int;
BEGIN
    IF coalesce(max_rows, 0) <= 0 THEN
        RETURN 0;
    END IF;
    FOR q IN
        WITH picked AS (
            SELECT qq.id, qq.source_id, qq.model_id, qq.key
              FROM @extschema@.embedding_queue qq
             WHERE qq.attempts < 5
               AND (embedding_process.source IS NULL OR qq.source_id =
                    (SELECT es.source_id FROM @extschema@.embedding_source es
                      WHERE es.source = embedding_process.source))
             ORDER BY qq.id
             LIMIT max_rows
               FOR UPDATE SKIP LOCKED)
        SELECT * FROM picked ORDER BY source_id, model_id, id
    LOOP
        -- flush a pending batch when (source, model) changes
        IF cur_src IS DISTINCT FROM q.source_id OR cur_mod IS DISTINCT FROM q.model_id THEN
            IF cardinality(b_ids) > 0 THEN
                BEGIN
                    EXECUTE format('SELECT %s($1)', @extschema@.embedding__fn_call(m.embed_fn))
                       INTO vecs USING b_texts;
                    IF coalesce(cardinality(vecs), 0) <> cardinality(b_ids) THEN
                        RAISE EXCEPTION 'batch embed function returned % vectors for % inputs',
                            coalesce(cardinality(vecs), 0), cardinality(b_ids);
                    END IF;
                    FOR i IN 1 .. cardinality(b_ids) LOOP
                        PERFORM @extschema@.embedding__store_put(s, m.model_id, b_keys[i], vecs[i], b_hashes[i]);
                    END LOOP;
                    DELETE FROM @extschema@.embedding_queue WHERE id = ANY (b_ids);
                    done := done + cardinality(b_ids);
                EXCEPTION WHEN OTHERS THEN
                    UPDATE @extschema@.embedding_queue
                       SET attempts = attempts + 1, last_error = SQLERRM
                     WHERE id = ANY (b_ids);
                END;
                b_ids := '{}'; b_keys := '{}'; b_texts := '{}'; b_hashes := '{}';
            END IF;
            cur_src := q.source_id;
            cur_mod := q.model_id;
            SELECT * INTO s FROM @extschema@.embedding_source WHERE source_id = q.source_id;
            SELECT * INTO m FROM @extschema@.embedding_model WHERE model_id = q.model_id;
        END IF;

        IF m.retired_at IS NOT NULL THEN
            DELETE FROM @extschema@.embedding_queue WHERE id = q.id;
            CONTINUE;
        END IF;

        BEGIN
            content := NULL;
            EXECUTE format('SELECT (%s)::text FROM %s AS src WHERE %I = $1::%s',
                           s.content_expr, s.source, s.key_column, s.key_type)
               INTO content USING q.key;
            IF content IS NULL THEN
                -- row vanished or has no content: nothing to embed
                EXECUTE format('DELETE FROM %s WHERE key = $1::%s AND model_id = $2',
                               s.store, s.key_type) USING q.key, q.model_id;
                DELETE FROM @extschema@.embedding_queue WHERE id = q.id;
                CONTINUE;
            END IF;
            h := @extschema@.embedding__hash(content);
            EXECUTE format('SELECT content_hash FROM %s WHERE key = $1::%s AND model_id = $2',
                           s.store, s.key_type) INTO old_hash USING q.key, q.model_id;
            IF old_hash = h THEN
                -- content reverted / unchanged: the stored vector is current
                EXECUTE format('UPDATE %s SET status = ''ready'' WHERE key = $1::%s AND model_id = $2',
                               s.store, s.key_type) USING q.key, q.model_id;
                DELETE FROM @extschema@.embedding_queue WHERE id = q.id;
                CONTINUE;
            END IF;
            IF m.embed_fn IS NULL THEN
                RAISE EXCEPTION 'model %@% has no embed_fn; insert vectors into % directly',
                    m.name, m.version, s.store;
            END IF;
            IF m.batch THEN
                b_ids := b_ids || q.id;
                b_keys := b_keys || q.key;
                b_texts := b_texts || content;
                b_hashes := b_hashes || h;
                CONTINUE;
            END IF;
            EXECUTE format('SELECT %s($1)', @extschema@.embedding__fn_call(m.embed_fn))
               INTO vec USING content;
            PERFORM @extschema@.embedding__store_put(s, m.model_id, q.key, vec, h);
            DELETE FROM @extschema@.embedding_queue WHERE id = q.id;
            done := done + 1;
        EXCEPTION WHEN OTHERS THEN
            UPDATE @extschema@.embedding_queue
               SET attempts = attempts + 1, last_error = SQLERRM
             WHERE id = q.id;
        END;
    END LOOP;

    -- final batch flush
    IF cardinality(b_ids) > 0 THEN
        BEGIN
            EXECUTE format('SELECT %s($1)', @extschema@.embedding__fn_call(m.embed_fn))
               INTO vecs USING b_texts;
            IF coalesce(cardinality(vecs), 0) <> cardinality(b_ids) THEN
                RAISE EXCEPTION 'batch embed function returned % vectors for % inputs',
                    coalesce(cardinality(vecs), 0), cardinality(b_ids);
            END IF;
            FOR i IN 1 .. cardinality(b_ids) LOOP
                PERFORM @extschema@.embedding__store_put(s, m.model_id, b_keys[i], vecs[i], b_hashes[i]);
            END LOOP;
            DELETE FROM @extschema@.embedding_queue WHERE id = ANY (b_ids);
            done := done + cardinality(b_ids);
        EXCEPTION WHEN OTHERS THEN
            UPDATE @extschema@.embedding_queue
               SET attempts = attempts + 1, last_error = SQLERRM
             WHERE id = ANY (b_ids);
        END;
    END IF;
    RETURN done;
END;
$$;

-- Start maintaining to_model for source and enqueue every row.  The current
-- active model stays in use for search until embedding_promote().
CREATE FUNCTION embedding_reembed(source regclass, from_model text, from_version text,
                                  to_model text, to_version text)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
    s    @extschema@.embedding_source := @extschema@.embedding__source(source);
    fid  int := @extschema@.embedding__model_id(from_model, from_version);
    tid  int := @extschema@.embedding__model_id(to_model, to_version);
    m    @extschema@.embedding_model;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM @extschema@.embedding_source_model x
                    WHERE x.source_id = s.source_id AND x.model_id = fid) THEN
        RAISE EXCEPTION 'from-model is not tracked for %', source;
    END IF;
    SELECT * INTO m FROM @extschema@.embedding_model WHERE model_id = tid;
    IF m.retired_at IS NOT NULL THEN
        RAISE EXCEPTION 'embedding model %@% is retired', m.name, m.version;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM @extschema@.embedding_source_model x
                    WHERE x.source_id = s.source_id AND x.model_id = tid) THEN
        INSERT INTO @extschema@.embedding_source_model (source_id, model_id, index_name)
        VALUES (s.source_id, tid, @extschema@.embedding__create_index(s, m));
    END IF;
    RETURN @extschema@.embedding__enqueue_all(s, tid);
END;
$$;

CREATE FUNCTION embedding_promote(source regclass, model text, version text DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    s   @extschema@.embedding_source := @extschema@.embedding__source(source);
    mid int := @extschema@.embedding__model_id(model, version);
    pending bigint;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM @extschema@.embedding_source_model x
                    WHERE x.source_id = s.source_id AND x.model_id = mid) THEN
        RAISE EXCEPTION 'model is not tracked for %', source
            USING HINT = 'Use embedding_reembed() to start embedding with it.';
    END IF;
    IF EXISTS (SELECT 1 FROM @extschema@.embedding_model m
                WHERE m.model_id = mid AND m.retired_at IS NOT NULL) THEN
        RAISE EXCEPTION 'cannot promote a retired model';
    END IF;
    SELECT count(*) INTO pending FROM @extschema@.embedding_queue q
     WHERE q.source_id = s.source_id AND q.model_id = mid;
    IF pending > 0 THEN
        RAISE WARNING 'promoting with % rows still queued for this model; they are missing from search until processed',
            pending;
    END IF;
    UPDATE @extschema@.embedding_source es SET active_model_id = mid
     WHERE es.source_id = s.source_id;
END;
$$;

CREATE FUNCTION embedding_status(source regclass)
RETURNS TABLE (model text, active boolean, status text, count bigint)
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    s @extschema@.embedding_source := @extschema@.embedding__source(source);
BEGIN
    RETURN QUERY EXECUTE format(
        'WITH c AS (
             SELECT st.model_id, st.status, count(*) AS n FROM %s st GROUP BY 1, 2
             UNION ALL
             SELECT q.model_id, CASE WHEN q.attempts >= 5 THEN ''failed'' ELSE ''queued'' END,
                    count(*)
               FROM @extschema@.embedding_queue q WHERE q.source_id = $1 GROUP BY 1, 2)
         SELECT m.name || ''@'' || m.version, m.model_id = $2, c.status, c.n
           FROM c JOIN @extschema@.embedding_model m USING (model_id)
          ORDER BY m.name, m.version, c.status',
        s.store)
      USING s.source_id, s.active_model_id;
END;
$$;

-- Model predicate for queries on a store table.  Current AlohaDB builds ship
-- the pgvector-based hnsw / ivfflat access methods, whose ordering operators
-- declare a float8 sort family, so `model_id = N` lets the planner use the
-- per-model partial ANN index.  Older AlohaDB builds lacked the sort family
-- (pg_amop.amopsortfamily = 0) and their ANN indexes could not serve ORDER BY
-- distance; there the predicate is written so that it does not imply the
-- partial-index predicate and an exact scan is used instead.
CREATE FUNCTION embedding__model_pred(p_alias text)
RETURNS text
LANGUAGE sql STABLE
AS $$
    SELECT CASE WHEN EXISTS (
                    SELECT 1 FROM pg_catalog.pg_amop o
                      JOIN pg_catalog.pg_am a ON a.oid = o.amopmethod
                     WHERE a.amname IN ('hnsw', 'ivfflat') AND o.amoppurpose = 'o')
                 AND NOT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_amop o
                      JOIN pg_catalog.pg_am a ON a.oid = o.amopmethod
                     WHERE a.amname IN ('hnsw', 'ivfflat') AND o.amoppurpose = 'o'
                       AND o.amopsortfamily = 0)
                THEN pg_catalog.format('%I.model_id = $2', p_alias)
                ELSE pg_catalog.format('(%I.model_id + 0) = $2', p_alias)
           END
$$;

------------------------------------------------------------------------------
-- Search
------------------------------------------------------------------------------

-- Pick the model to search: explicit (must be tracked) or the active one.
CREATE FUNCTION embedding__search_model(s embedding_source, model text, version text)
RETURNS embedding_model
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    mid int;
    m   @extschema@.embedding_model;
BEGIN
    IF model IS NULL THEN
        mid := s.active_model_id;
    ELSE
        mid := @extschema@.embedding__model_id(model, version);
        IF NOT EXISTS (SELECT 1 FROM @extschema@.embedding_source_model x
                        WHERE x.source_id = s.source_id AND x.model_id = mid) THEN
            RAISE EXCEPTION 'model is not tracked for %', s.source;
        END IF;
    END IF;
    SELECT * INTO m FROM @extschema@.embedding_model WHERE model_id = mid;
    RETURN m;
END;
$$;

CREATE FUNCTION embedding__check_query(m embedding_model, q vector)
RETURNS void
LANGUAGE plpgsql IMMUTABLE
AS $$
BEGIN
    IF q IS NULL THEN
        RAISE EXCEPTION 'query vector must not be NULL';
    END IF;
    IF vector_dims(q) <> m.dims THEN
        RAISE EXCEPTION 'query vector has % dimensions but model %@% requires %',
            vector_dims(q), m.name, m.version, m.dims;
    END IF;
END;
$$;

-- k nearest source rows by the model's metric.  The store is joined to the
-- source, so rows the caller cannot see (RLS) or that were deleted are never
-- returned.  key is the source key rendered as text.
CREATE FUNCTION embedding_search(source regclass, query vector, k int DEFAULT 10,
                                 model text DEFAULT NULL, version text DEFAULT NULL)
RETURNS TABLE (key text, distance float8)
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    s  @extschema@.embedding_source := @extschema@.embedding__source(source);
    m  @extschema@.embedding_model := @extschema@.embedding__search_model(s, model, version);
    op text := @extschema@.embedding__distance_op(m.metric);
BEGIN
    PERFORM @extschema@.embedding__check_query(m, query);
    RETURN QUERY EXECUTE format(
        'SELECT st.key::text, (st.embedding::vector(%s) %s $1)::float8 AS distance
           FROM %s st JOIN %s src ON src.%I = st.key
          WHERE %s
          ORDER BY st.embedding::vector(%s) %s $1, st.key
          LIMIT $3',
        m.dims, op, s.store, s.source, s.key_column,
        @extschema@.embedding__model_pred('st'), m.dims, op)
      USING query, m.model_id, coalesce(k, 10);
END;
$$;

-- Embed query_text with the model's embed_fn, then embedding_search().
CREATE FUNCTION embedding_embed(model text, version text, input text)
RETURNS vector
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    m   @extschema@.embedding_model;
    vec vector;
    vecs vector[];
BEGIN
    SELECT * INTO m FROM @extschema@.embedding_model em
     WHERE em.model_id = @extschema@.embedding__model_id(embedding_embed.model,
                                                       embedding_embed.version);
    IF m.embed_fn IS NULL THEN
        RAISE EXCEPTION 'model %@% has no embed_fn', m.name, m.version;
    END IF;
    IF input IS NULL THEN
        RETURN NULL;
    END IF;
    IF m.batch THEN
        EXECUTE format('SELECT %s($1)', @extschema@.embedding__fn_call(m.embed_fn))
           INTO vecs USING ARRAY[input];
        vec := vecs[1];
    ELSE
        EXECUTE format('SELECT %s($1)', @extschema@.embedding__fn_call(m.embed_fn))
           INTO vec USING input;
    END IF;
    RETURN vec;
END;
$$;

CREATE FUNCTION embedding_search_text(source regclass, query_text text, k int DEFAULT 10,
                                      model text DEFAULT NULL, version text DEFAULT NULL)
RETURNS TABLE (key text, distance float8)
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    s  @extschema@.embedding_source := @extschema@.embedding__source(source);
    m  @extschema@.embedding_model := @extschema@.embedding__search_model(s, model, version);
BEGIN
    RETURN QUERY SELECT * FROM @extschema@.embedding_search(
        source, @extschema@.embedding_embed(m.name, m.version, query_text), k,
        m.name, m.version);
END;
$$;

-- Two-stage search: Hamming-distance prefilter on the stored sign codes
-- (embedding_binary_quantize) to `candidates` rows, then exact re-ranking with
-- the model's metric.  Best suited to cosine / ip models on roughly centred
-- embeddings.
CREATE FUNCTION embedding_search_rerank(source regclass, query vector, k int DEFAULT 10,
                                        candidates int DEFAULT 100,
                                        model text DEFAULT NULL, version text DEFAULT NULL)
RETURNS TABLE (key text, distance float8, hamming int)
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    s  @extschema@.embedding_source := @extschema@.embedding__source(source);
    m  @extschema@.embedding_model := @extschema@.embedding__search_model(s, model, version);
    op text := @extschema@.embedding__distance_op(m.metric);
BEGIN
    PERFORM @extschema@.embedding__check_query(m, query);
    RETURN QUERY EXECUTE format(
        'WITH cand AS (
             SELECT st.key, st.embedding,
                    @extschema@.embedding_hamming(st.binary_code, $4) AS ham
               FROM %s st JOIN %s src ON src.%I = st.key
              WHERE %s
              ORDER BY ham, st.key
              LIMIT $5)
         SELECT cand.key::text, (cand.embedding %s $1)::float8 AS distance, cand.ham
           FROM cand
          ORDER BY 2, cand.key
          LIMIT $3',
        s.store, s.source, s.key_column, @extschema@.embedding__model_pred('st'), op)
      USING query, m.model_id, coalesce(k, 10),
            @extschema@.embedding_binary_quantize(query),
            greatest(coalesce(candidates, 100), coalesce(k, 10));
END;
$$;

-- Hybrid retrieval in a single SQL statement: up to `pool` ANN candidates
-- (pool = greatest(4*k, 20)) ranked by vector distance plus up to `pool`
-- full-text hits ranked by ts_rank_cd(websearch_to_tsquery(query_text)),
-- fused with Reciprocal Rank Fusion: score = sum 1/(rrf_k + rank).
--
-- text_column: a text or tsvector column of the source; when NULL the
-- tracked content_expr is used as the document.  filter: optional SQL boolean
-- expression over the source row (alias "src", columns may be unqualified)
-- applied to both candidate lists before ranking.  It is interpolated into the
-- query and runs with the caller's privileges (the function is SECURITY
-- INVOKER, RLS on the source applies); ";" and comments are rejected, but
-- never pass untrusted end-user input as filter.
CREATE FUNCTION embedding_hybrid_search(
    source     regclass,
    query_vec  vector,
    query_text text,
    k          int DEFAULT 10,
    filter     text DEFAULT NULL,
    text_column text DEFAULT NULL,
    rrf_k      int DEFAULT 60,
    ts_config  regconfig DEFAULT 'english')
RETURNS TABLE (key text, score float8, vector_rank int, text_rank int)
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    s    @extschema@.embedding_source := @extschema@.embedding__source(source);
    m    @extschema@.embedding_model := @extschema@.embedding__search_model(s, NULL, NULL);
    op   text := @extschema@.embedding__distance_op(m.metric);
    kk   int := coalesce(k, 10);
    pool int;
    doc  text;
    tsv  text;
    ttype regtype;
    flt  text;
BEGIN
    IF query_vec IS NULL AND query_text IS NULL THEN
        RAISE EXCEPTION 'query_vec and query_text must not both be NULL';
    END IF;
    IF query_vec IS NOT NULL THEN
        PERFORM @extschema@.embedding__check_query(m, query_vec);
    END IF;
    IF coalesce(rrf_k, 60) < 0 THEN
        RAISE EXCEPTION 'rrf_k must be >= 0';
    END IF;
    pool := greatest(4 * kk, 20);
    PERFORM @extschema@.embedding__check_fragment(filter, 'filter');
    flt := CASE WHEN filter IS NULL OR btrim(filter) = '' THEN 'true'
                ELSE '(' || filter || ')' END;

    IF text_column IS NULL THEN
        doc := '(' || s.content_expr || ')::text';
        tsv := format('to_tsvector(%L::regconfig, %s)', ts_config, doc);
    ELSE
        SELECT a.atttypid::regtype INTO ttype FROM pg_catalog.pg_attribute a
         WHERE a.attrelid = source AND a.attname = text_column AND a.attnum > 0
           AND NOT a.attisdropped;
        IF ttype IS NULL THEN
            RAISE EXCEPTION 'column "%" does not exist in %', text_column, source;
        END IF;
        IF ttype = 'tsvector'::regtype THEN
            tsv := format('src.%I', text_column);
        ELSE
            tsv := format('to_tsvector(%L::regconfig, src.%I::text)', ts_config, text_column);
        END IF;
    END IF;

    RETURN QUERY EXECUTE format(
        'WITH vec AS (
             SELECT v.key, row_number() OVER (ORDER BY v.d, v.key)::int AS r
               FROM (SELECT st.key, st.embedding::vector(%1$s) %2$s $1 AS d
                       FROM %3$s st
                       JOIN (SELECT * FROM %4$s src WHERE %6$s) src ON src.%5$I = st.key
                      WHERE $1 IS NOT NULL AND %9$s
                      ORDER BY st.embedding::vector(%1$s) %2$s $1, st.key
                      LIMIT $5) v),
         fts AS (
             SELECT t.key, row_number() OVER (ORDER BY t.rank DESC, t.key)::int AS r
               FROM (SELECT src.%5$I AS key, ts_rank_cd(%7$s, tq) AS rank
                       FROM (SELECT * FROM %4$s src WHERE %6$s) src,
                            websearch_to_tsquery(%8$L::regconfig, $3) AS tq
                      WHERE $3 IS NOT NULL AND %7$s @@ tq
                      ORDER BY 2 DESC, 1
                      LIMIT $5) t)
         SELECT coalesce(vec.key, fts.key)::text,
                (coalesce(1.0 / ($6 + vec.r), 0) + coalesce(1.0 / ($6 + fts.r), 0))::float8 AS score,
                vec.r, fts.r
           FROM vec FULL JOIN fts ON vec.key = fts.key
          ORDER BY 2 DESC, coalesce(vec.key, fts.key)
          LIMIT $4',
        m.dims, op, s.store, s.source, s.key_column, flt, tsv, ts_config,
        @extschema@.embedding__model_pred('st'))
      USING query_vec, m.model_id, query_text, kk, pool, coalesce(rrf_k, 60);
END;
$$;
