/* contrib/pg_embeddings/pg_embeddings--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_embeddings UPDATE TO '1.1'" to load this file. \quit

-- embed_fn becomes a schema-qualified text signature instead of a
-- regprocedure: pg_upgrade refuses tables with reg* columns other than
-- regclass, regrole and regtype.  If the column was already changed to text
-- by hand (needed before running pg_upgrade from 1.0), re-qualify it.

CREATE FUNCTION embedding__fn_text(p_fn regprocedure)
RETURNS text
LANGUAGE sql STABLE STRICT
SET search_path = pg_catalog
AS $$ SELECT p_fn::text $$;

DO $$
BEGIN
    IF (SELECT atttypid FROM pg_catalog.pg_attribute
         WHERE attrelid = '@extschema@.embedding_model'::regclass
           AND attname = 'embed_fn') = 'regprocedure'::regtype THEN
        ALTER TABLE @extschema@.embedding_model
            ALTER COLUMN embed_fn TYPE text
            USING @extschema@.embedding__fn_text(embed_fn);
    ELSE
        UPDATE @extschema@.embedding_model
           SET embed_fn = @extschema@.embedding__fn_text(embed_fn::regprocedure)
         WHERE embed_fn IS NOT NULL;
    END IF;
END
$$;

CREATE FUNCTION embedding__fn_call(p_fn text)
RETURNS text
LANGUAGE sql STABLE STRICT
AS $$ SELECT @extschema@.embedding__fn_call(p_fn::pg_catalog.regprocedure) $$;

CREATE OR REPLACE FUNCTION embedding_model_register(
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

CREATE OR REPLACE FUNCTION embedding_models()
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
