/* contrib/pg_kv/pg_kv--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_kv" to load this file. \quit

-- Key-value storage in an unlogged table: writes skip the WAL and the table
-- is emptied after a crash, which is how a cache should behave.  Because it
-- is an ordinary table, every operation is transactional.  Expired rows are
-- invisible to kv_get and are removed by kv_purge_expired(), e.g.
--   SELECT cron_schedule('*/5 * * * *', 'SELECT kv_purge_expired()');
-- The lower fillfactor leaves room for HOT updates of hot keys.
CREATE UNLOGGED TABLE kv_store (
    key        text PRIMARY KEY,
    value      text NOT NULL,
    expires_at timestamptz
) WITH (fillfactor = 70, autovacuum_vacuum_scale_factor = 0.05);

CREATE INDEX kv_store_expires_idx ON kv_store (expires_at)
    WHERE expires_at IS NOT NULL;

COMMENT ON TABLE kv_store IS 'pg_kv entries (unlogged: emptied after a crash)';

CREATE FUNCTION kv_set(key text, value text, ttl interval DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql VOLATILE
SET search_path = @extschema@, pg_temp
AS $$
#variable_conflict use_column
BEGIN
    IF key IS NULL OR value IS NULL THEN
        RAISE EXCEPTION 'pg_kv key and value must not be NULL';
    END IF;
    IF ttl IS NOT NULL AND ttl <= interval '0' THEN
        RAISE EXCEPTION 'pg_kv TTL must be positive';
    END IF;
    INSERT INTO kv_store AS s (key, value, expires_at)
    VALUES (kv_set.key, kv_set.value, clock_timestamp() + ttl)
    ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at;
END
$$;

CREATE FUNCTION kv_get(key text)
RETURNS text
LANGUAGE sql VOLATILE STRICT
SET search_path = @extschema@, pg_temp
AS $$
    SELECT s.value FROM kv_store s
    WHERE s.key = kv_get.key
      AND (s.expires_at IS NULL OR s.expires_at > clock_timestamp())
$$;

CREATE FUNCTION kv_get_many(keys text[])
RETURNS TABLE (key text, value text)
LANGUAGE sql VOLATILE STRICT
SET search_path = @extschema@, pg_temp
AS $$
    SELECT s.key, s.value FROM kv_store s
    WHERE s.key = ANY (kv_get_many.keys)
      AND (s.expires_at IS NULL OR s.expires_at > clock_timestamp())
$$;

CREATE FUNCTION kv_del(key text)
RETURNS boolean
LANGUAGE sql VOLATILE STRICT
SET search_path = @extschema@, pg_temp
AS $$
    WITH d AS (
        DELETE FROM kv_store s WHERE s.key = kv_del.key
        RETURNING s.expires_at
    )
    SELECT EXISTS (SELECT 1 FROM d
                   WHERE d.expires_at IS NULL OR d.expires_at > clock_timestamp())
$$;

-- Store only if the key is absent (or expired); true if stored.
CREATE FUNCTION kv_add(key text, value text, ttl interval DEFAULT NULL)
RETURNS boolean
LANGUAGE plpgsql VOLATILE
SET search_path = @extschema@, pg_temp
AS $$
#variable_conflict use_column
DECLARE
    stored boolean;
BEGIN
    IF key IS NULL OR value IS NULL THEN
        RAISE EXCEPTION 'pg_kv key and value must not be NULL';
    END IF;
    IF ttl IS NOT NULL AND ttl <= interval '0' THEN
        RAISE EXCEPTION 'pg_kv TTL must be positive';
    END IF;
    INSERT INTO kv_store AS s (key, value, expires_at)
    VALUES (kv_add.key, kv_add.value, clock_timestamp() + ttl)
    ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at
        WHERE s.expires_at IS NOT NULL AND s.expires_at <= clock_timestamp()
    RETURNING true INTO stored;
    RETURN coalesce(stored, false);
END
$$;

-- Add delta to an integer value (a missing or expired key counts as 0).
-- An existing key keeps its expiry; ttl applies when the key is created.
CREATE FUNCTION kv_incr(key text, delta bigint DEFAULT 1, ttl interval DEFAULT NULL)
RETURNS bigint
LANGUAGE plpgsql VOLATILE
SET search_path = @extschema@, pg_temp
AS $$
#variable_conflict use_column
DECLARE
    result bigint;
BEGIN
    IF key IS NULL THEN
        RETURN NULL;
    END IF;
    IF ttl IS NOT NULL AND ttl <= interval '0' THEN
        RAISE EXCEPTION 'pg_kv TTL must be positive';
    END IF;
    INSERT INTO kv_store AS s (key, value, expires_at)
    VALUES (kv_incr.key, coalesce(delta, 1)::text, clock_timestamp() + ttl)
    ON CONFLICT (key) DO UPDATE
        SET value = CASE
                WHEN s.expires_at IS NOT NULL AND s.expires_at <= clock_timestamp()
                    THEN EXCLUDED.value
                ELSE (s.value::bigint + coalesce(delta, 1))::text
            END,
            expires_at = CASE
                WHEN s.expires_at IS NOT NULL AND s.expires_at <= clock_timestamp()
                    THEN EXCLUDED.expires_at
                ELSE s.expires_at
            END
    RETURNING s.value::bigint INTO result;
    RETURN result;
EXCEPTION
    WHEN invalid_text_representation THEN
        RAISE EXCEPTION 'pg_kv value for key "%" is not an integer', key
            USING ERRCODE = 'invalid_text_representation';
END
$$;

-- Set (or with NULL, remove) the expiry of an existing key.
CREATE FUNCTION kv_expire(key text, ttl interval)
RETURNS boolean
LANGUAGE plpgsql VOLATILE
SET search_path = @extschema@, pg_temp
AS $$
#variable_conflict use_column
BEGIN
    IF ttl IS NOT NULL AND ttl <= interval '0' THEN
        RAISE EXCEPTION 'pg_kv TTL must be positive';
    END IF;
    UPDATE kv_store s SET expires_at = clock_timestamp() + ttl
    WHERE s.key = kv_expire.key
      AND (s.expires_at IS NULL OR s.expires_at > clock_timestamp());
    RETURN FOUND;
END
$$;

-- Remaining time to live: infinity if the key never expires, NULL if absent.
CREATE FUNCTION kv_ttl(key text)
RETURNS interval
LANGUAGE sql VOLATILE STRICT
SET search_path = @extschema@, pg_temp
AS $$
    SELECT coalesce(s.expires_at - clock_timestamp(), interval 'infinity')
    FROM kv_store s
    WHERE s.key = kv_ttl.key
      AND (s.expires_at IS NULL OR s.expires_at > clock_timestamp())
$$;

CREATE FUNCTION kv_keys(pattern text DEFAULT '%')
RETURNS SETOF text
LANGUAGE sql VOLATILE
SET search_path = @extschema@, pg_temp
AS $$
    SELECT s.key FROM kv_store s
    WHERE s.key LIKE coalesce(pattern, '%') COLLATE "C"
      AND (s.expires_at IS NULL OR s.expires_at > clock_timestamp())
    ORDER BY s.key COLLATE "C"
$$;

CREATE FUNCTION kv_purge_expired()
RETURNS bigint
LANGUAGE sql VOLATILE
SET search_path = @extschema@, pg_temp
AS $$
    WITH d AS (
        DELETE FROM kv_store s
        WHERE s.expires_at IS NOT NULL AND s.expires_at <= clock_timestamp()
        RETURNING 1
    )
    SELECT count(*) FROM d
$$;

CREATE FUNCTION kv_flush()
RETURNS bigint
LANGUAGE sql VOLATILE
SET search_path = @extschema@, pg_temp
AS $$
    WITH d AS (DELETE FROM kv_store RETURNING 1)
    SELECT count(*) FROM d
$$;

-- Like any table, kv_store is private to its owner until granted.
REVOKE ALL ON kv_store FROM PUBLIC;
REVOKE ALL ON FUNCTION kv_set(text, text, interval), kv_get(text), kv_get_many(text[]),
    kv_del(text), kv_add(text, text, interval), kv_incr(text, bigint, interval),
    kv_expire(text, interval), kv_ttl(text), kv_keys(text), kv_purge_expired(),
    kv_flush() FROM PUBLIC;
