/* contrib/pg_tsfunc/pg_tsfunc--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_tsfunc" to load this file. \quit

--
-- Point type shared by all series-returning functions
--
CREATE TYPE ts_point AS (ts timestamptz, value float8);

-- Expand a ts_point[] into rows (plain unnest() works too)
CREATE FUNCTION ts_points(points ts_point[])
RETURNS TABLE(ts timestamptz, value float8)
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
BEGIN ATOMIC
    SELECT u.ts, u.value FROM unnest(points) WITH ORDINALITY AS u(ts, value, ord)
    ORDER BY u.ord;
END;

--
-- Downsampling: LTTB and min/max per bucket
--
-- Shared transition function: (state, ts, value, int parameter)
CREATE FUNCTION ts_points_int_trans(internal, timestamptz, float8, int4)
RETURNS internal
LANGUAGE C PARALLEL SAFE
AS 'MODULE_PATHNAME', 'ts_points_int_trans';

CREATE FUNCTION ts_lttb_final(internal)
RETURNS ts_point[]
LANGUAGE C PARALLEL SAFE
AS 'MODULE_PATHNAME', 'ts_lttb_final';

CREATE AGGREGATE ts_lttb(ts timestamptz, value float8, n int4) (
    SFUNC = ts_points_int_trans,
    STYPE = internal,
    FINALFUNC = ts_lttb_final
);

-- Array form, e.g. on the output of ts_decode_timestamps/ts_decode_floats
CREATE FUNCTION ts_lttb(ts timestamptz[], value float8[], n int4)
RETURNS ts_point[]
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
AS 'MODULE_PATHNAME', 'ts_lttb_arrays';

CREATE FUNCTION ts_minmax_final(internal)
RETURNS ts_point[]
LANGUAGE C PARALLEL SAFE
AS 'MODULE_PATHNAME', 'ts_minmax_final';

CREATE AGGREGATE ts_minmax_downsample(ts timestamptz, value float8, buckets int4) (
    SFUNC = ts_points_int_trans,
    STYPE = internal,
    FINALFUNC = ts_minmax_final
);

--
-- Irregular -> regular grid resampling.
--
-- Aggregates cannot have parameter defaults, so the optional parameters
-- are provided as overloads.  A NULL method means 'linear', a NULL origin
-- means "the first observation", a NULL max_gap means "no limit".  The
-- transition function is deliberately not STRICT.
--
CREATE FUNCTION ts_resample_trans(internal, timestamptz, float8, interval)
RETURNS internal LANGUAGE C PARALLEL SAFE
AS 'MODULE_PATHNAME', 'ts_resample_trans';
CREATE FUNCTION ts_resample_trans(internal, timestamptz, float8, interval, text)
RETURNS internal LANGUAGE C PARALLEL SAFE
AS 'MODULE_PATHNAME', 'ts_resample_trans';
CREATE FUNCTION ts_resample_trans(internal, timestamptz, float8, interval, text,
                                  timestamptz)
RETURNS internal LANGUAGE C PARALLEL SAFE
AS 'MODULE_PATHNAME', 'ts_resample_trans';
CREATE FUNCTION ts_resample_trans(internal, timestamptz, float8, interval, text,
                                  timestamptz, interval)
RETURNS internal LANGUAGE C PARALLEL SAFE
AS 'MODULE_PATHNAME', 'ts_resample_trans';

CREATE FUNCTION ts_resample_final(internal)
RETURNS ts_point[]
LANGUAGE C PARALLEL SAFE
AS 'MODULE_PATHNAME', 'ts_resample_final';

CREATE AGGREGATE ts_resample(ts timestamptz, value float8, step interval) (
    SFUNC = ts_resample_trans, STYPE = internal, FINALFUNC = ts_resample_final
);
CREATE AGGREGATE ts_resample(ts timestamptz, value float8, step interval,
                             method text) (
    SFUNC = ts_resample_trans, STYPE = internal, FINALFUNC = ts_resample_final
);
CREATE AGGREGATE ts_resample(ts timestamptz, value float8, step interval,
                             method text, origin timestamptz) (
    SFUNC = ts_resample_trans, STYPE = internal, FINALFUNC = ts_resample_final
);
CREATE AGGREGATE ts_resample(ts timestamptz, value float8, step interval,
                             method text, origin timestamptz,
                             max_gap interval) (
    SFUNC = ts_resample_trans, STYPE = internal, FINALFUNC = ts_resample_final
);

--
-- Out-of-order / duplicate cleanup
--
CREATE FUNCTION ts_sort_dedup(ts timestamptz[], value float8[],
                              keep text DEFAULT 'last')
RETURNS ts_point[]
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
AS 'MODULE_PATHNAME', 'ts_sort_dedup';

--
-- As-of joins
--
-- Whole-series alignment by merge (not STRICT: tolerance defaults to NULL).
CREATE FUNCTION ts_asof_join(left_ts timestamptz[], right_ts timestamptz[],
                             tolerance interval DEFAULT NULL,
                             direction text DEFAULT 'backward')
RETURNS TABLE(left_idx int4, right_idx int4)
LANGUAGE C IMMUTABLE PARALLEL SAFE
AS 'MODULE_PATHNAME', 'ts_asof_join';

-- Single-point lookup of the row of "tbl" as of "at", meant for use in a
-- LATERAL subquery or the select list.  Not STRICT (defaults are NULL).
CREATE FUNCTION ts_asof(tbl regclass, ts_column text, at timestamptz,
                        by_column text DEFAULT NULL,
                        by_value text DEFAULT NULL,
                        tolerance interval DEFAULT NULL,
                        direction text DEFAULT 'backward')
RETURNS jsonb
LANGUAGE plpgsql STABLE
AS $fn$
DECLARE
    dir      text := lower(coalesce(direction, 'backward'));
    by_type  text;
    by_sql   text := '';
    bwd_sql  text;
    fwd_sql  text;
    b_row    jsonb;
    f_row    jsonb;
    b_ts     timestamptz;
    f_ts     timestamptz;
BEGIN
    IF tbl IS NULL OR ts_column IS NULL THEN
        RAISE EXCEPTION 'ts_asof: tbl and ts_column must not be NULL';
    END IF;
    IF dir NOT IN ('backward', 'forward', 'nearest') THEN
        RAISE EXCEPTION 'ts_asof: direction must be backward, forward or nearest, not "%"', direction;
    END IF;
    IF tolerance IS NOT NULL AND tolerance < interval '0' THEN
        RAISE EXCEPTION 'ts_asof: tolerance must not be negative';
    END IF;
    PERFORM 1 FROM pg_catalog.pg_attribute
     WHERE attrelid = tbl AND attname = ts_column
       AND attnum > 0 AND NOT attisdropped;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'ts_asof: column "%" does not exist in %', ts_column, tbl;
    END IF;
    IF at IS NULL THEN
        RETURN NULL;
    END IF;

    IF by_column IS NOT NULL THEN
        SELECT pg_catalog.format_type(atttypid, atttypmod) INTO by_type
          FROM pg_catalog.pg_attribute
         WHERE attrelid = tbl AND attname = by_column
           AND attnum > 0 AND NOT attisdropped;
        IF by_type IS NULL THEN
            RAISE EXCEPTION 'ts_asof: column "%" does not exist in %', by_column, tbl;
        END IF;
        IF by_value IS NULL THEN
            RETURN NULL;        -- "=" never matches NULL
        END IF;
        -- typed comparison keeps an index on (by_column, ts_column) usable
        by_sql := format(' AND t.%I = $2::%s', by_column, by_type);
    END IF;

    IF dir IN ('backward', 'nearest') THEN
        bwd_sql := format('SELECT to_jsonb(t), t.%1$I::timestamptz FROM %2$s t'
                          ' WHERE t.%1$I <= $1%3$s%4$s'
                          ' ORDER BY t.%1$I DESC LIMIT 1',
                          ts_column, tbl, by_sql,
                          CASE WHEN tolerance IS NOT NULL
                               THEN format(' AND t.%I >= $1 - $3', ts_column)
                               ELSE '' END);
        EXECUTE bwd_sql INTO b_row, b_ts USING at, by_value, tolerance;
    END IF;
    IF dir IN ('forward', 'nearest') THEN
        fwd_sql := format('SELECT to_jsonb(t), t.%1$I::timestamptz FROM %2$s t'
                          ' WHERE t.%1$I >= $1%3$s%4$s'
                          ' ORDER BY t.%1$I ASC LIMIT 1',
                          ts_column, tbl, by_sql,
                          CASE WHEN tolerance IS NOT NULL
                               THEN format(' AND t.%I <= $1 + $3', ts_column)
                               ELSE '' END);
        EXECUTE fwd_sql INTO f_row, f_ts USING at, by_value, tolerance;
    END IF;

    IF dir = 'backward' THEN
        RETURN b_row;
    ELSIF dir = 'forward' THEN
        RETURN f_row;
    END IF;
    -- nearest: ties go to the earlier (backward) row
    IF b_row IS NULL THEN
        RETURN f_row;
    ELSIF f_row IS NULL THEN
        RETURN b_row;
    ELSIF (at - b_ts) <= (f_ts - at) THEN
        RETURN b_row;
    ELSE
        RETURN f_row;
    END IF;
END;
$fn$;

--
-- Value codecs (pure bytea <-> array functions; no storage management)
--
CREATE FUNCTION ts_encode_timestamps(timestamptz[])
RETURNS bytea
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
AS 'MODULE_PATHNAME', 'ts_encode_timestamps';

CREATE FUNCTION ts_decode_timestamps(bytea)
RETURNS timestamptz[]
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
AS 'MODULE_PATHNAME', 'ts_decode_timestamps';

CREATE FUNCTION ts_encode_floats(float8[])
RETURNS bytea
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
AS 'MODULE_PATHNAME', 'ts_encode_floats';

CREATE FUNCTION ts_decode_floats(bytea)
RETURNS float8[]
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
AS 'MODULE_PATHNAME', 'ts_decode_floats';

CREATE FUNCTION ts_codec_stats(data bytea,
                               OUT codec text, OUT version int4,
                               OUT count int4, OUT nulls int4,
                               OUT bytes int4, OUT bits_per_value float8)
RETURNS record
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
AS 'MODULE_PATHNAME', 'ts_codec_stats';
