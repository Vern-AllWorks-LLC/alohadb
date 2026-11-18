/* contrib/pg_compat19/pg_compat19--1.0.sql */

\echo Use "CREATE EXTENSION pg_compat19" to load this file. \quit

-- Backports of PostgreSQL 19 functions and casts that need no catalog,
-- grammar or on-disk-format changes.  Signatures and error behaviour follow
-- the 19 release.  DROP this extension before upgrading the cluster to a
-- PostgreSQL 19-based AlohaDB, where these objects are built in.

-- error_on_null(anyelement): return the argument, or fail on NULL
CREATE FUNCTION error_on_null(anyelement)
RETURNS anyelement
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
AS $$
BEGIN
    IF $1 IS NULL THEN
        RAISE EXCEPTION USING ERRCODE = 'null_value_not_allowed',
                              MESSAGE = 'null value not allowed';
    END IF;
    RETURN $1;
END;
$$;
COMMENT ON FUNCTION error_on_null(anyelement) IS 'return argument, or raise an error if it is NULL';

-- tid_block / tid_offset
CREATE FUNCTION tid_block(tid)
RETURNS int8
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
RETURN split_part(trim(both '()' from $1::text), ',', 1)::int8;
COMMENT ON FUNCTION tid_block(tid) IS 'extract block number from tid';

CREATE FUNCTION tid_offset(tid)
RETURNS int4
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
RETURN split_part(trim(both '()' from $1::text), ',', 2)::int4;
COMMENT ON FUNCTION tid_offset(tid) IS 'extract offset number from tid';

-- random(min, max) for date / timestamp / timestamptz (bounds inclusive).
-- Built on random(bigint, bigint), so setseed() applies.
CREATE FUNCTION random(min date, max date)
RETURNS date
LANGUAGE plpgsql VOLATILE STRICT PARALLEL RESTRICTED
AS $$
BEGIN
    IF min > max THEN
        RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
              MESSAGE = 'lower bound must be less than or equal to upper bound';
    END IF;
    IF NOT isfinite(min) OR NOT isfinite(max) THEN
        RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
              MESSAGE = 'lower and upper bounds must be finite';
    END IF;
    RETURN min + random(0::int8, (max - min)::int8)::int4;
END;
$$;
COMMENT ON FUNCTION random(date, date) IS 'random date in range';

CREATE FUNCTION random(min timestamp, max timestamp)
RETURNS timestamp
LANGUAGE plpgsql VOLATILE STRICT PARALLEL RESTRICTED
AS $$
BEGIN
    IF min > max THEN
        RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
              MESSAGE = 'lower bound must be less than or equal to upper bound';
    END IF;
    IF NOT isfinite(min) OR NOT isfinite(max) THEN
        RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
              MESSAGE = 'lower and upper bounds must be finite';
    END IF;
    -- interval * bigint goes through float8; clamp guards against rounding
    -- past max on spans wider than ~285 years
    RETURN least(max, min + random(0::int8,
                                   ((extract(epoch FROM max) - extract(epoch FROM min))
                                    * 1000000)::int8)
                            * interval '1 microsecond');
END;
$$;
COMMENT ON FUNCTION random(timestamp, timestamp) IS 'random timestamp in range';

CREATE FUNCTION random(min timestamptz, max timestamptz)
RETURNS timestamptz
LANGUAGE plpgsql VOLATILE STRICT PARALLEL RESTRICTED
AS $$
BEGIN
    IF min > max THEN
        RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
              MESSAGE = 'lower bound must be less than or equal to upper bound';
    END IF;
    IF NOT isfinite(min) OR NOT isfinite(max) THEN
        RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
              MESSAGE = 'lower and upper bounds must be finite';
    END IF;
    -- A pure-microseconds interval has no day/month part, so adding it is
    -- absolute-time arithmetic unaffected by time zones or DST.
    RETURN least(max, min + random(0::int8,
                                   ((extract(epoch FROM max) - extract(epoch FROM min))
                                    * 1000000)::int8)
                            * interval '1 microsecond');
END;
$$;
COMMENT ON FUNCTION random(timestamptz, timestamptz) IS 'random timestamptz in range';

-- Range subtraction returning a set (0, 1 or 2 ranges)
CREATE FUNCTION range_minus_multi(anyrange, anyrange)
RETURNS SETOF anyrange
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE ROWS 2
AS $$
    SELECT r FROM unnest(multirange($1) - multirange($2)) AS r
$$;
COMMENT ON FUNCTION range_minus_multi(anyrange, anyrange) IS 'remove portion from range';

CREATE FUNCTION multirange_minus_multi(anymultirange, anymultirange)
RETURNS SETOF anymultirange
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE ROWS 1
AS $$
    SELECT m FROM (SELECT $1 - $2) AS s(m) WHERE NOT isempty(m)
$$;
COMMENT ON FUNCTION multirange_minus_multi(anymultirange, anymultirange) IS 'remove portion from multirange';

-- Casts between uuid and bytea
CREATE FUNCTION uuid_bytea(uuid)
RETURNS bytea
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
RETURN decode(replace($1::text, '-', ''), 'hex');
COMMENT ON FUNCTION uuid_bytea(uuid) IS 'convert uuid to bytea';

CREATE FUNCTION bytea_uuid(bytea)
RETURNS uuid
LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
BEGIN
    IF length($1) <> 16 THEN
        RAISE EXCEPTION USING ERRCODE = 'invalid_binary_representation',
              MESSAGE = 'invalid input length for type uuid',
              DETAIL  = format('Expected %s bytes, got %s.', 16, length($1));
    END IF;
    RETURN encode($1, 'hex')::uuid;
END;
$$;
COMMENT ON FUNCTION bytea_uuid(bytea) IS 'convert bytea to uuid';

CREATE CAST (uuid AS bytea) WITH FUNCTION uuid_bytea(uuid);
CREATE CAST (bytea AS uuid) WITH FUNCTION bytea_uuid(bytea);
