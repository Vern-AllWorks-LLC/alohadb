/* contrib/alohadb_approx/alohadb_approx--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION alohadb_approx UPDATE TO '1.1'" to load this file. \quit

-- =====================================================================
-- Bloom Filter type and functions
-- =====================================================================

CREATE FUNCTION bloom_in(cstring) RETURNS bloom_filter
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION bloom_out(bloom_filter) RETURNS cstring
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE bloom_filter (
    INPUT = bloom_in,
    OUTPUT = bloom_out,
    STORAGE = extended
);

CREATE FUNCTION bloom_create(expected_items int, fpr float8 DEFAULT 0.01)
RETURNS bloom_filter
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION bloom_add(bf bloom_filter, item text)
RETURNS bloom_filter
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION bloom_contains(bf bloom_filter, item text)
RETURNS boolean
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION bloom_merge(bf1 bloom_filter, bf2 bloom_filter)
RETURNS bloom_filter
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION bloom_stats(bf bloom_filter)
RETURNS TABLE(bits int, hash_functions int, items_added int, est_fpr float8)
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION bloom_agg_transfn(internal, text, int, float8)
RETURNS internal
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE;

CREATE FUNCTION bloom_agg_finalfn(internal)
RETURNS bloom_filter
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE;

CREATE AGGREGATE bloom_agg(text, int, float8) (
    SFUNC = bloom_agg_transfn,
    STYPE = internal,
    FINALFUNC = bloom_agg_finalfn
);

-- =====================================================================
-- T-Digest type and functions
-- =====================================================================

CREATE FUNCTION tdigest_in(cstring) RETURNS tdigest
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tdigest_out(tdigest) RETURNS cstring
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE tdigest (
    INPUT = tdigest_in,
    OUTPUT = tdigest_out,
    STORAGE = extended
);

CREATE FUNCTION tdigest_create(compression float8 DEFAULT 100)
RETURNS tdigest
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tdigest_add(td tdigest, value float8)
RETURNS tdigest
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tdigest_quantile(td tdigest, quantile float8)
RETURNS float8
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tdigest_cdf(td tdigest, value float8)
RETURNS float8
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tdigest_merge(td1 tdigest, td2 tdigest)
RETURNS tdigest
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION approx_percentile_transfn(internal, float8, float8, float8)
RETURNS internal
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE;

CREATE FUNCTION approx_percentile_finalfn(internal)
RETURNS float8
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE;

CREATE AGGREGATE approx_percentile(float8, float8, float8) (
    SFUNC = approx_percentile_transfn,
    STYPE = internal,
    FINALFUNC = approx_percentile_finalfn
);
