/* contrib/alohadb_approx/alohadb_approx--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_approx" to load this file. \quit

-- =====================================================================
-- HyperLogLog approximate distinct counting
-- =====================================================================

CREATE FUNCTION approx_count_distinct_transfn(internal, anyelement)
RETURNS internal
AS 'MODULE_PATHNAME', 'approx_count_distinct_transfn'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION approx_count_distinct_finalfn(internal)
RETURNS int8
AS 'MODULE_PATHNAME', 'approx_count_distinct_finalfn'
LANGUAGE C IMMUTABLE;

CREATE AGGREGATE approx_count_distinct(anyelement) (
    SFUNC = approx_count_distinct_transfn,
    STYPE = internal,
    FINALFUNC = approx_count_distinct_finalfn
);

-- =====================================================================
-- Count-Min Sketch type and functions
-- =====================================================================

CREATE FUNCTION cms_in(cstring) RETURNS cms
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION cms_out(cms) RETURNS cstring
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE cms (
    INPUT = cms_in,
    OUTPUT = cms_out,
    STORAGE = extended
);

CREATE FUNCTION cms_create(int, int) RETURNS cms
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION cms_add(cms, text) RETURNS cms
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION cms_estimate(cms, text) RETURNS int8
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION cms_merge(cms, cms) RETURNS cms
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

-- Space-Saving top-K aggregate (disabled pending FINALFUNC_EXTRA fix)
