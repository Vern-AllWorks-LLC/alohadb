/* contrib/alohadb_analytics/alohadb_analytics--1.0.sql */

\echo Use "CREATE EXTENSION alohadb_analytics" to load this file.\quit

-- Metadata table for continuous aggregates
CREATE TABLE alohadb_analytics_cont_aggs (
    name text PRIMARY KEY,
    source_table text NOT NULL,
    agg_query text NOT NULL,
    watermark_col text NOT NULL,
    refresh_interval interval NOT NULL DEFAULT '1 hour',
    last_refresh timestamptz,
    last_watermark text
);

-- Continuous aggregate management
CREATE FUNCTION analytics_create_cont_agg(
    name text,
    source_table text,
    agg_query text,
    watermark_col text,
    refresh_interval text DEFAULT '1 hour'
)
RETURNS void
LANGUAGE C
AS 'MODULE_PATHNAME', 'analytics_create_cont_agg';

CREATE FUNCTION analytics_drop_cont_agg(name text)
RETURNS void
LANGUAGE C
AS 'MODULE_PATHNAME', 'analytics_drop_cont_agg';

CREATE FUNCTION analytics_refresh_cont_agg(name text)
RETURNS void
LANGUAGE C
AS 'MODULE_PATHNAME', 'analytics_refresh_cont_agg';

CREATE FUNCTION analytics_cont_agg_status()
RETURNS TABLE(
    name text,
    source_table text,
    watermark_col text,
    refresh_interval text,
    last_refresh text,
    last_watermark text
)
LANGUAGE C
AS 'MODULE_PATHNAME', 'analytics_cont_agg_status';

-- Projections (trigger-based auto-refresh MVs)
CREATE FUNCTION analytics_create_projection(
    name text,
    source_table text,
    agg_query text
)
RETURNS void
LANGUAGE C
AS 'MODULE_PATHNAME', 'analytics_create_projection';

CREATE FUNCTION analytics_drop_projection(name text)
RETURNS void
LANGUAGE C
AS 'MODULE_PATHNAME', 'analytics_drop_projection';

-- Window functions
CREATE FUNCTION analytics_interpolate(
    time_col timestamptz,
    value float8,
    method text DEFAULT 'linear'
)
RETURNS float8
LANGUAGE C IMMUTABLE
WINDOW
AS 'MODULE_PATHNAME', 'analytics_interpolate';

CREATE FUNCTION analytics_moving_avg(value float8, window_size int)
RETURNS float8
LANGUAGE C IMMUTABLE
WINDOW
AS 'MODULE_PATHNAME', 'analytics_moving_avg';

CREATE FUNCTION analytics_moving_sum(value float8, window_size int)
RETURNS float8
LANGUAGE C IMMUTABLE
WINDOW
AS 'MODULE_PATHNAME', 'analytics_moving_sum';

-- Gap fill SRF
CREATE FUNCTION analytics_gap_fill(
    time_col timestamptz,
    bucket_width interval,
    range_start timestamptz,
    range_end timestamptz
)
RETURNS SETOF timestamptz
LANGUAGE C STABLE
AS 'MODULE_PATHNAME', 'analytics_gap_fill';

-- First/Last aggregates (value float8, time timestamptz)
CREATE FUNCTION analytics_first_transfn(internal, float8, timestamptz)
RETURNS internal
LANGUAGE C
AS 'MODULE_PATHNAME', 'analytics_first_transfn';

CREATE FUNCTION analytics_first_finalfn(internal)
RETURNS float8
LANGUAGE C
AS 'MODULE_PATHNAME', 'analytics_first_finalfn';

CREATE AGGREGATE analytics_first(float8, timestamptz) (
    SFUNC = analytics_first_transfn,
    STYPE = internal,
    FINALFUNC = analytics_first_finalfn
);

CREATE FUNCTION analytics_last_transfn(internal, float8, timestamptz)
RETURNS internal
LANGUAGE C
AS 'MODULE_PATHNAME', 'analytics_last_transfn';

CREATE FUNCTION analytics_last_finalfn(internal)
RETURNS float8
LANGUAGE C
AS 'MODULE_PATHNAME', 'analytics_last_finalfn';

CREATE AGGREGATE analytics_last(float8, timestamptz) (
    SFUNC = analytics_last_transfn,
    STYPE = internal,
    FINALFUNC = analytics_last_finalfn
);

-- Delta and rate window functions
CREATE FUNCTION analytics_delta(value float8)
RETURNS float8
LANGUAGE C IMMUTABLE
WINDOW
AS 'MODULE_PATHNAME', 'analytics_delta';

CREATE FUNCTION analytics_rate(value float8, time_col timestamptz)
RETURNS float8
LANGUAGE C IMMUTABLE
WINDOW
AS 'MODULE_PATHNAME', 'analytics_rate';

-- Comments
COMMENT ON FUNCTION analytics_create_cont_agg(text, text, text, text, text) IS
    'Create a continuous aggregate (materialized view with metadata)';
COMMENT ON FUNCTION analytics_interpolate(timestamptz, float8, text) IS
    'Window function: linear interpolation between non-NULL values';
COMMENT ON FUNCTION analytics_moving_avg(float8, int) IS
    'Window function: moving average over last N rows';
COMMENT ON FUNCTION analytics_gap_fill(timestamptz, interval, timestamptz, timestamptz) IS
    'Generate time buckets filling gaps in a time range';
