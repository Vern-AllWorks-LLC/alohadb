CREATE EXTENSION alohadb_tsfunc;
SET timezone = 'UTC';

-- 10-point series, one point per minute.  Hand-computed LTTB to 4 points
-- (bucket size (10-2)/(4-2) = 4, x in minutes, areas scaled by 2):
--  bucket 1 = points 1..4, next-bucket average = (6.5, 5), a = (0,0):
--    area ~ |5x - 6.5y|: p1 1.5, p2 22.5, p3 2, p4 0.5        -> p2 (2,5)
--  bucket 2 = points 5..8, next-bucket average = (9, 0), a = (2,5):
--    area ~ |45 - 5x - 7y|: p5 43, p6 13, p7 3, p8 37         -> p5 (5,9)
--  result: p0 (0,0), p2 (2,5), p5 (5,9), p9 (9,0)
CREATE TEMP TABLE s10 (ts timestamptz, v float8);
INSERT INTO s10
SELECT '2026-01-01 00:00:00+00'::timestamptz + make_interval(mins => i), v
FROM unnest(ARRAY[0,1,5,2,3,9,4,1,6,0]::float8[]) WITH ORDINALITY AS u(v, o),
     LATERAL (SELECT (o - 1)::int AS i) x;

-- input deliberately fed in a scrambled order: the aggregate sorts
SELECT ts_lttb(ts, v, 4) FROM (SELECT * FROM s10 ORDER BY v DESC, ts) s;
SELECT * FROM ts_points((SELECT ts_lttb(ts, v, 4) FROM s10));
SELECT p.* FROM unnest((SELECT ts_lttb(ts, v, 3) FROM s10)) p;

-- n >= number of points returns everything (sorted)
SELECT cardinality(ts_lttb(ts, v, 10)), cardinality(ts_lttb(ts, v, 50)) FROM s10;
-- NULL rows are ignored
SELECT ts_lttb(ts, v, 5) FROM (SELECT ts, v FROM s10 UNION ALL SELECT NULL, 1 UNION ALL SELECT now(), NULL) s;
-- no rows -> NULL
SELECT ts_lttb(ts, v, 4) IS NULL FROM s10 WHERE false;
-- array form gives the same answer
SELECT ts_lttb(array_agg(ts), array_agg(v), 4) = (SELECT ts_lttb(ts, v, 4) FROM s10) FROM s10;
-- errors
SELECT ts_lttb(ts, v, 2) FROM s10;
SELECT ts_lttb(ts, v, NULL) FROM s10;
SELECT ts_lttb(ARRAY[now()], ARRAY[1.0, 2.0]::float8[], 3);

-- larger series: endpoints kept, output sorted, size == n
WITH d AS (
  SELECT '2026-01-01'::timestamptz + make_interval(secs => g) AS ts,
         sin(g / 50.0) * 100 + (g % 7) AS v
  FROM generate_series(0, 9999) g
), r AS (SELECT ts_lttb(ts, v, 100) AS a FROM d)
SELECT cardinality(a),
       (a[1]).ts = '2026-01-01'::timestamptz AS first_kept,
       (a[100]).ts = '2026-01-01'::timestamptz + interval '9999 seconds' AS last_kept,
       (SELECT bool_and(ts > lag_ts) FROM (SELECT ts, lag(ts) OVER (ORDER BY o) AS lag_ts
          FROM unnest(a) WITH ORDINALITY u(ts, value, o)) z WHERE lag_ts IS NOT NULL) AS sorted
FROM r;

-- min/max downsampling, 3 equal-width time buckets over minutes 0..9:
-- [0,3) -> (0,0) (2,5); [3,6) -> (3,2) (5,9); [6,9] -> (8,6) (9,0)
SELECT * FROM unnest((SELECT ts_minmax_downsample(ts, v, 3) FROM s10));
SELECT ts_minmax_downsample(ts, v, 1) FROM s10;
-- a single point
SELECT ts_minmax_downsample(ts, v, 4) FROM s10 WHERE v = 9;
SELECT ts_minmax_downsample(ts, v, 0) FROM s10;

-- per-group usage
SELECT g, cardinality(ts_lttb(ts, v, 3))
FROM (SELECT ts, v, (v::int % 2) AS g FROM s10) s GROUP BY g ORDER BY g;

-- sort + dedup of out-of-order data
SELECT * FROM unnest(ts_sort_dedup(
  ARRAY['2026-01-01 00:02', '2026-01-01 00:00', '2026-01-01 00:01',
        '2026-01-01 00:00', NULL, '2026-01-01 00:02']::timestamptz[],
  ARRAY[3, 1, 2, 10, 99, NULL]::float8[]));
SELECT * FROM unnest(ts_sort_dedup(
  ARRAY['2026-01-01 00:02', '2026-01-01 00:00', '2026-01-01 00:01',
        '2026-01-01 00:00', NULL, '2026-01-01 00:02']::timestamptz[],
  ARRAY[3, 1, 2, 10, 99, NULL]::float8[], 'first'));
SELECT k, ts_sort_dedup(
  ARRAY['2026-01-01 00:00', '2026-01-01 00:00', '2026-01-01 00:00', '2026-01-01 00:01']::timestamptz[],
  ARRAY[4, 1, NULL, NULL]::float8[], k)
FROM unnest(ARRAY['min', 'max', 'avg']) k;
SELECT ts_sort_dedup('{}'::timestamptz[], '{}'::float8[]);
SELECT ts_sort_dedup(ARRAY[now()], ARRAY[1]::float8[], 'median');
