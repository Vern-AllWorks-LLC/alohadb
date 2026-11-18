SET timezone = 'UTC';

-- irregular samples; note the 35 minute gap between 00:25 and 01:00
CREATE TEMP TABLE obs (sensor int, ts timestamptz, v float8);
INSERT INTO obs VALUES
  (1, '2026-03-01 01:00', 0),
  (1, '2026-03-01 00:10', 10),
  (1, '2026-03-01 00:00', 0),
  (1, '2026-03-01 00:25', 40),
  (1, '2026-03-01 00:40', NULL),     -- NULL values are ignored
  (2, '2026-03-01 00:00', 100),
  (2, '2026-03-01 00:20', 200),
  (2, '2026-03-01 00:20', 300);      -- duplicate timestamp: last row wins

-- linear: 0, 10, 30, 34.29, 22.86, 11.43, 0
SELECT * FROM unnest((SELECT ts_resample(ts, v, '10 minutes') FROM obs WHERE sensor = 1));
-- linear with max_gap: no interpolation across the 35 minute gap
SELECT * FROM unnest((SELECT ts_resample(ts, v, '10 minutes', 'linear', NULL, '20 minutes')
                      FROM obs WHERE sensor = 1));
-- locf: 0, 10, 10, 40, 40, 40, 0
SELECT * FROM unnest((SELECT ts_resample(ts, v, '10 minutes', 'locf') FROM obs WHERE sensor = 1));
-- locf, values older than 20 minutes are not carried
SELECT * FROM unnest((SELECT ts_resample(ts, v, '10 minutes', 'locf', NULL, '20 minutes')
                      FROM obs WHERE sensor = 1));
-- nearest (ties go to the earlier sample): 0, 10, 40, 40, 40, 0, 0
SELECT * FROM unnest((SELECT ts_resample(ts, v, '10 minutes', 'nearest') FROM obs WHERE sensor = 1));
SELECT * FROM unnest((SELECT ts_resample(ts, v, '10 minutes', 'nearest', NULL, '8 minutes')
                      FROM obs WHERE sensor = 1));
-- mean per 30 minute bucket: (0+10+40)/3, empty -> NULL, 0
SELECT * FROM unnest((SELECT ts_resample(ts, v, '30 minutes', 'mean') FROM obs WHERE sensor = 1));
-- explicit origin: grid at :05, :15, ... inside [00:00, 01:00]
SELECT * FROM unnest((SELECT ts_resample(ts, v, '10 minutes', 'linear', '2020-01-01 00:05')
                      FROM obs WHERE sensor = 1));
-- mean with an origin: buckets start on the origin grid (floor of first sample)
SELECT * FROM unnest((SELECT ts_resample(ts, v, '20 minutes', 'mean', '2026-03-01 00:05')
                      FROM obs WHERE sensor = 1));
-- NULL method / origin / max_gap mean the defaults
SELECT (SELECT ts_resample(ts, v, '10 minutes', NULL, NULL, NULL) FROM obs WHERE sensor = 1) =
       (SELECT ts_resample(ts, v, '10 minutes') FROM obs WHERE sensor = 1) AS same;
-- per sensor; duplicate timestamps keep the last row for interpolation,
-- and all rows count for 'mean'
SELECT sensor, ts_resample(ts, v, '10 minutes') FROM obs GROUP BY sensor ORDER BY sensor;
SELECT sensor, ts_resample(ts, v, '10 minutes', 'mean') FROM obs GROUP BY sensor ORDER BY sensor;
-- single sample, and a span shorter than one step on an origin grid
SELECT ts_resample(ts, v, '1 hour') FROM obs WHERE sensor = 1 AND v = 40;
SELECT ts_resample(ts, v, '1 hour', 'linear', '2026-03-01 00:30') FROM obs WHERE sensor = 2;
-- no rows -> NULL
SELECT ts_resample(ts, v, '1 hour') IS NULL FROM obs WHERE false;
-- errors
SELECT ts_resample(ts, v, '1 month') FROM obs;
SELECT ts_resample(ts, v, '0 seconds') FROM obs;
SELECT ts_resample(ts, v, '-1 second') FROM obs;
SELECT ts_resample(ts, v, NULL) FROM obs;
SELECT ts_resample(ts, v, '1 minute', 'cubic') FROM obs;
SELECT ts_resample(ts, v, '1 minute', 'linear', NULL, '-1 minute') FROM obs;
SELECT ts_resample(ts, v, '1 minute') FROM (VALUES ('infinity'::timestamptz, 1::float8)) x(ts, v);
SELECT ts_resample(ts, v, '1 microsecond') FROM (VALUES ('2000-01-01'::timestamptz, 1::float8),
                                                       ('2026-01-01', 2)) x(ts, v);

-- larger irregular series: grid length and exactness at the samples
WITH d AS (
  SELECT '2026-01-01'::timestamptz + make_interval(secs => g * 10 + (g % 3)) AS ts,
         g::float8 AS v
  FROM generate_series(0, 999) g
), r AS (SELECT ts_resample(ts, v, '1 minute') AS a FROM d)
SELECT cardinality(a), (a[1]).value, (a[cardinality(a)]).ts FROM r;
