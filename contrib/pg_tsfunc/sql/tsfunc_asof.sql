SET timezone = 'UTC';

-- whole-series alignment: left (trades) vs right (quotes), 1-based indexes
-- right sorted: 00:01(#5) 00:04(#1) 00:10(#2) 00:10(#3) 00:30(#4)
SELECT * FROM ts_asof_join(
  ARRAY['2026-01-01 00:00', '2026-01-01 00:05', '2026-01-01 00:10',
        '2026-01-01 00:20', NULL, '2026-01-01 00:31']::timestamptz[],
  ARRAY['2026-01-01 00:04', '2026-01-01 00:10', '2026-01-01 00:10',
        '2026-01-01 00:30', '2026-01-01 00:01']::timestamptz[]);
SELECT * FROM ts_asof_join(
  ARRAY['2026-01-01 00:00', '2026-01-01 00:05', '2026-01-01 00:10',
        '2026-01-01 00:20', NULL, '2026-01-01 00:31']::timestamptz[],
  ARRAY['2026-01-01 00:04', '2026-01-01 00:10', '2026-01-01 00:10',
        '2026-01-01 00:30', '2026-01-01 00:01']::timestamptz[],
  direction => 'forward');
SELECT * FROM ts_asof_join(
  ARRAY['2026-01-01 00:00', '2026-01-01 00:05', '2026-01-01 00:10',
        '2026-01-01 00:20', NULL, '2026-01-01 00:31']::timestamptz[],
  ARRAY['2026-01-01 00:04', '2026-01-01 00:10', '2026-01-01 00:10',
        '2026-01-01 00:30', '2026-01-01 00:01']::timestamptz[],
  direction => 'nearest');
-- with a tolerance of 2 minutes
SELECT * FROM ts_asof_join(
  ARRAY['2026-01-01 00:00', '2026-01-01 00:05', '2026-01-01 00:10',
        '2026-01-01 00:20', NULL, '2026-01-01 00:31']::timestamptz[],
  ARRAY['2026-01-01 00:04', '2026-01-01 00:10', '2026-01-01 00:10',
        '2026-01-01 00:30', '2026-01-01 00:01']::timestamptz[],
  '2 minutes', 'backward');
SELECT * FROM ts_asof_join(
  ARRAY['2026-01-01 00:00', '2026-01-01 00:05', '2026-01-01 00:10',
        '2026-01-01 00:20', NULL, '2026-01-01 00:31']::timestamptz[],
  ARRAY['2026-01-01 00:04', '2026-01-01 00:10', '2026-01-01 00:10',
        '2026-01-01 00:30', '2026-01-01 00:01']::timestamptz[],
  '2 minutes', 'nearest');
-- edge cases
SELECT * FROM ts_asof_join('{}', ARRAY[now()]);
SELECT * FROM ts_asof_join(ARRAY['2026-01-01']::timestamptz[], '{}');
SELECT * FROM ts_asof_join(NULL, ARRAY[now()]);
SELECT * FROM ts_asof_join(ARRAY['-infinity', 'infinity']::timestamptz[],
                           ARRAY['infinity', '-infinity']::timestamptz[], direction => 'nearest');
SELECT * FROM ts_asof_join(ARRAY[now()], ARRAY[now()], direction => 'sideways');
SELECT * FROM ts_asof_join(ARRAY[now()], ARRAY[now()], '-1 second');
SELECT * FROM ts_asof_join(ARRAY[now()], ARRAY[now()], '1 mon');

-- joining back to the data, using the 1-based indexes
WITH l AS (SELECT array_agg(ts ORDER BY ts) AS t FROM generate_series(
             '2026-01-01 00:00'::timestamptz, '2026-01-01 00:06', '90 seconds') ts),
     r AS (SELECT ARRAY['2026-01-01 00:01', '2026-01-01 00:03', '2026-01-01 00:05']::timestamptz[] AS t,
                  ARRAY[1.5, 3.5, 5.5]::float8[] AS v)
SELECT l.t[j.left_idx] AS left_ts, r.t[j.right_idx] AS right_ts, r.v[j.right_idx] AS v
FROM l, r, ts_asof_join(l.t, r.t) j ORDER BY j.left_idx;

-- larger merge agrees with a brute-force reference
WITH l AS (SELECT array_agg('2026-01-01'::timestamptz + make_interval(secs => (g * 7919) % 1000)) AS t
           FROM generate_series(1, 300) g),
     r AS (SELECT array_agg('2026-01-01'::timestamptz + make_interval(secs => (g * 104729) % 1000)) AS t
           FROM generate_series(1, 200) g),
     j AS (SELECT j.* FROM l, r, ts_asof_join(l.t, r.t, '20 seconds') j),
     ref AS (SELECT li AS left_idx,
                    (SELECT ri FROM generate_subscripts(r.t, 1) ri
                      WHERE r.t[ri] <= l.t[li] AND r.t[ri] >= l.t[li] - interval '20 seconds'
                      ORDER BY r.t[ri] DESC, ri DESC LIMIT 1) AS right_idx
             FROM l, r, generate_subscripts(l.t, 1) li)
SELECT count(*) AS n, count(*) FILTER (WHERE j.right_idx IS NOT DISTINCT FROM ref.right_idx) AS agree,
       count(j.right_idx) AS matched
FROM j JOIN ref USING (left_idx);

-- row lookup for LATERAL joins
CREATE TEMP TABLE quotes (sym text, ts timestamptz, bid numeric);
CREATE INDEX ON quotes (sym, ts);
INSERT INTO quotes VALUES
  ('A', '2026-01-01 09:00', 10.0), ('A', '2026-01-01 09:05', 10.5),
  ('A', '2026-01-01 09:10', 11.0), ('B', '2026-01-01 09:02', 50.0),
  ('B', '2026-01-01 09:20', 51.0);
CREATE TEMP TABLE trades (id int, sym text, ts timestamptz);
INSERT INTO trades VALUES
  (1, 'A', '2026-01-01 08:59'), (2, 'A', '2026-01-01 09:06'),
  (3, 'A', '2026-01-01 09:30'), (4, 'B', '2026-01-01 09:03'),
  (5, 'B', '2026-01-01 09:12'), (6, 'C', '2026-01-01 09:00');

SELECT t.id, q->>'bid' AS bid, q->>'ts' AS quote_ts
FROM trades t
LEFT JOIN LATERAL ts_asof('quotes', 'ts', t.ts, 'sym', t.sym) q ON true
ORDER BY t.id;
SELECT t.id,
       ts_asof('quotes', 'ts', t.ts, 'sym', t.sym, direction => 'forward')->>'bid' AS fwd,
       ts_asof('quotes', 'ts', t.ts, 'sym', t.sym, direction => 'nearest')->>'bid' AS nearest,
       ts_asof('quotes', 'ts', t.ts, 'sym', t.sym, '5 minutes')->>'bid' AS bwd_5min,
       ts_asof('quotes', 'ts', t.ts, 'sym', t.sym, '5 minutes', 'nearest')->>'bid' AS near_5min
FROM trades t ORDER BY t.id;
-- without by_column: latest quote of any symbol
SELECT ts_asof('quotes', 'ts', '2026-01-01 09:15')->>'sym';
SELECT ts_asof('quotes', 'ts', NULL) IS NULL, ts_asof('quotes', 'ts', now(), 'sym', NULL) IS NULL;
-- equivalent plain-SQL pattern (uses the (sym, ts) index directly)
SELECT t.id, q.bid
FROM trades t
LEFT JOIN LATERAL (SELECT bid FROM quotes q
                   WHERE q.sym = t.sym AND q.ts <= t.ts
                   ORDER BY q.ts DESC LIMIT 1) q ON true
ORDER BY t.id;
-- errors
SELECT ts_asof('quotes', 'nope', now());
SELECT ts_asof('quotes', 'ts', now(), 'nope', 'A');
SELECT ts_asof('quotes', 'ts', now(), direction => 'up');
SELECT ts_asof('quotes', 'ts', now(), tolerance => '-1 minute');
