-- pg_compat19 regression test (expected results match PostgreSQL 19)

CREATE EXTENSION pg_compat19;

-- error_on_null
SELECT error_on_null(1), error_on_null('x'::text), error_on_null(ARRAY[1, NULL]);
SELECT error_on_null(NULL::int);

-- tid_block / tid_offset
SELECT tid_block('(42,7)'::tid), tid_offset('(42,7)'::tid);
SELECT tid_block('(4294967295,65535)'::tid), tid_offset('(4294967295,65535)'::tid);
CREATE TEMP TABLE t AS SELECT g FROM generate_series(1, 3) g;
SELECT g, tid_block(ctid), tid_offset(ctid) FROM t ORDER BY g;

-- random(min, max) for dates and timestamps
SELECT setseed(0.5);
SELECT bool_and(d BETWEEN '2026-01-01' AND '2026-01-10') AS in_range,
       count(DISTINCT d) > 5 AS spread
FROM   (SELECT random('2026-01-01'::date, '2026-01-10'::date) AS d
        FROM generate_series(1, 200)) s;
SELECT random('2026-03-03'::date, '2026-03-03'::date);
SELECT bool_and(ts BETWEEN '2026-01-01 00:00' AND '2026-01-01 00:00:01') AS in_range
FROM   (SELECT random('2026-01-01 00:00'::timestamp, '2026-01-01 00:00:01'::timestamp) AS ts
        FROM generate_series(1, 200)) s;
SET timezone = 'America/New_York';
SELECT bool_and(ts BETWEEN '2026-03-08 01:59:59-05' AND '2026-03-08 03:00:01-04') AS dst_in_range
FROM   (SELECT random('2026-03-08 01:59:59-05'::timestamptz, '2026-03-08 03:00:01-04'::timestamptz) AS ts
        FROM generate_series(1, 200)) s;
RESET timezone;
SELECT random('0001-01-01'::timestamp, '9999-12-31'::timestamp) <= '9999-12-31'::timestamp AS wide_ok;
-- reproducible under setseed()
SELECT setseed(0.25);
SELECT random('2000-01-01'::date, '2099-12-31'::date) AS a \gset
SELECT setseed(0.25);
SELECT random('2000-01-01'::date, '2099-12-31'::date) = :'a' AS same_seed_same_value;
SELECT random('2026-01-02'::date, '2026-01-01'::date);
SELECT random('2026-01-01'::date, 'infinity'::date);
SELECT random('-infinity'::timestamptz, now());

-- range_minus_multi / multirange_minus_multi
SELECT * FROM range_minus_multi('[1,10)'::int4range, '[3,5)'::int4range);
SELECT * FROM range_minus_multi('[1,10)'::int4range, '[5,20)'::int4range);
SELECT * FROM range_minus_multi('[1,10)'::int4range, '[0,20)'::int4range);
SELECT * FROM range_minus_multi('[1,10)'::int4range, 'empty'::int4range);
SELECT * FROM range_minus_multi('empty'::int4range, '[1,2)'::int4range);
SELECT * FROM range_minus_multi('[2026-01-01,2026-12-31]'::daterange, '[2026-06-01,2026-06-30]'::daterange);
SELECT * FROM range_minus_multi('[1.5,9.5]'::numrange, '(3,4)'::numrange);
SELECT * FROM multirange_minus_multi('{[1,10)}'::int4multirange, '{[3,5)}'::int4multirange);
SELECT count(*) FROM multirange_minus_multi('{[1,10)}'::int4multirange, '{[0,20)}'::int4multirange);

-- uuid <-> bytea casts
SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid::bytea;
SELECT '\xa0eebc999c0b4ef8bb6d6bb9bd380a11'::bytea::uuid;
SELECT gen_random_uuid() AS u \gset
SELECT :'u'::uuid::bytea::uuid = :'u'::uuid AS round_trip;
SELECT '\x0102'::bytea::uuid;

DROP EXTENSION pg_compat19;
