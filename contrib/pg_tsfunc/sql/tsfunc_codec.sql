SET timezone = 'UTC';

-- empty, single, NULL elements
SELECT ts_encode_timestamps('{}'), ts_decode_timestamps(ts_encode_timestamps('{}'));
SELECT ts_encode_floats('{}'), ts_decode_floats(ts_encode_floats('{}'));
SELECT ts_encode_timestamps(ARRAY['2026-01-01 00:00:00+00']::timestamptz[]);
SELECT ts_decode_timestamps(ts_encode_timestamps(ARRAY['2026-01-01 00:00:00+00']::timestamptz[]));
SELECT ts_encode_floats(ARRAY[1.5]::float8[]);
SELECT ts_decode_floats(ts_encode_floats(ARRAY[1.5]::float8[]));
SELECT ts_decode_timestamps(ts_encode_timestamps(
  ARRAY[NULL, '2026-01-01', NULL, '2026-01-02', '2026-01-03', NULL]::timestamptz[]));
SELECT ts_decode_floats(ts_encode_floats(ARRAY[NULL, 1, NULL, 2, 2, NULL]::float8[]));
SELECT ts_decode_floats(ts_encode_floats(ARRAY[NULL, NULL]::float8[]));
SELECT * FROM ts_codec_stats(ts_encode_floats(ARRAY[NULL, 1, NULL, 2, 2, NULL]::float8[]));
SELECT * FROM ts_codec_stats(ts_encode_floats('{}'));

-- special floats: bit-exact round trip (compare the binary send format,
-- which distinguishes -0.0 and preserves NaN)
WITH v AS (
  SELECT ARRAY['NaN', 'Infinity', '-Infinity', '0', '-0', '4.9e-324', '-4.9e-324',
               '2.2250738585072009e-308', '2.2250738585072014e-308',
               '1.7976931348623157e308', '-1.7976931348623157e308',
               '1', '1', '-0', 'NaN', '0.1', '0.30000000000000004', '3.141592653589793']::float8[] AS a
)
SELECT array_length(d, 1) AS n,
       (SELECT bool_and(float8send(x) = float8send(y))
          FROM unnest(a, d) u(x, y)) AS bit_exact,
       d
FROM v, LATERAL (SELECT ts_decode_floats(ts_encode_floats(a)) AS d) z;
-- -0.0 keeps its sign
SELECT float8send(x) FROM unnest(ts_decode_floats(ts_encode_floats(ARRAY['-0', '0']::float8[]))) x;
-- infinite and extreme timestamps
SELECT ts_decode_timestamps(ts_encode_timestamps(
  ARRAY['infinity', '-infinity', '4714-11-24 00:00:00+00 BC', '294276-12-31 23:59:59.999999+00',
        'infinity', '2026-01-01 12:34:56.789012+00', '-infinity']::timestamptz[]));

-- deterministic pseudo-random data
WITH d AS (
  SELECT array_agg(sin(g * 1.3) * 1000 + cos(g / 7.0) ORDER BY g) AS f,
         array_agg('2026-01-01'::timestamptz
                   + make_interval(secs => g * 10 + (sin(g)::numeric * 3)::float8) ORDER BY g) AS t
  FROM generate_series(1, 5000) g
)
SELECT ts_decode_floats(ts_encode_floats(f)) = f AS floats_ok,
       ts_decode_timestamps(ts_encode_timestamps(t)) = t AS ts_ok,
       (SELECT bool_and(float8send(x) = float8send(y))
          FROM unnest(f, ts_decode_floats(ts_encode_floats(f))) u(x, y)) AS floats_bit_exact
FROM d;
-- unsorted timestamps and large jumps round-trip too
WITH d AS (
  SELECT array_agg('2000-01-01'::timestamptz
                   + make_interval(secs => ((g * 2654435761) % 1000000007)::float8 * 1000) ORDER BY g) AS t
  FROM generate_series(1, 2000) g
)
SELECT ts_decode_timestamps(ts_encode_timestamps(t)) = t FROM d;
-- slowly changing float series compress well; constant series even better
SELECT round((s).bits_per_value::numeric, 2) AS bits_per_value, (s).count
FROM (SELECT ts_codec_stats(ts_encode_floats(array_agg(20 + (g / 100)::int * 0.5))) s
      FROM generate_series(1, 10000) g) z;
SELECT (s).bytes, round((s).bits_per_value::numeric, 3) AS bits_per_value
FROM (SELECT ts_codec_stats(ts_encode_floats(array_fill(42.0::float8, ARRAY[10000]))) s) z;

-- regular timestamps: about 1 bit per value
SELECT (s).codec, (s).count, (s).bytes, round((s).bits_per_value::numeric, 3) AS bits_per_value
FROM (SELECT ts_codec_stats(ts_encode_timestamps(array_agg(ts))) s
      FROM generate_series('2026-01-01'::timestamptz, '2026-01-02', '1 second') ts) z;
-- slightly jittered timestamps (+-1 ms) stay small too
SELECT round((s).bits_per_value::numeric, 1) < 16 AS small
FROM (SELECT ts_codec_stats(ts_encode_timestamps(array_agg(
        '2026-01-01'::timestamptz + make_interval(secs => g + (g % 3 - 1) / 1000.0)))) s
      FROM generate_series(1, 10000) g) z;

-- multidimensional arrays are rejected
SELECT ts_encode_floats('{{1,2},{3,4}}');
-- type confusion is rejected
SELECT ts_decode_floats(ts_encode_timestamps(ARRAY[now()]));
SELECT ts_decode_timestamps(ts_encode_floats(ARRAY[1.0]::float8[]));

-- corrupted input is rejected
SELECT ts_decode_floats('\x');
SELECT ts_decode_floats('\x4601000000');
SELECT ts_decode_floats('\x5801000000000000');                -- bad codec id
SELECT ts_decode_floats('\x4602000000000000');                -- bad version
SELECT ts_decode_floats('\x4601020000000000');                -- unknown flag
SELECT ts_decode_floats('\x460100ffffffff');                  -- count too large
SELECT ts_decode_floats('\x460100ffffff00');                  -- count 16M, no data
SELECT ts_decode_floats('\x46010001000000');                  -- 1 value, no data
SELECT ts_decode_floats('\x4601000100000000');                -- truncated value
SELECT ts_decode_floats('\x460100000000003ff0');              -- empty with trailing bytes
SELECT ts_decode_floats('\x460101020000000500');              -- bitmap: unused bits set
SELECT ts_decode_floats('\x46010102000000');                  -- truncated bitmap
SELECT ts_decode_floats('\x46010100000000');                  -- bitmap on empty series
-- valid two-value stream, then truncated, extended and with bad padding
SELECT ts_encode_floats(ARRAY[1.0, 1.0]::float8[]);
SELECT ts_decode_floats('\x46010002000000' || '\x3ff000000000000000'::bytea);
SELECT ts_decode_floats('\x46010002000000' || '\x3ff0000000000000'::bytea);
SELECT ts_decode_floats('\x46010002000000' || '\x3ff00000000000000000'::bytea);
SELECT ts_decode_floats('\x46010002000000' || '\x3ff000000000000001'::bytea);
-- '10' window reuse before any window was defined
SELECT ts_decode_floats('\x46010002000000' || '\x3ff000000000000080'::bytea);
-- '11' + lead 31 + length 64 (> 64 bits total)
SELECT ts_decode_floats('\x46010002000000' || '\x3ff0000000000000ffff'::bytea);
-- timestamp stream decoding to an out-of-range timestamp
SELECT ts_decode_timestamps('\x54010001000000' || '\x7fffffffffff0000'::bytea);
-- huge delta-of-delta followed by garbage padding
SELECT ts_decode_timestamps('\x54010002000000' || '\x0000000000000000f800000000ffffffff'::bytea);
SELECT * FROM ts_codec_stats('\x4601');

-- bit-flip fuzzing: every single-bit corruption either fails cleanly or
-- decodes to some array (never crashes or over-reads)
CREATE FUNCTION try_decode(b bytea) RETURNS text LANGUAGE plpgsql AS $$
BEGIN
  IF get_byte(b, 0) = 84 THEN
    RETURN coalesce(cardinality(ts_decode_timestamps(b)), -1)::text;
  ELSE
    RETURN coalesce(cardinality(ts_decode_floats(b)), -1)::text;
  END IF;
EXCEPTION WHEN data_corrupted OR invalid_parameter_value THEN
  RETURN 'error';
END $$;
WITH enc AS (
  SELECT ts_encode_floats(ARRAY[1.5, NULL, 2.25, 2.25, -7, 1e300, 'NaN']::float8[]) AS b
  UNION ALL
  SELECT ts_encode_timestamps(ARRAY['2026-01-01', NULL, '2026-01-01 00:00:10',
                                    '2026-01-01 00:00:20', '2026-01-01 00:01']::timestamptz[])
), flips AS (
  SELECT try_decode(set_bit(b, i, 1 - get_bit(b, i))) AS r
  FROM enc, generate_series(0, length(b) * 8 - 1) i
)
SELECT r, count(*) FROM flips GROUP BY r ORDER BY r;
-- every truncation fails cleanly
WITH enc AS (SELECT ts_encode_floats(ARRAY[1.5, 2.25, 2.25, -7]::float8[]) AS b)
SELECT DISTINCT try_decode(substr(b, 1, k)) FROM enc, generate_series(1, length(b) - 1) k;
