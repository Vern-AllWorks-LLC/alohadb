-- pg_parquetio 1.2: write_parquet / write_arrow / arrow_ipc_stream
--
-- Files are written below /tmp (the default pg_parquetio.allowed_paths);
-- file names are kept in psql variables and never printed.

CREATE EXTENSION pg_parquetio VERSION '1.0';
ALTER EXTENSION pg_parquetio UPDATE;
SELECT extversion FROM pg_extension WHERE extname = 'pg_parquetio';

SELECT proname, pg_get_function_arguments(oid) AS args,
       pg_get_function_result(oid) AS result, proisstrict
FROM pg_proc
WHERE proname IN ('write_parquet', 'write_arrow', 'arrow_ipc_stream',
                  'read_parquet', 'read_csv', 'read_json')
ORDER BY proname;

SET pg_parquetio.allowed_paths = '/tmp';
SET timezone = 'UTC';
SET extra_float_digits = 0;

\set f_types '/tmp/pg_parquetio_regress_types.parquet'
\set f_types_none '/tmp/pg_parquetio_regress_types_none.parquet'
\set f_empty '/tmp/pg_parquetio_regress_empty.parquet'
\set f_multi '/tmp/pg_parquetio_regress_multi.parquet'
\set f_fail '/tmp/pg_parquetio_regress_fail.parquet'
\set f_priv '/tmp/pg_parquetio_regress_priv.parquet'
\set f_arrow '/tmp/pg_parquetio_regress_types.arrow'
\set f_arrow_multi '/tmp/pg_parquetio_regress_multi.arrow'
\set f_arrow_empty '/tmp/pg_parquetio_regress_empty.arrow'

-- -----------------------------------------------------------------------
-- Every supported type, with NULLs and edge values
-- -----------------------------------------------------------------------
CREATE TABLE pq_types (
    id int, b bool, i2 int2, i4 int4, i8 int8, f4 float4, f8 float8,
    n numeric, t text, vc varchar(10), bc char(3), by bytea, d date,
    ts timestamp, tstz timestamptz, u uuid, j json, jb jsonb,
    v vector(3), vv vector, fa float4[], da float8[], iv interval
);
INSERT INTO pq_types VALUES
 (1, true, -32768, 2147483647, -9223372036854775808, 1.5, 3.141592653589793,
  12345.678901234567890, 'héllo wörld ✓ 日本', 'abc', 'x', '\x00ff10',
  '2024-02-29', '2024-02-29 12:34:56.123456', '2024-02-29 12:34:56.123456+05:30',
  'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{"a": [1,2]}', '{"k": "v", "n": 1}',
  '[1,2,3]', '[1.5,2.5]', '{1.5,NULL,3}', '{1e15,-0.0}', '1 day 2 hours'),
 (2, false, 32767, -2147483648, 9223372036854775807, 'NaN', '-Infinity',
  -0.5, '', '', '', '', '1970-01-01', '1970-01-01 00:00:00',
  '1999-12-31 23:59:59.999999+00', '00000000-0000-0000-0000-000000000000',
  '[]', '"s"', '[0,0,0]', '[4]', '{}', '{}', '-3 mons'),
 (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
 (4, true, 0, 0, 0, '-0', 'Infinity', 0, E'a"b\\c\ntab\tx', 'z', 'yz', '\x',
  '0001-01-01', '1900-01-01 00:00:00.000001', '2262-04-11 23:47:16.854775+00',
  'ffffffff-ffff-ffff-ffff-ffffffffffff', '{"nested": {"x": null}}',
  '{"arr": [true, false]}', '[-1e-30,3.4e38,0.1]', '[7]', '{NULL}',
  '{NULL,2}', '0');

SELECT write_parquet('SELECT * FROM pq_types ORDER BY id', :'f_types');
SELECT write_parquet('SELECT * FROM pq_types ORDER BY id', :'f_types_none',
                     'none', 1);

-- Read back with this extension's reader
SELECT r FROM read_parquet(:'f_types') r;

-- Uncompressed, one row group per row: same content
SELECT (SELECT array_agg(r) FROM read_parquet(:'f_types') r) =
       (SELECT array_agg(r) FROM read_parquet(:'f_types_none') r) AS same_content;

-- Value-by-value round trip
SELECT t.id,
       (r->>'b')::bool IS NOT DISTINCT FROM t.b AS b,
       (r->>'i2')::int2 IS NOT DISTINCT FROM t.i2 AS i2,
       (r->>'i4')::int4 IS NOT DISTINCT FROM t.i4 AS i4,
       (r->>'i8')::int8 IS NOT DISTINCT FROM t.i8 AS i8,
       (r->>'f4')::float4 IS NOT DISTINCT FROM t.f4 AS f4,
       (r->>'f8')::float8 IS NOT DISTINCT FROM t.f8 AS f8,
       (r->>'n')::numeric IS NOT DISTINCT FROM t.n AS n,
       (r->>'t') IS NOT DISTINCT FROM t.t AS t,
       (r->>'vc') IS NOT DISTINCT FROM t.vc AS vc,
       (r->>'bc')::char(3) IS NOT DISTINCT FROM t.bc AS bc,
       (r->>'by')::bytea IS NOT DISTINCT FROM t.by AS by,
       (r->>'d')::date IS NOT DISTINCT FROM t.d AS d,
       (r->>'ts')::timestamp IS NOT DISTINCT FROM t.ts AS ts,
       (r->>'tstz')::timestamptz IS NOT DISTINCT FROM t.tstz AS tstz,
       (r->>'u')::uuid IS NOT DISTINCT FROM t.u AS u,
       nullif(r->'j', 'null') IS NOT DISTINCT FROM t.j::jsonb AS j,
       nullif(r->'jb', 'null') IS NOT DISTINCT FROM t.jb AS jb,
       (r->>'v')::vector IS NOT DISTINCT FROM t.v AS v,
       (r->>'vv')::vector IS NOT DISTINCT FROM t.vv AS vv,
       nullif(r->'fa', 'null') IS NOT DISTINCT FROM to_jsonb(t.fa) AS fa,
       nullif(r->'da', 'null') IS NOT DISTINCT FROM to_jsonb(t.da) AS da,
       (r->>'iv')::interval IS NOT DISTINCT FROM t.iv AS iv
FROM pq_types t
JOIN read_parquet(:'f_types') r ON (r->>'id')::int = t.id
ORDER BY t.id;

-- -----------------------------------------------------------------------
-- Empty result, several row groups
-- -----------------------------------------------------------------------
SELECT write_parquet('SELECT * FROM pq_types WHERE false', :'f_empty');
SELECT count(*) FROM read_parquet(:'f_empty');

SELECT write_parquet(
    $$SELECT g AS id, CASE WHEN g % 5 = 0 THEN NULL ELSE 'row ' || g END AS label,
             g * 0.5::float8 AS half, ARRAY[g, -g]::float8[] AS pair
      FROM generate_series(1, 2500) g$$,
    :'f_multi', 'snappy', 1000);
SELECT count(*) AS nrows,
       count(r->>'label') AS nonnull_labels,
       sum((r->>'id')::int) AS id_sum,
       sum((r->>'half')::float8) AS half_sum,
       bool_and((r->'pair'->>1)::float8 = -(r->>'id')::float8) AS pairs_ok,
       bool_and((r->>'id')::int % 5 = 0 OR r->>'label' = 'row ' || (r->>'id')) AS labels_ok
FROM read_parquet(:'f_multi') r;

-- File framing: "PAR1" at both ends
SELECT pg_read_binary_file(:'f_multi', 0, 4) = 'PAR1'::bytea AS head_magic,
       pg_read_binary_file(:'f_multi', (pg_stat_file(:'f_multi')).size - 4, 4)
         = 'PAR1'::bytea AS tail_magic;

-- -----------------------------------------------------------------------
-- Arrow IPC file and stream
-- -----------------------------------------------------------------------
SELECT write_arrow('SELECT * FROM pq_types ORDER BY id', :'f_arrow');
SELECT pg_read_binary_file(:'f_arrow', 0, 6) = 'ARROW1'::bytea AS head_magic,
       pg_read_binary_file(:'f_arrow', (pg_stat_file(:'f_arrow')).size - 6, 6)
         = 'ARROW1'::bytea AS tail_magic,
       (pg_stat_file(:'f_arrow')).size % 2 = 0 AS even_size;

SELECT write_arrow('SELECT g, g::text AS s FROM generate_series(1, 1000) g',
                   :'f_arrow_multi', 300);
SELECT (pg_stat_file(:'f_arrow_multi')).size > (pg_stat_file(:'f_arrow')).size AS bigger;
SELECT write_arrow('SELECT * FROM pq_types WHERE false', :'f_arrow_empty');
SELECT pg_read_binary_file(:'f_arrow_empty', 0, 6) = 'ARROW1'::bytea AS empty_magic;

-- stream: schema message, 3 record batches, end-of-stream marker
SELECT ord, substr(m, 1, 4) = '\xffffffff'::bytea AS continuation,
       m = '\xffffffff00000000'::bytea AS eos
FROM arrow_ipc_stream('SELECT g FROM generate_series(1, 10) g', 4)
     WITH ORDINALITY AS s(m, ord)
ORDER BY ord;

-- -----------------------------------------------------------------------
-- Errors
-- -----------------------------------------------------------------------
SELECT write_parquet('SELECT 1', :'f_fail', 'zstd');
SELECT write_parquet('SELECT 1', :'f_fail', 'snappy', 0);
SELECT write_arrow('SELECT 1', :'f_fail', 0);
SELECT arrow_ipc_stream('SELECT 1', -1);
SELECT write_parquet('SELECT 1', 'relative.parquet');
SELECT write_parquet('SELECT 1', '/tmp/pg_parquetio_no_such_dir/x.parquet');
SELECT write_parquet('SELECT 1', '/tmpx_alohadb_parquet.parquet');
SELECT write_parquet('SELECT 1', '/tmp/');
SELECT write_parquet('CREATE TABLE pq_nope (a int)', :'f_fail');
SELECT write_parquet('SELEC 1', :'f_fail');
SELECT write_parquet('SELECT 1 AS a, 2 AS a', :'f_fail');
SELECT write_parquet('SELECT ''{{1,2},{3,4}}''::float8[] AS m', :'f_fail');
SELECT write_arrow('SELECT ''infinity''::timestamp AS ts', :'f_fail');
-- an error during the export leaves no file (and no temporary file) behind
SELECT write_parquet('SELECT 1 / (g - 3) AS x FROM generate_series(1, 5) g', :'f_fail');
SELECT count(*) AS leftover_files
FROM pg_ls_dir('/tmp') AS f
WHERE f LIKE 'pg_parquetio_regress_fail%';

-- -----------------------------------------------------------------------
-- Privileges: like COPY TO file, writing requires pg_write_server_files
-- -----------------------------------------------------------------------
CREATE ROLE regress_pq_writer;
GRANT SELECT ON pq_types TO regress_pq_writer;
SET ROLE regress_pq_writer;
SELECT write_parquet('SELECT 1 AS x', :'f_priv');
SELECT write_arrow('SELECT 1 AS x', :'f_priv');
-- the stream variant writes no file and needs no special privilege
SELECT count(*) FROM arrow_ipc_stream('SELECT id FROM pq_types');
RESET ROLE;
GRANT pg_write_server_files TO regress_pq_writer;
SET ROLE regress_pq_writer;
SELECT write_parquet('SELECT id, t FROM pq_types ORDER BY id', :'f_priv');
RESET ROLE;
SELECT count(*) FROM read_parquet(:'f_priv');

-- -----------------------------------------------------------------------
-- Cleanup
-- -----------------------------------------------------------------------
REVOKE pg_write_server_files FROM regress_pq_writer;
DROP TABLE pq_types;
DROP ROLE regress_pq_writer;
DROP EXTENSION pg_parquetio;
