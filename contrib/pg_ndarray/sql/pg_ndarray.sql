CREATE EXTENSION pg_ndarray;

--
-- 2-D float64 array, 5x7 with 2x3 chunks (grid 3x3, edge chunks partial)
--
SELECT ndarray_create('m', 'float64', '{5,7}', '{2,3}') > 0 AS created;
SELECT name, dtype, shape, chunk_shape, chunk_grid, nelems, codec, fill_value
  FROM ndarray_info('m');

-- a 3x5 region spanning 2x3 chunks
SELECT ndarray_write('m', '{1,2}',
       '{{1,2,3,4,5},{6,7,8,9,10},{11,12,13,14,15}}');
SELECT ndarray_read('m', '{0,0}', '{5,7}');
-- strided read: rows 0,2,4, cols 0,3,6
SELECT ndarray_read('m', '{0,0}', '{5,7}', '{2,3}');
SELECT ndarray_read('m', '{1,3}', '{3,6}');
SELECT ndarray_get('m', '{3,6}') AS v15, ndarray_get('m', '{4,6}') AS missing;
SELECT chunks_total, chunks_stored, raw_bytes FROM ndarray_storage('m');
SELECT chunk_idx FROM ndarray_chunk c JOIN ndarray a USING (array_id)
 WHERE a.name = 'm' ORDER BY 1;

-- reductions (full array: 1..15 plus twenty zeros)
SELECT ndarray_reduce('m', 'sum') AS sum,
       round(ndarray_reduce('m', 'mean')::numeric, 6) AS mean,
       ndarray_reduce('m', 'min') AS min,
       ndarray_reduce('m', 'max') AS max,
       round(ndarray_reduce('m', 'std')::numeric, 6) AS std,
       ndarray_reduce('m', 'count_nonzero') AS nnz;
-- partial region rows 2..3: 6..15 plus four zeros
SELECT ndarray_reduce('m', 'sum', '{2,0}', '{4,7}') AS sum,
       ndarray_reduce('m', 'mean', '{2,0}', '{4,7}') AS mean,
       ndarray_reduce('m', 'min', '{2,2}', '{4,7}') AS min,
       ndarray_reduce('m', 'count_nonzero', '{0,0}', '{2,4}') AS nnz;
-- start only / stop only
SELECT ndarray_reduce('m', 'sum', '{3,0}') AS tail,
       ndarray_reduce('m', 'sum', NULL, '{2,7}') AS head;
SELECT ndarray_reduce_axis('m', 'sum', 0) AS colsum,
       ndarray_reduce_axis('m', 'sum', 1) AS rowsum,
       ndarray_reduce_axis('m', 'max', 1) AS rowmax,
       ndarray_reduce_axis('m', 'count_nonzero', 0) AS colnnz;
SELECT ndarray_reduce_axis('m', 'mean', 1) AS rowmean;
SELECT array(SELECT round(x::numeric, 6)
               FROM unnest(ndarray_reduce_axis('m', 'std', 0)) x) AS colstd;

-- overwrite a single element, tensor write
SELECT ndarray_write('m', '{0,0}', '{{-1}}');
SELECT ndarray_write_tensor('m', '{4,5}', 'shape=(1,2) data=[100,200]');
SELECT ndarray_read('m', '{0,0}', '{1,2}') AS r0, ndarray_read('m', '{4,4}', '{5,7}') AS r4;
SELECT ndarray_reduce('m', 'min') AS min, ndarray_reduce('m', 'sum') AS sum;

-- transactional: rolled back writes vanish
BEGIN;
SELECT ndarray_write('m', '{2,2}', '{{999}}');
SELECT ndarray_get('m', '{2,2}');
ROLLBACK;
SELECT ndarray_get('m', '{2,2}');

--
-- int32 array with fill 7, chunk 3x3 over 4x4: dtype conversion
--
SELECT ndarray_create('f', 'int32', '{4,4}', '{3,3}', fill_value => 7) > 0 AS created;
SELECT ndarray_read('f', '{0,0}', '{4,4}');
SELECT ndarray_reduce('f', 'sum') AS sum, ndarray_reduce_axis('f', 'sum', 0) AS colsum;
SELECT ndarray_write('f', '{0,0}', '{{-3.7, 5.5}}');
SELECT ndarray_write('f', '{3,3}', '{{2.9}}');
SELECT ndarray_read('f', '{0,0}', '{4,4}');
SELECT ndarray_write('f', '{1,1}', '{{3e9}}');
SELECT ndarray_write('f', '{1,1}', '{{NaN}}');
SELECT ndarray_create('bad', 'int32', '{4}', '{2}', fill_value => 'NaN');
-- fill values are stored as the dtype represents them
SELECT ndarray_create('f2', 'int64', '{3}', '{2}', 'none', 2.75) > 0 AS created;
SELECT ndarray_read('f2', '{0}', '{3}'), fill_value FROM ndarray_info('f2');
SELECT ndarray_create('f3', 'float32', '{3}', '{2}', fill_value => 0.1) > 0 AS created;
SELECT ndarray_get('f3', '{1}') = 0.1::float4::float8 AS float32_fill;
SELECT ndarray_write('f3', '{0}', '{1e300}');

--
-- 3-D float32 array 3x4x5, chunks 2x3x2 (all dims have edge chunks),
-- uncompressed, fill -1
--
SELECT ndarray_create('c', 'float32', '{3,4,5}', '{2,3,2}', 'none', -1) > 0 AS created;
SELECT ndarray_write('c', '{0,0,0}',
       (SELECT array_agg(m ORDER BY i) FROM
          (SELECT i, array_agg(r ORDER BY j) m FROM
             (SELECT i, j, array_agg((i*20 + j*5 + k)::float8 ORDER BY k) r
                FROM generate_series(0,2) i, generate_series(0,3) j,
                     generate_series(0,4) k GROUP BY i, j) s
           GROUP BY i) t));
SELECT ndarray_read('c', '{0,1,1}', '{3,4,5}', '{2,2,3}');
SELECT ndarray_read('c', '{1,3,0}', '{2,4,5}');
SELECT ndarray_reduce('c', 'sum') AS sum, ndarray_reduce('c', 'max') AS max,
       ndarray_reduce('c', 'sum', '{1,1,1}', '{3,3,3}') AS partial;
SELECT ndarray_reduce_axis('c', 'sum', 2);
SELECT ndarray_reduce_axis('c', 'min', 0);
SELECT ndarray_read_tensor('c', '{1,0,0}', '{2,2,3}');
SELECT ndarray_read_tensor('c', '{0,0,0}', '{3,4,5}', '{2,3,4}');
SELECT chunks_total, chunks_stored, raw_bytes, stored_bytes, compression_ratio
  FROM ndarray_storage('c');

-- sparse 3-D: one element written, the rest is fill
SELECT ndarray_create('c2', 'float64', '{3,4,5}', '{2,2,2}', fill_value => 1) > 0 AS created;
SELECT ndarray_write('c2', '{2,3,4}', '{{{10}}}');
SELECT ndarray_reduce('c2', 'sum') AS sum, ndarray_reduce('c2', 'mean') AS mean,
       ndarray_reduce('c2', 'min') AS min, ndarray_reduce('c2', 'max') AS max,
       round(ndarray_reduce('c2', 'std')::numeric, 6) AS std,
       ndarray_reduce('c2', 'count_nonzero') AS nnz;
SELECT ndarray_reduce('c2', 'sum', '{0,0,0}', '{2,4,5}') AS untouched;
SELECT ndarray_read('c2', '{1,2,3}', '{3,4,5}');

-- large sparse array: point lookups vs range scan
SELECT ndarray_create('big', 'float64', '{1000,1000}', '{10,10}') > 0 AS created;
SELECT ndarray_write('big', '{509,509}', '{{1,2},{3,4}}');
SELECT ndarray_reduce('big', 'sum') AS full_sum,
       ndarray_reduce('big', 'sum', '{0,509}', '{1000,510}') AS col509,
       ndarray_reduce('big', 'mean', '{509,509}', '{511,511}') AS mean4,
       ndarray_reduce('big', 'count_nonzero') AS nnz;
SELECT ndarray_read('big', '{508,508}', '{512,512}');
SELECT ndarray_read('big', '{0,1}', '{1000,1000}', '{509,508}');
SELECT chunks_total, chunks_stored FROM ndarray_storage('big');

--
-- zstd round trip and compression ratio on a constant array
--
SELECT ndarray_create('z', 'float64', '{100,100}', '{50,50}') > 0 AS created;
SELECT ndarray_write('z', '{0,0}', array_fill(3.0::float8, ARRAY[100,100]));
SELECT ndarray_reduce('z', 'sum') AS sum, ndarray_reduce('z', 'min') AS min,
       ndarray_reduce('z', 'max') AS max, ndarray_reduce('z', 'std') AS std;
SELECT chunks_stored, raw_bytes, stored_bytes < raw_bytes / 10 AS small,
       compression_ratio > 1 AS compressed
  FROM ndarray_storage('z');
SELECT ndarray_read('z', '{48,49}', '{52,51}');

--
-- map (chunkwise into a new array)
--
SELECT ndarray_map('m', 'm2', 'mul', 2);
SELECT ndarray_read('m2', '{0,0}', '{5,7}');
SELECT ndarray_map('m', 'mp1', 'add', 1);
SELECT ndarray_read('mp1', '{3,0}', '{5,3}'), fill_value FROM ndarray_info('mp1');
SELECT ndarray_map('m', 'mc', 'clip', 3, 8);
SELECT ndarray_read('mc', '{0,0}', '{5,7}');
SELECT ndarray_map('m', 'mlo', 'clip', scalar2 => 2);
SELECT ndarray_read('mlo', '{1,0}', '{2,7}');
SELECT ndarray_map('m', 'mabs', 'abs');
SELECT ndarray_get('mabs', '{0,0}');
SELECT ndarray_map('f', 'fsqrt', 'sqrt');
SELECT dtype, fill_value FROM ndarray_info('fsqrt');
SELECT round(ndarray_get('fsqrt', '{0,1}')::numeric, 6) AS sqrt5,
       ndarray_get('fsqrt', '{0,0}') AS sqrt_neg;
SELECT ndarray_map('f', 'fdiv', 'div', 2, dtype => 'int32');
SELECT ndarray_read('fdiv', '{0,0}', '{4,4}'), fill_value FROM ndarray_info('fdiv');
SELECT ndarray_map('z', 'zexp', 'exp');
SELECT ndarray_map('zexp', 'zlog', 'log');
SELECT ndarray_reduce('zlog', 'mean') AS mean_log_exp;
SELECT ndarray_map('m', 'mpow', 'pow', 2);
SELECT ndarray_reduce('mpow', 'sum') AS sum_squares;

--
-- binary (elementwise over the union of stored chunks)
--
SELECT ndarray_binary('m', 'm2', 'mb', 'add');
SELECT ndarray_read('mb', '{0,0}', '{5,7}');
SELECT ndarray_binary('m', 'mp1', 'mm', 'mul');
SELECT ndarray_read('mm', '{3,4}', '{5,7}'), fill_value FROM ndarray_info('mm');
SELECT ndarray_binary('m2', 'm', 'mmax', 'max');
SELECT ndarray_reduce('mmax', 'min') AS min_of_max;
SELECT ndarray_binary('m', 'c', 'bad', 'add');
SELECT ndarray_binary('m', 'm2', 'bad', 'mod');

--
-- matmul (blocked, 2-D only)
--
SELECT ndarray_create('A', 'float64', '{3,4}', '{2,2}') > 0 AS created;
SELECT ndarray_create('B', 'int32', '{4,2}', '{2,1}') > 0 AS created;
SELECT ndarray_write('A', '{0,0}', '{{1,2,3,4},{5,6,7,8},{9,10,11,12}}');
SELECT ndarray_write('B', '{0,0}', '{{1,0},{0,1},{1,0},{0,1}}');
SELECT ndarray_matmul('A', 'B', 'AB');
SELECT ndarray_read('AB', '{0,0}', '{3,2}');
SELECT ndarray_matmul('A', 'A', 'bad');

--
-- tensor slicing helpers
--
SELECT tensor_slice('shape=(2,3,4) data=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]'::tensor,
                    '{0,1,1}', '{2,3,4}');
SELECT tensor_slice('shape=(2,3,4) data=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]'::tensor,
                    '{0,0,0}', '{2,3,4}', '{1,2,2}');
SELECT tensor_index('shape=(2,3,4) data=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]'::tensor,
                    '{1,2,3}') AS t123,
       tensor_index('shape=(2,3) data=[1.5,2.5,3.5,4.5,5.5,6.5]'::tensor, '{1,0}') AS t10;
SELECT tensor_slice('shape=(4) data=[1,2,3,4]'::tensor, NULL, '{2}') IS NULL AS null_in;
SELECT tensor_slice('shape=(2,3) data=[1,2,3,4,5,6]'::tensor, '{0,0}', '{3,3}');
SELECT tensor_slice('shape=(2,3) data=[1,2,3,4,5,6]'::tensor, '{0}', '{1}');
SELECT tensor_index('shape=(2,3) data=[1,2,3,4,5,6]'::tensor, '{0,3}');

--
-- error cases
--
SELECT ndarray_read('m', '{0,0}', '{6,7}');
SELECT ndarray_read('m', '{2,0}', '{2,7}');
SELECT ndarray_read('m', '{-1,0}', '{2,7}');
SELECT ndarray_read('m', '{0,0}', '{2,7}', '{0,1}');
SELECT ndarray_read('m', '{0}', '{2}');
SELECT ndarray_read('m', '{0,NULL}', '{2,2}');
SELECT ndarray_get('m', '{5,0}');
SELECT ndarray_write('m', '{4,5}', '{{1,2,3}}');
SELECT ndarray_write('m', '{0,0}', '{1,2,3}');
SELECT ndarray_write('m', '{0,0}', '{{1,NULL}}');
SELECT ndarray_write_tensor('m', '{0,0}', 'shape=(3) data=[1,2,3]');
SELECT ndarray_read('nosuch', '{0}', '{1}');
SELECT ndarray_create('m', 'float64', '{5}', '{2}');
SELECT ndarray_create('x', 'complex64', '{5}', '{2}');
SELECT ndarray_create('x', 'float64', '{5}', '{0}');
SELECT ndarray_create('x', 'float64', '{5,0}', '{2,2}');
SELECT ndarray_create('x', 'float64', '{5,5}', '{2}');
SELECT ndarray_create('x', 'float64', '{5}', '{2}', 'lz4');
SELECT ndarray_create('x', 'float64', '{100000,100000}', '{10000,10000}');
SELECT ndarray_create('x', 'float64', '{4611686018427387904,4}', '{1,1}');
SELECT ndarray_create('x', 'float64', '{1,1,1,1,1,1,1,1,1}', '{1,1,1,1,1,1,1,1,1}');
SELECT ndarray_create(NULL, 'float64', '{5}', '{2}');
SELECT ndarray_reduce('m', 'median');
SELECT ndarray_reduce_axis('m', 'sum', 2);
SELECT ndarray_map('m', 'm2', 'mul', 2);
SELECT ndarray_map('m', 'x', 'mul');
SELECT ndarray_map('m', 'x', 'sqrt', 2);
SELECT ndarray_map('m', 'x', 'clip', 5, 1);
SELECT ndarray_create('wide', 'float32', '{70000}', '{1000}') > 0 AS created;
SELECT ndarray_read_tensor('wide', '{0}', '{70000}');
SELECT tensor_shape(ndarray_read_tensor('wide', '{0}', '{70000}', '{2}'));
SELECT ndarray_write('wide', '{5}', '{Infinity}');
SELECT ndarray_read_tensor('wide', '{4}', '{6}');
SELECT cardinality(ndarray_read('big', '{0,0}', '{1000,1000}')) AS n_1m;

--
-- catalog management
--
SELECT name, dtype, shape, chunk_shape, codec, fill_value, chunks_stored
  FROM ndarray_list() WHERE name IN ('m', 'f', 'c', 'z');
SELECT ndarray_drop('m2');
SELECT ndarray_drop('m2', true);
SELECT ndarray_drop('m2');
SELECT count(*) AS orphan_chunks FROM ndarray_chunk c
 WHERE NOT EXISTS (SELECT 1 FROM ndarray a WHERE a.array_id = c.array_id);
SELECT attstorage FROM pg_attribute
 WHERE attrelid = 'ndarray_chunk'::regclass AND attname = 'data';
SELECT ndarray_create('attrs', 'float64', '{2}', '{2}', attrs => '{"units": "K"}') > 0 AS created;
SELECT attrs FROM ndarray_info('attrs');
DROP EXTENSION pg_ndarray;
