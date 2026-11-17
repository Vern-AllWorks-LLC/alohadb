--
-- Casts from numeric arrays to the core vector type (as in pgvector).
-- Cases adapted from pgvector 0.8.7 test/sql/cast.sql (PostgreSQL License);
-- error texts and the dimension limit are those of the core vector type.
--
SELECT ARRAY[1,2,3]::vector;
SELECT ARRAY[1.0,2.0,3.0]::vector;
SELECT ARRAY[1,2,3]::real[]::vector;
SELECT ARRAY[1,2,3]::double precision[]::vector;
SELECT ARRAY[1,2,3]::numeric[]::vector;

SELECT '{1,2,3}'::real[]::vector;
SELECT '{1,2,3}'::real[]::vector(3);
SELECT '{1,2,3}'::real[]::vector(2);
SELECT '{NULL}'::real[]::vector;
SELECT '{NaN}'::real[]::vector;
SELECT '{Infinity}'::real[]::vector;
SELECT '{-Infinity}'::real[]::vector;
SELECT '{}'::real[]::vector;
SELECT '{{1}}'::real[]::vector;

SELECT '{1,2,3}'::double precision[]::vector;
SELECT '{1,2,3}'::double precision[]::vector(3);
SELECT '{1,2,3}'::double precision[]::vector(2);
SELECT '{4e38,-4e38}'::double precision[]::vector;
SELECT '{1e-46,-1e-46}'::double precision[]::vector;
SELECT '{1e40}'::numeric[]::vector;

SELECT array_agg(n)::vector FROM generate_series(1, 16385) n;
SELECT array_to_vector(array_agg(n), 16385, false) FROM generate_series(1, 16385) n;
SELECT vector_dims(array_agg(n)::vector) FROM generate_series(1, 16384) n;

-- assignment casts: arrays can be stored in vector columns directly
CREATE TEMP TABLE vc (v vector(3));
INSERT INTO vc VALUES (ARRAY[1,2,3]), ('{4.5,5,6}'::real[]), (ARRAY[7,8,9]::float8[]), (ARRAY[1.5,2.5,3.5]::numeric[]);
INSERT INTO vc VALUES (ARRAY[1,2]);
UPDATE vc SET v = ARRAY[0,0,1] WHERE v = '[1,2,3]';
SELECT v FROM vc ORDER BY v <-> '[0,0,0]';
-- not implicit: no cast to vector when resolving operators
SELECT ARRAY[1,2,3] <-> '[1,2,3]'::vector;
-- the original one-argument conversion function still exists
SELECT array_to_vector('{1,2,3}'::real[]);
