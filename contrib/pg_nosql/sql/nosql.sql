-- pg_nosql regression test: filter translation (doc_search / doc_count /
-- doc_patch / doc_remove share nosql_filter_to_where)

CREATE EXTENSION pg_nosql;
SELECT doc_create_collection('users') IS NOT NULL AS created;

SELECT count(*) FROM (
  SELECT doc_insert('users', d) FROM unnest(ARRAY[
    '{"name":"Kai","city":"Hilo","age":31,"tags":["a","b"],"vip":true}',
    '{"name":"Lani","city":"Kona","age":25,"tags":["b"],"vip":false}',
    '{"name":"Makoa","city":"Hilo","age":42,"tags":["c"],"note":null}',
    '{"name":"O''Neil","city":"Lihue","age":19,"tags":[]}'
  ]::jsonb[]) d) s;

-- Scalar string equality (used to fail: 'Hilo'::jsonb is not valid JSON)
SELECT d->>'name' AS name FROM doc_search('users', '{"city":"Hilo"}') AS d ORDER BY 1;
-- Numbers, booleans, null
SELECT d->>'name' AS name FROM doc_search('users', '{"age":25}') AS d;
SELECT d->>'name' AS name FROM doc_search('users', '{"vip":true}') AS d;
SELECT d->>'name' AS name FROM doc_search('users', '{"note":null}') AS d;
-- Quotes in values
SELECT d->>'name' AS name FROM doc_search('users', '{"name":"O''Neil"}') AS d;
-- Several keys are ANDed
SELECT d->>'name' AS name FROM doc_search('users', '{"city":"Hilo","age":42}') AS d;
-- Array value is containment (used to match EVERY row)
SELECT d->>'name' AS name FROM doc_search('users', '{"tags":["b"]}') AS d ORDER BY 1;
SELECT count(*) FROM doc_search('users', '{"tags":["zzz"]}');
-- Operator objects
SELECT d->>'name' AS name FROM doc_search('users', '{"age":{"gte":25,"lt":42}}') AS d ORDER BY 1;
SELECT d->>'name' AS name FROM doc_search('users', '{"city":{"in":["Kona","Lihue"]}}') AS d ORDER BY 1;
SELECT count(*) FROM doc_search('users', '{"city":{"in":[]}}');
SELECT d->>'name' AS name FROM doc_search('users', '{"tags":{"contains":"c"}}') AS d;
SELECT d->>'name' AS name FROM doc_search('users', '{"note":{"exists":true}}') AS d;
SELECT d->>'name' AS name FROM doc_search('users', '{"name":{"like":"%a%"}}') AS d ORDER BY 1;
SELECT doc_search('users', '{"age":{"between":[1,2]}}');   -- error
SELECT doc_search('users', '{"age":{"bogus":1}}');         -- error
-- Field names are quoted, not spliced into SQL
SELECT count(*) FROM doc_search('users', '{"x'') OR (true":"y"}');
SELECT count(*) FROM doc_search('users', '{"x'' OR ''1''=''1":{"eq":"z"}}');
-- Options: sort, projection, limit/offset (field names quoted)
SELECT d FROM doc_search('users', '{}', '{"sort":{"age":"desc"},"limit":2,"fields":["name","age"]}') AS d;
SELECT count(*) FROM doc_search('users', '{}', '{"sort":{"name''; DROP TABLE users; --":"asc"},"limit":1,"fields":["name"]}');
SELECT doc_count('users');
-- Counting / patching / removing with an array filter only touches matches
SELECT doc_count('users', '{"tags":["b"]}');
SELECT doc_patch('users', '{"tags":["c"]}', '{"vip":true}');
SELECT d->>'name' AS name FROM doc_search('users', '{"vip":true}') AS d ORDER BY 1;
SELECT doc_remove('users', '{"tags":["c"]}');
SELECT doc_count('users');
SELECT doc_drop_collection('users');
-- Sorting is numeric for numbers (text ordering would give 100, 25, 9)
SELECT doc_create_collection('nums') IS NOT NULL AS created;
SELECT count(*) FROM (SELECT doc_insert('nums', jsonb_build_object('n', v)) FROM unnest(ARRAY[25, 9, 100]) v) s;
SELECT d->'n' AS n FROM doc_search('nums', '{}', '{"sort":{"n":"asc"}}') AS d;
SELECT doc_drop_collection('nums');
DROP EXTENSION pg_nosql;
