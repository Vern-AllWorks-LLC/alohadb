CREATE EXTENSION pg_cache;

-- basic set / get
SELECT cache_set('j1', '{"a": 1, "b": [1, 2]}');
SELECT cache_get('j1');
SELECT cache_get_text('j1');
SELECT cache_set_text('t1', 'hello');
SELECT cache_get_text('t1');
SELECT cache_get('missing') IS NULL AS missing;
SELECT cache_delete('t1'), cache_delete('t1');
SELECT cache_get_text('t1') IS NULL AS deleted;

-- errors
SELECT cache_set_text('', 'x');
SELECT cache_set_text(repeat('k', 256), 'x');
SELECT cache_set_text('k', NULL);
SELECT cache_set_text('k', 'x', '-1 second');
SELECT cache_set_text('big', repeat('x', 300000));
SELECT cache_get('notjson') IS NULL;
SELECT cache_set_text('notjson', 'not json');
SELECT cache_get('notjson');

-- transactional writes: applied at commit, discarded on abort
BEGIN;
SELECT cache_set_text('tx', 'v1');
SELECT cache_get_text('tx') AS own_write_visible;
ROLLBACK;
SELECT cache_get_text('tx') IS NULL AS rolled_back;

BEGIN;
SELECT cache_set_text('tx', 'v2');
COMMIT;
SELECT cache_get_text('tx') AS committed;

-- a failing statement discards its cache writes too
SELECT cache_set_text('tx', 'v3'), 1/0;
SELECT cache_get_text('tx') AS still_v2;

-- delete and flush are transactional as well
BEGIN;
SELECT cache_delete('tx');
SELECT cache_get_text('tx') IS NULL AS deleted_in_txn;
ROLLBACK;
SELECT cache_get_text('tx') AS survived;

BEGIN;
SELECT cache_flush() > 0 AS flushed;
SELECT cache_get_text('tx') IS NULL AS flushed_in_txn;
SELECT cache_set_text('after_flush', 'x');
SELECT cache_get_text('after_flush');
ROLLBACK;
SELECT cache_get_text('tx') AS survived_flush, cache_get_text('after_flush') IS NULL AS no_after_flush;

-- subtransactions
BEGIN;
SELECT cache_set_text('s1', 'outer');
SAVEPOINT a;
SELECT cache_set_text('s1', 'inner');
SELECT cache_set_text('s2', 'inner');
ROLLBACK TO SAVEPOINT a;
SELECT cache_get_text('s1') AS s1_outer, cache_get_text('s2') IS NULL AS s2_gone;
SAVEPOINT b;
SELECT cache_set_text('s3', 'kept');
RELEASE SAVEPOINT b;
COMMIT;
SELECT cache_get_text('s1'), cache_get_text('s2'), cache_get_text('s3');

-- cache update tied to a table update in one transaction
CREATE TABLE users (id int PRIMARY KEY, name text);
INSERT INTO users VALUES (42, 'alice');
SELECT cache_set('user:42', to_jsonb(u)) FROM users u WHERE id = 42;
BEGIN;
UPDATE users SET name = 'bob' WHERE id = 42;
SELECT cache_set('user:42', to_jsonb(u)) FROM users u WHERE id = 42;
ROLLBACK;
SELECT u.name AS table_name, cache_get('user:42')->>'name' AS cached_name FROM users u WHERE id = 42;

-- PREPARE TRANSACTION refuses pending cache writes
BEGIN;
SELECT cache_set_text('p', 'x');
PREPARE TRANSACTION 'p1';
SELECT cache_get_text('p') IS NULL AS not_applied;

-- non-transactional mode applies immediately
SET pg_cache.transactional = off;
BEGIN;
SELECT cache_set_text('nt', 'now');
ROLLBACK;
SELECT cache_get_text('nt') AS immediate;
RESET pg_cache.transactional;

-- TTL
SELECT cache_set_text('ttl', 'x', '1 hour');
SELECT cache_ttl('ttl') BETWEEN '59 minutes' AND '1 hour' AS ttl_ok;
SELECT cache_ttl('t-none') IS NULL AS ttl_missing;
SELECT cache_set_text('t-inf', 'x');
SELECT cache_ttl('t-inf');
SELECT cache_expire('t-inf', '10 minutes');
SELECT cache_ttl('t-inf') <= '10 minutes' AS expire_applied;
SELECT cache_expire('t-inf', NULL);
SELECT cache_ttl('t-inf');
SELECT cache_expire('nope', '1 minute');
SELECT cache_set_text('short', 'x', '50 milliseconds');
SELECT pg_sleep(0.2);
SELECT cache_get_text('short') IS NULL AS expired;
SELECT cache_purge_expired() >= 0 AS purged;

-- add (set if absent) and incr are immediate and atomic
SELECT cache_add('lock:1', 'me'), cache_add('lock:1', 'you');
BEGIN;
SELECT cache_add('lock:2', 'x');
ROLLBACK;
SELECT cache_get_text('lock:2') AS add_not_rolled_back;
SELECT cache_incr('ctr'), cache_incr('ctr'), cache_incr('ctr', 10), cache_incr('ctr', -2);
SELECT cache_get_text('ctr');
SELECT cache_incr('lock:1');
SELECT cache_set_text('maxc', '9223372036854775807');
SELECT cache_incr('maxc');

-- keys
SELECT k FROM cache_keys('s%') AS k ORDER BY 1;

-- eviction: max_entries = 200 in the test configuration
SELECT cache_flush() >= 0;
SELECT count(cache_set_text('e' || g, repeat('v', 100))) FROM generate_series(1, 500) g;
SELECT entries <= 200 AS capped, evictions > 0 AS evicted FROM cache_stats();
-- recently read keys survive (SIEVE): touch e450..e500, then add more
SELECT count(cache_get_text('e' || g)) FROM generate_series(450, 500) g;
SELECT count(cache_set_text('f' || g, repeat('v', 100))) FROM generate_series(1, 100) g;
SELECT count(cache_get_text('e' || g)) AS hot_kept FROM generate_series(450, 500) g;

-- memory cap: 1MB in the test configuration
SELECT cache_flush() >= 0;
SELECT count(cache_set_text('m' || g, repeat('v', 20000))) FROM generate_series(1, 150) g;
SELECT used_bytes <= max_bytes AS within_budget, entries < 150 AS evicted_for_memory FROM cache_stats();

SELECT max_entries, max_bytes FROM cache_stats();

-- privileges
CREATE ROLE cache_user;
SET ROLE cache_user;
SELECT cache_get_text('x');
RESET ROLE;
GRANT EXECUTE ON FUNCTION cache_get_text(text) TO cache_user;
SET ROLE cache_user;
SELECT cache_get_text('ctr');
RESET ROLE;
REVOKE EXECUTE ON FUNCTION cache_get_text(text) FROM cache_user;
DROP ROLE cache_user;

DROP TABLE users;
DROP EXTENSION pg_cache;
