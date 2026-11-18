CREATE EXTENSION pg_kv;

SELECT relpersistence FROM pg_class WHERE relname = 'kv_store';

SELECT kv_set('a', 'one');
SELECT kv_get('a');
SELECT kv_set('a', 'uno');
SELECT kv_get('a');
SELECT kv_get('missing') IS NULL AS missing;
SELECT kv_del('a'), kv_del('a');
SELECT kv_set(NULL, 'x');
SELECT kv_set('x', 'y', '0 seconds');

-- transactional, like any table
BEGIN;
SELECT kv_set('tx', 'v');
ROLLBACK;
SELECT kv_get('tx') IS NULL AS rolled_back;

-- add / incr
SELECT kv_add('lock', 'me'), kv_add('lock', 'you');
SELECT kv_get('lock');
SELECT kv_incr('n'), kv_incr('n'), kv_incr('n', 5), kv_incr('n', -1);
SELECT kv_incr('lock');

-- TTL
SELECT kv_set('t', 'x', '1 hour');
SELECT kv_ttl('t') BETWEEN '59 minutes' AND '1 hour' AS ttl_ok;
SELECT kv_ttl('n');
SELECT kv_ttl('none') IS NULL AS ttl_missing;
SELECT kv_expire('n', '10 minutes'), kv_ttl('n') <= '10 minutes' AS expire_applied;
SELECT kv_expire('n', NULL), kv_ttl('n');
SELECT kv_expire('none', '1 minute');
SELECT kv_set('short', 'x', '50 milliseconds');
SELECT kv_add('short2', 'x', '50 milliseconds');
SELECT kv_incr('short3', 7, '50 milliseconds');
SELECT pg_sleep(0.2);
SELECT kv_get('short') IS NULL AS expired;
SELECT kv_add('short2', 'again') AS add_over_expired, kv_get('short2');
SELECT kv_incr('short3') AS incr_restarts, kv_ttl('short3');
SELECT kv_del('short') AS del_expired_is_false;
SELECT kv_purge_expired();

-- multi-get and key listing
SELECT * FROM kv_get_many(ARRAY['lock', 'n', 'nope']) ORDER BY key;
SELECT kv_keys('s%');
SELECT kv_keys();

SELECT kv_flush() AS flushed;
SELECT count(*) FROM kv_keys();

DROP EXTENSION pg_kv;
