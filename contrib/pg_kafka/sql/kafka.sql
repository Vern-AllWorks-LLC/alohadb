-- pg_kafka regression test
-- Tests: consumer registration, management functions, message table structure

CREATE EXTENSION pg_kafka;

-- -----------------------------------------------------------------------
-- Consumer registration (SQL layer only; no real broker needed)
-- -----------------------------------------------------------------------

SELECT kafka_consumer_create(
    'orders_consumer',
    'kafka',
    'broker1:9092,broker2:9092',
    'orders',
    'orders_group'
);

SELECT kinesis_stream_create(
    'events_stream',
    'us-east-1',
    'my-event-stream',
    '{"aws_access_key_id": "AKIAIOSFODNN7EXAMPLE", "aws_secret_access_key": "test"}'::jsonb
);

-- -----------------------------------------------------------------------
-- Consumer status (newly created consumers are stopped)
-- -----------------------------------------------------------------------

SELECT consumer_name, consumer_type, topic_or_stream, status
FROM   kafka_consumer_status()
ORDER BY consumer_name;

-- -----------------------------------------------------------------------
-- Start / stop
-- -----------------------------------------------------------------------

SELECT kafka_consumer_start('orders_consumer');

SELECT consumer_name, status
FROM   kafka_consumer_status()
WHERE  consumer_name = 'orders_consumer';

SELECT kafka_consumer_stop('orders_consumer');

SELECT consumer_name, status
FROM   kafka_consumer_status()
WHERE  consumer_name = 'orders_consumer';

-- -----------------------------------------------------------------------
-- Message table structure
-- -----------------------------------------------------------------------

SELECT column_name, data_type
FROM   information_schema.columns
WHERE  table_schema = 'public'
  AND  table_name   = 'pg_kafka_messages'
ORDER BY ordinal_position;

-- -----------------------------------------------------------------------
-- Simulate stored messages
-- -----------------------------------------------------------------------

INSERT INTO pg_kafka_messages
    (consumer_id, payload, partition_key, offset_or_seqno)
SELECT
    consumer_id,
    convert_to('{"event":"order_created","id":1}', 'UTF8'),
    '0',
    100
FROM pg_kafka_consumers
WHERE consumer_name = 'orders_consumer';

SELECT count(*) AS msg_count
FROM   pg_kafka_messages m
JOIN   pg_kafka_consumers c USING (consumer_id)
WHERE  c.consumer_name = 'orders_consumer';

-- Poll (marks as processed)
SELECT count(*) AS polled
FROM   kafka_messages_poll('orders_consumer', 10);

-- Purge processed messages
SELECT kafka_messages_purge('orders_consumer');

SELECT count(*) AS after_purge
FROM   pg_kafka_messages m
JOIN   pg_kafka_consumers c USING (consumer_id)
WHERE  c.consumer_name = 'orders_consumer';

-- -----------------------------------------------------------------------
-- Cleanup
-- -----------------------------------------------------------------------

DELETE FROM pg_kafka_consumers
WHERE consumer_name IN ('orders_consumer', 'events_stream');

DROP EXTENSION pg_kafka;
