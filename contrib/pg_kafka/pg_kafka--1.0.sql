\echo Use "CREATE EXTENSION pg_kafka" to load this file. \quit

-- -----------------------------------------------------------------------
-- pg_kafka: inbound streaming from Apache Kafka and Amazon Kinesis
--
-- Kafka: background worker uses librdkafka (Apache 2.0).
-- Kinesis: uses the public AWS Kinesis REST API (HTTP GET Records).
-- SigV4 signing is performed in pg_objectstore; Kinesis consumers
-- here delegate signing to the objectstore_s3 module.
-- Patent-mitigation: Kafka protocol is an open standard; librdkafka is
-- Apache 2.0.  Kinesis REST API is publicly documented.  No proprietary
-- streaming algorithms used.
-- -----------------------------------------------------------------------

-- Consumer registry
CREATE TABLE pg_kafka_consumers (
    consumer_id     bigserial   PRIMARY KEY,
    consumer_name   text        NOT NULL UNIQUE,
    consumer_type   text        NOT NULL DEFAULT 'kafka',  -- 'kafka' | 'kinesis'
    -- Kafka fields
    broker_list     text,           -- comma-separated host:port list
    topic           text,
    consumer_group  text,
    offset_reset    text        NOT NULL DEFAULT 'latest',  -- 'earliest' | 'latest'
    -- Kinesis fields
    stream_arn      text,
    aws_region      text,
    shard_iterator_type text    DEFAULT 'LATEST',  -- 'LATEST' | 'TRIM_HORIZON'
    credential_label text,          -- references pg_objectstore credentials
    -- Control
    enabled         boolean     NOT NULL DEFAULT false,
    poll_batch_size int         NOT NULL DEFAULT 500,
    created_by      text        NOT NULL DEFAULT current_user,
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- Consumed message store
CREATE TABLE pg_kafka_messages (
    msg_id          bigserial   PRIMARY KEY,
    consumer_id     bigint      NOT NULL REFERENCES pg_kafka_consumers,
    topic_partition text,           -- "topic:partition" for Kafka; shard id for Kinesis
    msg_offset      bigint,         -- Kafka offset or Kinesis sequence number as int
    msg_key         text,
    payload         jsonb,
    headers         jsonb,
    received_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
    processed       boolean     NOT NULL DEFAULT false
);

CREATE INDEX pg_kafka_messages_consumer_idx
    ON pg_kafka_messages (consumer_id, processed);
CREATE INDEX pg_kafka_messages_time_idx
    ON pg_kafka_messages (received_at DESC);

-- Consumer runtime stats (updated by background worker)
CREATE TABLE pg_kafka_stats (
    consumer_id         bigint      PRIMARY KEY REFERENCES pg_kafka_consumers,
    messages_consumed   bigint      NOT NULL DEFAULT 0,
    last_consumed_at    timestamptz,
    last_offset         bigint,
    last_error          text,
    worker_pid          int
);

-- -----------------------------------------------------------------------
-- Management API
-- -----------------------------------------------------------------------

CREATE FUNCTION kafka_consumer_create(
    p_name          text,
    p_broker_list   text,
    p_topic         text,
    p_consumer_group text DEFAULT NULL,
    p_offset_reset  text DEFAULT 'latest',
    p_batch_size    int  DEFAULT 500
)
RETURNS bigint
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE v_id bigint;
BEGIN
    INSERT INTO pg_kafka_consumers
        (consumer_name, consumer_type, broker_list, topic,
         consumer_group, offset_reset, poll_batch_size)
    VALUES
        (p_name, 'kafka', p_broker_list, p_topic,
         COALESCE(p_consumer_group, 'alohadb_' || p_name),
         p_offset_reset, p_batch_size)
    RETURNING consumer_id INTO v_id;

    INSERT INTO pg_kafka_stats (consumer_id) VALUES (v_id);
    RETURN v_id;
END;
$$;

CREATE FUNCTION kinesis_stream_create(
    p_name                  text,
    p_stream_arn            text,
    p_aws_region            text,
    p_credential_label      text,
    p_shard_iterator_type   text DEFAULT 'LATEST',
    p_batch_size            int  DEFAULT 500
)
RETURNS bigint
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE v_id bigint;
BEGIN
    INSERT INTO pg_kafka_consumers
        (consumer_name, consumer_type, stream_arn, aws_region,
         credential_label, shard_iterator_type, poll_batch_size)
    VALUES
        (p_name, 'kinesis', p_stream_arn, p_aws_region,
         p_credential_label, p_shard_iterator_type, p_batch_size)
    RETURNING consumer_id INTO v_id;

    INSERT INTO pg_kafka_stats (consumer_id) VALUES (v_id);
    RETURN v_id;
END;
$$;

-- Enable / disable a consumer (background worker checks enabled flag)
CREATE FUNCTION kafka_consumer_start(p_consumer_name text)
RETURNS void
LANGUAGE plpgsql VOLATILE STRICT
AS $$
BEGIN
    UPDATE pg_kafka_consumers SET enabled = true
    WHERE  consumer_name = p_consumer_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'kafka_consumer_start: consumer "%" not found', p_consumer_name;
    END IF;
END;
$$;

CREATE FUNCTION kafka_consumer_stop(p_consumer_name text)
RETURNS void
LANGUAGE plpgsql VOLATILE STRICT
AS $$
BEGIN
    UPDATE pg_kafka_consumers SET enabled = false
    WHERE  consumer_name = p_consumer_name;
END;
$$;

-- Poll pending (unprocessed) messages from the message store
CREATE FUNCTION kafka_messages_poll(
    p_consumer_name text,
    p_limit         int DEFAULT 1000
)
RETURNS TABLE(
    msg_id          bigint,
    topic_partition text,
    msg_offset      bigint,
    msg_key         text,
    payload         jsonb,
    headers         jsonb,
    received_at     timestamptz
)
LANGUAGE sql VOLATILE STRICT
AS $$
    WITH msgs AS (
        SELECT m.msg_id, m.topic_partition, m.msg_offset, m.msg_key,
               m.payload, m.headers, m.received_at
        FROM   pg_kafka_messages m
        JOIN   pg_kafka_consumers c ON c.consumer_id = m.consumer_id
        WHERE  c.consumer_name = p_consumer_name
          AND  m.processed = false
        ORDER BY m.msg_id
        LIMIT  p_limit
        FOR UPDATE OF m SKIP LOCKED
    ),
    mark AS (
        UPDATE pg_kafka_messages SET processed = true
        WHERE  msg_id IN (SELECT msg_id FROM msgs)
    )
    SELECT msg_id, topic_partition, msg_offset, msg_key,
           payload, headers, received_at
    FROM   msgs;
$$;

-- Consumer status
CREATE FUNCTION kafka_consumer_status(p_consumer_name text DEFAULT NULL)
RETURNS TABLE(
    consumer_name       text,
    consumer_type       text,
    enabled             boolean,
    messages_consumed   bigint,
    pending_messages    bigint,
    last_consumed_at    timestamptz,
    last_error          text,
    worker_pid          int
)
LANGUAGE sql STABLE
AS $$
    SELECT c.consumer_name,
           c.consumer_type,
           c.enabled,
           s.messages_consumed,
           (SELECT count(*) FROM pg_kafka_messages m
            WHERE m.consumer_id = c.consumer_id AND NOT m.processed),
           s.last_consumed_at,
           s.last_error,
           s.worker_pid
    FROM   pg_kafka_consumers c
    JOIN   pg_kafka_stats     s ON s.consumer_id = c.consumer_id
    WHERE  p_consumer_name IS NULL OR c.consumer_name = p_consumer_name
    ORDER BY c.consumer_name;
$$;

-- Purge processed messages older than an interval
CREATE FUNCTION kafka_messages_purge(
    p_consumer_name text,
    p_older_than    interval DEFAULT '7 days'
)
RETURNS bigint
LANGUAGE plpgsql VOLATILE STRICT
AS $$
DECLARE deleted bigint;
BEGIN
    DELETE FROM pg_kafka_messages m
    USING  pg_kafka_consumers c
    WHERE  c.consumer_id    = m.consumer_id
      AND  c.consumer_name  = p_consumer_name
      AND  m.processed      = true
      AND  m.received_at    < now() - p_older_than;
    GET DIAGNOSTICS deleted = ROW_COUNT;
    RETURN deleted;
END;
$$;
