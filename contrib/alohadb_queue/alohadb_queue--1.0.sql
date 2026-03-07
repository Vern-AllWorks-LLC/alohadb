/* contrib/alohadb_queue/alohadb_queue--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_queue" to load this file. \quit

-- ----------------------------------------------------------------
-- alohadb_queue_queues
--
-- Registry of named queues with their configuration parameters.
-- ----------------------------------------------------------------
CREATE TABLE alohadb_queue_queues (
    name                text        PRIMARY KEY,
    visibility_timeout  interval    NOT NULL DEFAULT '30 seconds',
    retention_period    interval    NOT NULL DEFAULT '7 days',
    created_at          timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE alohadb_queue_queues IS
'Registry of message queues managed by alohadb_queue';

COMMENT ON COLUMN alohadb_queue_queues.visibility_timeout IS
'Default time a received message stays invisible before re-delivery';

COMMENT ON COLUMN alohadb_queue_queues.retention_period IS
'How long messages are retained before eligible for cleanup';

-- ----------------------------------------------------------------
-- alohadb_queue_messages
--
-- Stores individual messages.  The status column tracks the
-- message lifecycle: ready -> delivered -> acknowledged.
-- ----------------------------------------------------------------
CREATE TABLE alohadb_queue_messages (
    msg_id          bigserial       PRIMARY KEY,
    queue_name      text            NOT NULL REFERENCES alohadb_queue_queues(name),
    body            jsonb           NOT NULL,
    headers         jsonb,
    status          text            NOT NULL DEFAULT 'ready',
    visible_after   timestamptz     NOT NULL DEFAULT now(),
    delivery_count  int             NOT NULL DEFAULT 0,
    created_at      timestamptz     NOT NULL DEFAULT now(),
    updated_at      timestamptz     NOT NULL DEFAULT now()
);

COMMENT ON TABLE alohadb_queue_messages IS
'Messages stored in alohadb_queue queues';

-- Partial index for efficient message polling: only ready messages
-- that are past their visibility window.
CREATE INDEX ON alohadb_queue_messages (queue_name, visible_after)
    WHERE status = 'ready';

-- ----------------------------------------------------------------
-- alohadb_queue_consumers
--
-- Tracks consumer group offsets per queue for Kafka-style
-- ordered consumption.
-- ----------------------------------------------------------------
CREATE TABLE alohadb_queue_consumers (
    consumer_group  text    NOT NULL,
    queue_name      text    NOT NULL REFERENCES alohadb_queue_queues(name),
    last_offset     bigint  NOT NULL DEFAULT 0,
    PRIMARY KEY (consumer_group, queue_name)
);

COMMENT ON TABLE alohadb_queue_consumers IS
'Consumer group offset tracking for ordered message consumption';

-- ----------------------------------------------------------------
-- queue_create
--
-- Create a new named queue.
-- ----------------------------------------------------------------
CREATE FUNCTION queue_create(
    name                text,
    visibility_timeout  interval DEFAULT '30 seconds',
    retention_period    interval DEFAULT '7 days'
)
RETURNS void
AS 'MODULE_PATHNAME', 'queue_create'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION queue_create(text, interval, interval) IS
'Create a new message queue with the given name and configuration';

-- ----------------------------------------------------------------
-- queue_drop
--
-- Drop a queue and all its messages and consumer subscriptions.
-- ----------------------------------------------------------------
CREATE FUNCTION queue_drop(
    name text
)
RETURNS void
AS 'MODULE_PATHNAME', 'queue_drop'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION queue_drop(text) IS
'Drop a queue and delete all its messages and consumer subscriptions';

-- ----------------------------------------------------------------
-- queue_send
--
-- Send a single message to a queue.  Returns the msg_id.
-- ----------------------------------------------------------------
CREATE FUNCTION queue_send(
    queue_name  text,
    body        jsonb,
    headers     jsonb DEFAULT NULL
)
RETURNS bigint
AS 'MODULE_PATHNAME', 'queue_send'
LANGUAGE C VOLATILE CALLED ON NULL INPUT;

COMMENT ON FUNCTION queue_send(text, jsonb, jsonb) IS
'Send a single message to the named queue; returns the message ID';

-- ----------------------------------------------------------------
-- queue_send_batch
--
-- Send multiple messages from a jsonb array.  Returns count.
-- ----------------------------------------------------------------
CREATE FUNCTION queue_send_batch(
    queue_name  text,
    bodies      jsonb[]
)
RETURNS bigint
AS 'MODULE_PATHNAME', 'queue_send_batch'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION queue_send_batch(text, jsonb[]) IS
'Send a batch of messages to the named queue; returns the count inserted';

-- ----------------------------------------------------------------
-- queue_receive
--
-- Receive messages from a queue.  Messages are atomically locked
-- and marked as delivered.
-- ----------------------------------------------------------------
CREATE FUNCTION queue_receive(
    queue_name          text,
    batch_size          int      DEFAULT 1,
    visibility_timeout  interval DEFAULT NULL
)
RETURNS TABLE(
    msg_id          bigint,
    body            jsonb,
    headers         jsonb,
    delivery_count  int
)
AS 'MODULE_PATHNAME', 'queue_receive'
LANGUAGE C VOLATILE CALLED ON NULL INPUT;

COMMENT ON FUNCTION queue_receive(text, int, interval) IS
'Receive up to batch_size messages from the queue, marking them as delivered';

-- ----------------------------------------------------------------
-- queue_ack
--
-- Acknowledge a message as processed.
-- ----------------------------------------------------------------
CREATE FUNCTION queue_ack(
    queue_name  text,
    msg_id      bigint
)
RETURNS boolean
AS 'MODULE_PATHNAME', 'queue_ack'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION queue_ack(text, bigint) IS
'Acknowledge a message, marking it as processed';

-- ----------------------------------------------------------------
-- queue_nack
--
-- Negatively acknowledge a message, returning it to ready status.
-- ----------------------------------------------------------------
CREATE FUNCTION queue_nack(
    queue_name  text,
    msg_id      bigint
)
RETURNS boolean
AS 'MODULE_PATHNAME', 'queue_nack'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION queue_nack(text, bigint) IS
'Negatively acknowledge a message, returning it to ready status for re-delivery';

-- ----------------------------------------------------------------
-- queue_purge
--
-- Delete all messages from a queue.
-- ----------------------------------------------------------------
CREATE FUNCTION queue_purge(
    queue_name text
)
RETURNS bigint
AS 'MODULE_PATHNAME', 'queue_purge'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION queue_purge(text) IS
'Delete all messages from the named queue; returns the count deleted';

-- ----------------------------------------------------------------
-- queue_stats
--
-- Return message count statistics broken down by status.
-- ----------------------------------------------------------------
CREATE FUNCTION queue_stats(
    queue_name text
)
RETURNS TABLE(
    total           bigint,
    ready           bigint,
    delivered       bigint,
    acknowledged    bigint
)
AS 'MODULE_PATHNAME', 'queue_stats'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION queue_stats(text) IS
'Return message count statistics for the named queue by status';

-- ----------------------------------------------------------------
-- queue_subscribe
--
-- Register a consumer group for a queue.
-- ----------------------------------------------------------------
CREATE FUNCTION queue_subscribe(
    consumer_group  text,
    queue_name      text
)
RETURNS void
AS 'MODULE_PATHNAME', 'queue_subscribe'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION queue_subscribe(text, text) IS
'Subscribe a consumer group to a queue for offset-based consumption';

-- ----------------------------------------------------------------
-- queue_poll
--
-- Poll for messages using consumer group offset tracking.
-- ----------------------------------------------------------------
CREATE FUNCTION queue_poll(
    consumer_group  text,
    queue_name      text,
    batch_size      int DEFAULT 1
)
RETURNS TABLE(
    msg_id          bigint,
    body            jsonb,
    headers         jsonb,
    delivery_count  int
)
AS 'MODULE_PATHNAME', 'queue_poll'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION queue_poll(text, text, int) IS
'Poll for messages using consumer group offset; does not lock or change message status';

-- ----------------------------------------------------------------
-- queue_commit_offset
--
-- Update the consumer group offset for a queue.
-- ----------------------------------------------------------------
CREATE FUNCTION queue_commit_offset(
    consumer_group  text,
    queue_name      text,
    offset_val      bigint
)
RETURNS void
AS 'MODULE_PATHNAME', 'queue_commit_offset'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION queue_commit_offset(text, text, bigint) IS
'Commit the consumer group offset, marking all messages up to offset_val as consumed';
