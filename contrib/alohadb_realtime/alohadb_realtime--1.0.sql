/* contrib/alohadb_realtime/alohadb_realtime--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_realtime" to load this file. \quit

-- ----------------------------------------------------------------
-- alohadb_realtime_subscriptions
--
-- Each row defines a realtime subscription: a table to watch,
-- an optional filter expression, and the NOTIFY channel to use.
-- ----------------------------------------------------------------
CREATE TABLE alohadb_realtime_subscriptions (
    id            serial       PRIMARY KEY,
    table_name    regclass     NOT NULL,
    filter_expr   text,
    channel       text         DEFAULT 'alohadb_realtime',
    created_at    timestamptz  DEFAULT now()
);

COMMENT ON TABLE alohadb_realtime_subscriptions IS
'Subscriptions for realtime change notifications on tables';

COMMENT ON COLUMN alohadb_realtime_subscriptions.table_name IS
'The table being watched for changes';

COMMENT ON COLUMN alohadb_realtime_subscriptions.filter_expr IS
'Optional SQL expression to filter which row changes generate events';

COMMENT ON COLUMN alohadb_realtime_subscriptions.channel IS
'NOTIFY channel name for delivering change notifications';

-- ----------------------------------------------------------------
-- alohadb_realtime_events
--
-- Stores captured change events. Each row represents one INSERT,
-- UPDATE, or DELETE that occurred on a subscribed table.
-- ----------------------------------------------------------------
CREATE TABLE alohadb_realtime_events (
    event_id         bigserial    PRIMARY KEY,
    subscription_id  int,
    table_name       text,
    operation        char(1),
    row_data         jsonb,
    old_row_data     jsonb,
    created_at       timestamptz  DEFAULT now()
);

CREATE INDEX idx_alohadb_realtime_events_created_at
    ON alohadb_realtime_events (created_at);

COMMENT ON TABLE alohadb_realtime_events IS
'Captured change events from subscribed tables';

COMMENT ON COLUMN alohadb_realtime_events.operation IS
'I = INSERT, U = UPDATE, D = DELETE';

COMMENT ON COLUMN alohadb_realtime_events.row_data IS
'JSON representation of the new row (INSERT/UPDATE) or deleted row (DELETE)';

COMMENT ON COLUMN alohadb_realtime_events.old_row_data IS
'JSON representation of the old row (UPDATE only), NULL for INSERT/DELETE';

-- ----------------------------------------------------------------
-- alohadb_realtime_trigger()
--
-- Trigger function that captures row changes into the events table
-- and sends a NOTIFY on the subscription channel.
-- ----------------------------------------------------------------
CREATE FUNCTION alohadb_realtime_trigger()
RETURNS trigger
AS 'MODULE_PATHNAME', 'alohadb_realtime_trigger'
LANGUAGE C;

COMMENT ON FUNCTION alohadb_realtime_trigger() IS
'Trigger function that captures row changes and sends NOTIFY';

-- ----------------------------------------------------------------
-- alohadb_realtime_subscribe(regclass, text)
--
-- Creates a subscription on the given table. Installs an AFTER
-- trigger that fires on INSERT, UPDATE, and DELETE.
-- Returns the subscription ID.
-- ----------------------------------------------------------------
CREATE FUNCTION alohadb_realtime_subscribe(
    target_table  regclass,
    filter        text DEFAULT NULL
)
RETURNS int
AS 'MODULE_PATHNAME', 'alohadb_realtime_subscribe'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION alohadb_realtime_subscribe(regclass, text) IS
'Subscribe to realtime change notifications on a table; returns subscription ID';

-- ----------------------------------------------------------------
-- alohadb_realtime_unsubscribe(int)
--
-- Removes a subscription and drops the associated trigger.
-- ----------------------------------------------------------------
CREATE FUNCTION alohadb_realtime_unsubscribe(
    sub_id  int
)
RETURNS void
AS 'MODULE_PATHNAME', 'alohadb_realtime_unsubscribe'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION alohadb_realtime_unsubscribe(int) IS
'Remove a realtime subscription and drop the associated trigger';

-- ----------------------------------------------------------------
-- alohadb_realtime_poll(bigint)
--
-- Returns events with event_id greater than the given cursor.
-- Used for polling-based consumption of the event stream.
-- ----------------------------------------------------------------
CREATE FUNCTION alohadb_realtime_poll(
    since_event_id  bigint DEFAULT 0
)
RETURNS SETOF alohadb_realtime_events
AS 'MODULE_PATHNAME', 'alohadb_realtime_poll'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION alohadb_realtime_poll(bigint) IS
'Poll for new realtime events since the given event_id';

-- ----------------------------------------------------------------
-- alohadb_realtime_cleanup(interval)
--
-- Deletes events older than the specified interval.
-- Returns the number of events deleted.
-- ----------------------------------------------------------------
CREATE FUNCTION alohadb_realtime_cleanup(
    older_than  interval DEFAULT '1 hour'
)
RETURNS int
AS 'MODULE_PATHNAME', 'alohadb_realtime_cleanup'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION alohadb_realtime_cleanup(interval) IS
'Delete realtime events older than the specified interval; returns count deleted';
