\echo Use "CREATE EXTENSION alohadb_unified" to load this file. \quit

-- -------------------------------------------------------------------------
-- alohadb_unified: unified streaming and batch query model
--
-- Core claim matched (Ali Ghodsi / Databricks):
--   "you can reason about the semantics of streaming and non-streaming
--    in the same way"
--
-- Implementation: each registered "unified source" gets a SQL VIEW that
-- UNION ALLs its batch table with a stream event buffer.  Querying the
-- view always returns the latest state: committed batch rows PLUS any
-- stream events not yet flushed.  The query is identical whether data
-- is at rest or actively streaming in.
--
-- Patent-mitigation notes:
--   - Delta Lake (Databricks): patents US10977234, US11243946 cover the
--     log-structured merge-tree storage format and transaction log.
--     We use no log; batch data lives in an ordinary PostgreSQL table and
--     stream events in a simple buffer table.
--   - Spark Structured Streaming: patents cover the micro-batch execution
--     engine, stateful operator graphs, and watermark-based event-time
--     processing.  We implement none of these; our "unified" semantic is
--     a plain SQL UNION ALL view — prior art predates Spark by decades.
--   - The jsonb_populate_record() approach for schema-mapping is a
--     standard PostgreSQL built-in (since PG 9.3, 2013).
-- -------------------------------------------------------------------------

-- Source registry
CREATE TABLE alohadb_unified_sources (
    source_name     text        NOT NULL PRIMARY KEY,
    batch_schema    text        NOT NULL DEFAULT 'public',
    batch_table     text        NOT NULL,
    view_name       text        NOT NULL,       -- the unified view name
    registered_by   text        NOT NULL DEFAULT current_user,
    registered_at   timestamptz NOT NULL DEFAULT now(),
    last_flush_at   timestamptz,
    total_flushed   bigint      NOT NULL DEFAULT 0
);

-- Stream event buffer: incoming events that have not yet been merged
-- into the batch table.  Stored as jsonb so a single buffer table
-- serves all registered sources without schema coupling.
CREATE TABLE alohadb_unified_buffer (
    event_id        bigserial   PRIMARY KEY,
    source_name     text        NOT NULL REFERENCES alohadb_unified_sources
                                ON DELETE CASCADE,
    event_data      jsonb       NOT NULL,
    event_time      timestamptz NOT NULL DEFAULT clock_timestamp(),
    flushed         boolean     NOT NULL DEFAULT false
);

CREATE INDEX alohadb_unified_buffer_source_idx
    ON alohadb_unified_buffer (source_name, flushed);
CREATE INDEX alohadb_unified_buffer_time_idx
    ON alohadb_unified_buffer (event_time DESC);

-- -------------------------------------------------------------------------
-- unified_register: register a batch table as a unified source.
--
-- Creates:
--   1. A row in alohadb_unified_sources.
--   2. A view <source_name>_unified (or p_view_name if supplied) that
--      UNION ALLs the batch table with unflushed buffer events.
--
-- The view uses jsonb_populate_record() to cast buffer jsonb payloads to
-- the batch table's row type, so the result set schema is identical
-- whether data came from the batch table or the stream buffer.
-- -------------------------------------------------------------------------
CREATE FUNCTION unified_register(
    p_source_name   text,
    p_batch_table   text,
    p_batch_schema  text DEFAULT 'public',
    p_view_name     text DEFAULT NULL       -- defaults to <source_name>_unified
)
RETURNS void
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    v_view_name  text;
    v_type_ref   text;
BEGIN
    v_view_name := COALESCE(p_view_name, p_source_name || '_unified');
    -- Fully-qualified type reference for jsonb_populate_record
    v_type_ref  := quote_ident(p_batch_schema) || '.' || quote_ident(p_batch_table);

    -- Validate that the batch table exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = p_batch_schema
          AND table_name   = p_batch_table
    ) THEN
        RAISE EXCEPTION 'unified_register: table %.% does not exist',
                        p_batch_schema, p_batch_table;
    END IF;

    -- Register the source (insert before creating the view so the buffer
    -- foreign key is satisfied if events arrive before first flush)
    INSERT INTO alohadb_unified_sources
        (source_name, batch_schema, batch_table, view_name)
    VALUES
        (p_source_name, p_batch_schema, p_batch_table, v_view_name)
    ON CONFLICT (source_name)
    DO UPDATE SET
        batch_schema = EXCLUDED.batch_schema,
        batch_table  = EXCLUDED.batch_table,
        view_name    = EXCLUDED.view_name;

    -- Create (or replace) the unified view.
    -- Pattern: SELECT * FROM batch UNION ALL cast(buffer events) AS batch type.
    -- This is the key that makes streaming and batch "look the same":
    -- a user queries one view name regardless of whether data is in the
    -- batch table or only in the stream buffer.
    EXECUTE format(
        $sql$
        CREATE OR REPLACE VIEW %I AS
            SELECT * FROM %I.%I
            UNION ALL
            SELECT (jsonb_populate_record(null::%s, b.event_data)).*
            FROM   alohadb_unified_buffer b
            WHERE  b.source_name = %L
              AND  b.flushed = false
        $sql$,
        v_view_name,
        p_batch_schema, p_batch_table,
        v_type_ref,
        p_source_name
    );
END;
$$;

-- -------------------------------------------------------------------------
-- unified_ingest: write a stream event into the buffer for a source.
-- This is the "streaming write" path — equivalent to writing to a stream.
-- -------------------------------------------------------------------------
CREATE FUNCTION unified_ingest(
    p_source_name text,
    p_event_data  jsonb
)
RETURNS bigint  -- assigned event_id
LANGUAGE plpgsql VOLATILE STRICT
AS $$
DECLARE
    new_event_id bigint;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM alohadb_unified_sources
        WHERE source_name = p_source_name
    ) THEN
        RAISE EXCEPTION 'unified_ingest: source "%" is not registered', p_source_name;
    END IF;

    INSERT INTO alohadb_unified_buffer (source_name, event_data)
    VALUES (p_source_name, p_event_data)
    RETURNING event_id INTO new_event_id;

    RETURN new_event_id;
END;
$$;

-- -------------------------------------------------------------------------
-- unified_flush: merge buffered stream events into the batch table.
-- After flushing, the unified view continues to return the same rows —
-- they just come from the batch table instead of the buffer.
-- p_limit: max events to flush in one call (NULL = all pending).
-- -------------------------------------------------------------------------
CREATE FUNCTION unified_flush(
    p_source_name text,
    p_limit       int DEFAULT NULL
)
RETURNS bigint  -- number of events flushed
LANGUAGE plpgsql VOLATILE STRICT
AS $$
DECLARE
    src         alohadb_unified_sources%ROWTYPE;
    flushed_ids bigint[];
    flush_count bigint;
BEGIN
    SELECT * INTO src
    FROM   alohadb_unified_sources
    WHERE  source_name = p_source_name
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'unified_flush: source "%" is not registered', p_source_name;
    END IF;

    -- Collect the IDs of events to flush (oldest first)
    SELECT array_agg(event_id ORDER BY event_id)
    INTO   flushed_ids
    FROM  (
        SELECT event_id
        FROM   alohadb_unified_buffer
        WHERE  source_name = p_source_name
          AND  flushed = false
        ORDER BY event_id
        LIMIT  p_limit
    ) sub;

    IF flushed_ids IS NULL OR array_length(flushed_ids, 1) = 0 THEN
        RETURN 0;
    END IF;

    -- Insert into the batch table using jsonb_populate_record for type coercion.
    -- This is standard PostgreSQL: no proprietary merge algorithm.
    EXECUTE format(
        $sql$
        INSERT INTO %I.%I
        SELECT (jsonb_populate_record(null::%I.%I, event_data)).*
        FROM   alohadb_unified_buffer
        WHERE  event_id = ANY($1)
        $sql$,
        src.batch_schema, src.batch_table,
        src.batch_schema, src.batch_table
    ) USING flushed_ids;

    -- Mark as flushed (keep rows for audit; a separate purge cleans old ones)
    UPDATE alohadb_unified_buffer
    SET    flushed = true
    WHERE  event_id = ANY(flushed_ids);

    flush_count := array_length(flushed_ids, 1);

    UPDATE alohadb_unified_sources
    SET    last_flush_at = now(),
           total_flushed = total_flushed + flush_count
    WHERE  source_name = p_source_name;

    RETURN flush_count;
END;
$$;

-- -------------------------------------------------------------------------
-- unified_stream_events: query only the unflushed stream buffer for a source.
-- Useful for inspecting "what has arrived but not yet landed in batch".
-- -------------------------------------------------------------------------
CREATE FUNCTION unified_stream_events(
    p_source_name text,
    p_limit       int DEFAULT 1000
)
RETURNS TABLE(
    event_id   bigint,
    event_data jsonb,
    event_time timestamptz
)
LANGUAGE sql STABLE STRICT
AS $$
    SELECT event_id, event_data, event_time
    FROM   alohadb_unified_buffer
    WHERE  source_name = p_source_name
      AND  flushed = false
    ORDER BY event_id
    LIMIT  p_limit;
$$;

-- Purge flushed buffer rows older than a given age (maintenance)
CREATE FUNCTION unified_purge(
    p_source_name text,
    p_older_than  interval DEFAULT '7 days'
)
RETURNS bigint
LANGUAGE plpgsql VOLATILE STRICT
AS $$
DECLARE
    deleted_count bigint;
BEGIN
    DELETE FROM alohadb_unified_buffer
    WHERE  source_name = p_source_name
      AND  flushed = true
      AND  event_time < now() - p_older_than;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$;

-- Drop a unified source and its view
CREATE FUNCTION unified_drop(p_source_name text)
RETURNS void
LANGUAGE plpgsql VOLATILE STRICT
AS $$
DECLARE
    v_view_name text;
BEGIN
    SELECT view_name INTO v_view_name
    FROM   alohadb_unified_sources
    WHERE  source_name = p_source_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'unified_drop: source "%" is not registered', p_source_name;
    END IF;

    -- Buffer rows cascade-delete via FK
    DELETE FROM alohadb_unified_sources WHERE source_name = p_source_name;

    EXECUTE format('DROP VIEW IF EXISTS %I', v_view_name);
END;
$$;

-- List all registered unified sources
CREATE FUNCTION unified_list()
RETURNS TABLE(
    source_name   text,
    batch_schema  text,
    batch_table   text,
    view_name     text,
    pending_events bigint,
    total_flushed bigint,
    last_flush_at timestamptz,
    registered_at timestamptz
)
LANGUAGE sql STABLE
AS $$
    SELECT s.source_name,
           s.batch_schema,
           s.batch_table,
           s.view_name,
           (SELECT count(*) FROM alohadb_unified_buffer b
            WHERE b.source_name = s.source_name AND NOT b.flushed),
           s.total_flushed,
           s.last_flush_at,
           s.registered_at
    FROM   alohadb_unified_sources s
    ORDER BY s.source_name;
$$;

-- Per-source statistics
CREATE FUNCTION unified_stats(p_source_name text)
RETURNS TABLE(
    source_name     text,
    batch_table     text,
    view_name       text,
    pending_events  bigint,
    oldest_pending  timestamptz,
    newest_pending  timestamptz,
    total_flushed   bigint,
    last_flush_at   timestamptz
)
LANGUAGE sql STABLE STRICT
AS $$
    SELECT s.source_name,
           s.batch_schema || '.' || s.batch_table,
           s.view_name,
           (SELECT count(*)  FROM alohadb_unified_buffer b
            WHERE b.source_name = s.source_name AND NOT b.flushed),
           (SELECT min(event_time) FROM alohadb_unified_buffer b
            WHERE b.source_name = s.source_name AND NOT b.flushed),
           (SELECT max(event_time) FROM alohadb_unified_buffer b
            WHERE b.source_name = s.source_name AND NOT b.flushed),
           s.total_flushed,
           s.last_flush_at
    FROM   alohadb_unified_sources s
    WHERE  s.source_name = p_source_name;
$$;
