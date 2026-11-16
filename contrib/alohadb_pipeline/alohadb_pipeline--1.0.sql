\echo Use "CREATE EXTENSION alohadb_pipeline" to load this file. \quit

-- -----------------------------------------------------------------------
-- alohadb_pipeline: DAG-based ETL pipeline orchestration
--
-- Patent-mitigation: DAG scheduling uses Kahn's topological sort (1962).
-- Dependency-graph state machines predate modern workflow patents by
-- decades. No Airflow, Prefect, or Databricks-specific algorithms used.
-- -----------------------------------------------------------------------

-- Pipeline definitions
CREATE TABLE alohadb_pipeline_defs (
    pipeline_id     bigserial   PRIMARY KEY,
    pipeline_name   text        NOT NULL UNIQUE,
    description     text,
    enabled         boolean     NOT NULL DEFAULT true,
    created_by      text        NOT NULL DEFAULT current_user,
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- Task definitions
CREATE TABLE alohadb_pipeline_tasks (
    task_id         bigserial   PRIMARY KEY,
    pipeline_id     bigint      NOT NULL REFERENCES alohadb_pipeline_defs ON DELETE CASCADE,
    task_name       text        NOT NULL,
    task_type       text        NOT NULL DEFAULT 'sql', -- 'sql' | 'python' | 'function'
    task_sql        text,           -- for type='sql'
    task_script     text,           -- for type='python' (script name in alohadb_python)
    task_function   text,           -- for type='function' (schema.func(args) call)
    max_retries     int         NOT NULL DEFAULT 0,
    retry_delay_s   int         NOT NULL DEFAULT 60,
    timeout_s       int,            -- NULL = no timeout
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (pipeline_id, task_name)
);

CREATE INDEX alohadb_pipeline_tasks_pipeline_idx
    ON alohadb_pipeline_tasks (pipeline_id);

-- Task dependency edges (task_id depends on depends_on_task_id succeeding)
CREATE TABLE alohadb_pipeline_task_deps (
    task_id             bigint NOT NULL REFERENCES alohadb_pipeline_tasks ON DELETE CASCADE,
    depends_on_task_id  bigint NOT NULL REFERENCES alohadb_pipeline_tasks ON DELETE CASCADE,
    dep_type            text   NOT NULL DEFAULT 'success', -- 'success' | 'failure' | 'any'
    PRIMARY KEY (task_id, depends_on_task_id)
);

-- Pipeline run instances
CREATE TABLE alohadb_pipeline_runs (
    run_id          bigserial   PRIMARY KEY,
    pipeline_id     bigint      NOT NULL REFERENCES alohadb_pipeline_defs,
    status          text        NOT NULL DEFAULT 'pending',
        -- 'pending' | 'running' | 'succeeded' | 'failed' | 'cancelled'
    triggered_by    text        NOT NULL DEFAULT current_user,
    started_at      timestamptz,
    completed_at    timestamptz,
    error_message   text
);

CREATE INDEX alohadb_pipeline_runs_pipeline_idx
    ON alohadb_pipeline_runs (pipeline_id, status);

-- Per-task run state within a pipeline run
CREATE TABLE alohadb_pipeline_task_runs (
    task_run_id     bigserial   PRIMARY KEY,
    run_id          bigint      NOT NULL REFERENCES alohadb_pipeline_runs ON DELETE CASCADE,
    task_id         bigint      NOT NULL REFERENCES alohadb_pipeline_tasks,
    status          text        NOT NULL DEFAULT 'pending',
        -- 'pending' | 'ready' | 'running' | 'succeeded' | 'failed' | 'skipped'
    attempt         int         NOT NULL DEFAULT 0,
    started_at      timestamptz,
    completed_at    timestamptz,
    error_message   text,
    UNIQUE (run_id, task_id)
);

CREATE INDEX alohadb_pipeline_task_runs_run_idx
    ON alohadb_pipeline_task_runs (run_id, status);

-- -----------------------------------------------------------------------
-- Management API
-- -----------------------------------------------------------------------

-- Create a pipeline
CREATE FUNCTION pipeline_create(
    p_name        text,
    p_description text DEFAULT NULL
)
RETURNS bigint
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE v_id bigint;
BEGIN
    INSERT INTO alohadb_pipeline_defs (pipeline_name, description)
    VALUES (p_name, p_description)
    RETURNING pipeline_id INTO v_id;
    RETURN v_id;
END;
$$;

-- Add a task to a pipeline
CREATE FUNCTION pipeline_task_add(
    p_pipeline_name text,
    p_task_name     text,
    p_task_type     text   DEFAULT 'sql',
    p_task_sql      text   DEFAULT NULL,
    p_task_script   text   DEFAULT NULL,
    p_task_function text   DEFAULT NULL,
    p_max_retries   int    DEFAULT 0,
    p_retry_delay_s int    DEFAULT 60,
    p_timeout_s     int    DEFAULT NULL
)
RETURNS bigint
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    v_pipeline_id bigint;
    v_task_id     bigint;
BEGIN
    SELECT pipeline_id INTO v_pipeline_id
    FROM   alohadb_pipeline_defs
    WHERE  pipeline_name = p_pipeline_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pipeline_task_add: pipeline "%" not found', p_pipeline_name;
    END IF;

    INSERT INTO alohadb_pipeline_tasks
        (pipeline_id, task_name, task_type, task_sql, task_script,
         task_function, max_retries, retry_delay_s, timeout_s)
    VALUES
        (v_pipeline_id, p_task_name, p_task_type, p_task_sql, p_task_script,
         p_task_function, p_max_retries, p_retry_delay_s, p_timeout_s)
    RETURNING task_id INTO v_task_id;

    RETURN v_task_id;
END;
$$;

-- Declare a dependency between two tasks
CREATE FUNCTION pipeline_task_depend(
    p_pipeline_name     text,
    p_task_name         text,
    p_depends_on_name   text,
    p_dep_type          text DEFAULT 'success'
)
RETURNS void
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    v_pipeline_id       bigint;
    v_task_id           bigint;
    v_depends_on_task_id bigint;
BEGIN
    SELECT pipeline_id INTO v_pipeline_id
    FROM   alohadb_pipeline_defs WHERE pipeline_name = p_pipeline_name;

    SELECT task_id INTO v_task_id
    FROM   alohadb_pipeline_tasks
    WHERE  pipeline_id = v_pipeline_id AND task_name = p_task_name;

    SELECT task_id INTO v_depends_on_task_id
    FROM   alohadb_pipeline_tasks
    WHERE  pipeline_id = v_pipeline_id AND task_name = p_depends_on_name;

    IF v_task_id IS NULL OR v_depends_on_task_id IS NULL THEN
        RAISE EXCEPTION 'pipeline_task_depend: task not found in pipeline "%"',
                        p_pipeline_name;
    END IF;

    INSERT INTO alohadb_pipeline_task_deps
        (task_id, depends_on_task_id, dep_type)
    VALUES
        (v_task_id, v_depends_on_task_id, p_dep_type)
    ON CONFLICT DO NOTHING;
END;
$$;

-- Trigger a pipeline run (creates run + task_run rows; background worker executes)
CREATE FUNCTION pipeline_run(p_pipeline_name text)
RETURNS bigint
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    v_pipeline_id bigint;
    v_run_id      bigint;
BEGIN
    SELECT pipeline_id INTO v_pipeline_id
    FROM   alohadb_pipeline_defs
    WHERE  pipeline_name = p_pipeline_name AND enabled = true;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pipeline_run: pipeline "%" not found or disabled',
                        p_pipeline_name;
    END IF;

    INSERT INTO alohadb_pipeline_runs (pipeline_id, status)
    VALUES (v_pipeline_id, 'pending')
    RETURNING run_id INTO v_run_id;

    -- Seed task run rows for every task in this pipeline
    INSERT INTO alohadb_pipeline_task_runs (run_id, task_id, status)
    SELECT v_run_id, task_id, 'pending'
    FROM   alohadb_pipeline_tasks
    WHERE  pipeline_id = v_pipeline_id;

    -- Immediately mark tasks that have no dependencies as ready
    UPDATE alohadb_pipeline_task_runs tr
    SET    status = 'ready'
    WHERE  tr.run_id = v_run_id
      AND  NOT EXISTS (
               SELECT 1 FROM alohadb_pipeline_task_deps d
               WHERE d.task_id = tr.task_id
           );

    RETURN v_run_id;
END;
$$;

-- Cancel an in-progress run
CREATE FUNCTION pipeline_cancel(p_run_id bigint)
RETURNS void
LANGUAGE plpgsql VOLATILE STRICT
AS $$
BEGIN
    UPDATE alohadb_pipeline_runs
    SET    status = 'cancelled', completed_at = now()
    WHERE  run_id = p_run_id AND status IN ('pending', 'running');

    UPDATE alohadb_pipeline_task_runs
    SET    status = 'skipped'
    WHERE  run_id = p_run_id AND status IN ('pending', 'ready');
END;
$$;

-- Status of a run and all its tasks
CREATE FUNCTION pipeline_status(p_run_id bigint)
RETURNS TABLE(
    run_id        bigint,
    run_status    text,
    task_name     text,
    task_status   text,
    attempt       int,
    started_at    timestamptz,
    completed_at  timestamptz,
    error_message text
)
LANGUAGE sql STABLE STRICT
AS $$
    SELECT r.run_id,
           r.status          AS run_status,
           t.task_name,
           tr.status         AS task_status,
           tr.attempt,
           tr.started_at,
           tr.completed_at,
           tr.error_message
    FROM   alohadb_pipeline_runs        r
    JOIN   alohadb_pipeline_task_runs   tr ON tr.run_id    = r.run_id
    JOIN   alohadb_pipeline_tasks       t  ON t.task_id    = tr.task_id
    WHERE  r.run_id = p_run_id
    ORDER BY t.task_id;
$$;

-- List all pipelines
CREATE FUNCTION pipeline_list()
RETURNS TABLE(
    pipeline_name text,
    description   text,
    enabled       boolean,
    task_count    bigint,
    created_at    timestamptz
)
LANGUAGE sql STABLE
AS $$
    SELECT d.pipeline_name, d.description, d.enabled,
           (SELECT count(*) FROM alohadb_pipeline_tasks t
            WHERE t.pipeline_id = d.pipeline_id),
           d.created_at
    FROM   alohadb_pipeline_defs d
    ORDER BY d.pipeline_name;
$$;

-- Recent run history for a pipeline
CREATE FUNCTION pipeline_history(
    p_pipeline_name text,
    p_limit         int DEFAULT 20
)
RETURNS TABLE(
    run_id       bigint,
    status       text,
    triggered_by text,
    started_at   timestamptz,
    completed_at timestamptz
)
LANGUAGE sql STABLE STRICT
AS $$
    SELECT r.run_id, r.status, r.triggered_by, r.started_at, r.completed_at
    FROM   alohadb_pipeline_runs r
    JOIN   alohadb_pipeline_defs d ON d.pipeline_id = r.pipeline_id
    WHERE  d.pipeline_name = p_pipeline_name
    ORDER BY r.run_id DESC
    LIMIT  p_limit;
$$;

-- Internal: mark a task_run ready when all its dependencies have completed
-- Called by the background worker after each task finishes.
CREATE FUNCTION pipeline_advance_ready(p_run_id bigint)
RETURNS int  -- number of tasks newly marked ready
LANGUAGE plpgsql VOLATILE STRICT
AS $$
DECLARE
    newly_ready int;
BEGIN
    WITH deps_met AS (
        -- A task is ready when every dependency task_run has status matching dep_type
        SELECT tr.task_run_id
        FROM   alohadb_pipeline_task_runs tr
        WHERE  tr.run_id = p_run_id
          AND  tr.status = 'pending'
          AND  NOT EXISTS (
                   -- any unsatisfied dependency
                   SELECT 1
                   FROM   alohadb_pipeline_task_deps d
                   JOIN   alohadb_pipeline_task_runs dep_tr
                          ON  dep_tr.run_id  = p_run_id
                          AND dep_tr.task_id = d.depends_on_task_id
                   WHERE  d.task_id = tr.task_id
                     AND  NOT (
                              (d.dep_type = 'success'  AND dep_tr.status = 'succeeded') OR
                              (d.dep_type = 'failure'  AND dep_tr.status = 'failed')    OR
                              (d.dep_type = 'any'      AND dep_tr.status IN ('succeeded','failed'))
                          )
               )
    )
    UPDATE alohadb_pipeline_task_runs
    SET    status = 'ready'
    WHERE  task_run_id IN (SELECT task_run_id FROM deps_met);

    GET DIAGNOSTICS newly_ready = ROW_COUNT;
    RETURN newly_ready;
END;
$$;
