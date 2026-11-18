\echo Use "CREATE EXTENSION pg_model_registry" to load this file. \quit

-- -----------------------------------------------------------------------
-- pg_model_registry: ML model versioning, deployment, experiment tracking
--
-- Integrates with pg_inference (loads ONNX models on deploy).
-- Uses standard PostgreSQL versioned tables; no proprietary MLflow or
-- Databricks-specific algorithms.
-- -----------------------------------------------------------------------

-- Model catalog
CREATE TABLE pg_model_registry_models (
    model_id        bigserial   PRIMARY KEY,
    model_name      text        NOT NULL UNIQUE,
    framework       text        NOT NULL DEFAULT 'onnx',  -- 'onnx' | 'sklearn' | 'pytorch' | etc.
    description     text,
    tags            jsonb       NOT NULL DEFAULT '{}',
    created_by      text        NOT NULL DEFAULT current_user,
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- Model versions (immutable once created)
CREATE TABLE pg_model_registry_versions (
    version_id      bigserial   PRIMARY KEY,
    model_id        bigint      NOT NULL REFERENCES pg_model_registry_models ON DELETE CASCADE,
    version_num     int         NOT NULL,
    model_data      bytea       NOT NULL,   -- serialized model (e.g. ONNX bytes)
    input_schema    jsonb,                  -- {name: type, ...} for validation
    output_schema   jsonb,
    metrics         jsonb       NOT NULL DEFAULT '{}',  -- {accuracy: 0.95, loss: 0.05, ...}
    params          jsonb       NOT NULL DEFAULT '{}',  -- hyperparameters
    artifact_uri    text,                   -- optional external path (S3, etc.)
    stage           text        NOT NULL DEFAULT 'staging',
        -- 'staging' | 'production' | 'archived'
    created_by      text        NOT NULL DEFAULT current_user,
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (model_id, version_num)
);

CREATE INDEX pg_model_versions_model_idx
    ON pg_model_registry_versions (model_id, stage);

-- Deployment log
CREATE TABLE pg_model_registry_deployments (
    deploy_id       bigserial   PRIMARY KEY,
    model_id        bigint      NOT NULL REFERENCES pg_model_registry_models,
    version_id      bigint      NOT NULL REFERENCES pg_model_registry_versions,
    action          text        NOT NULL,   -- 'deploy' | 'rollback' | 'archive'
    performed_by    text        NOT NULL DEFAULT current_user,
    performed_at    timestamptz NOT NULL DEFAULT now(),
    notes           text
);

-- Experiment runs (training metadata)
CREATE TABLE pg_model_registry_experiments (
    exp_id          bigserial   PRIMARY KEY,
    model_id        bigint      NOT NULL REFERENCES pg_model_registry_models,
    run_name        text        NOT NULL,
    params          jsonb       NOT NULL DEFAULT '{}',
    metrics         jsonb       NOT NULL DEFAULT '{}',
    status          text        NOT NULL DEFAULT 'running',  -- 'running' | 'finished' | 'failed'
    started_at      timestamptz NOT NULL DEFAULT now(),
    ended_at        timestamptz,
    created_by      text        NOT NULL DEFAULT current_user
);

CREATE INDEX pg_model_experiments_model_idx
    ON pg_model_registry_experiments (model_id);

-- -----------------------------------------------------------------------
-- Model management API
-- -----------------------------------------------------------------------

CREATE FUNCTION model_register(
    p_name        text,
    p_framework   text   DEFAULT 'onnx',
    p_description text   DEFAULT NULL,
    p_tags        jsonb  DEFAULT '{}'
)
RETURNS bigint
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE v_id bigint;
BEGIN
    INSERT INTO pg_model_registry_models
        (model_name, framework, description, tags)
    VALUES
        (p_name, p_framework, p_description, p_tags)
    ON CONFLICT (model_name)
    DO UPDATE SET description = EXCLUDED.description, tags = EXCLUDED.tags
    RETURNING model_id INTO v_id;
    RETURN v_id;
END;
$$;

-- Add a new version (always staged; use model_deploy to promote)
CREATE FUNCTION model_version_add(
    p_model_name    text,
    p_model_data    bytea,
    p_metrics       jsonb  DEFAULT '{}',
    p_params        jsonb  DEFAULT '{}',
    p_input_schema  jsonb  DEFAULT NULL,
    p_output_schema jsonb  DEFAULT NULL,
    p_artifact_uri  text   DEFAULT NULL
)
RETURNS int  -- version number
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    v_model_id   bigint;
    v_version    int;
BEGIN
    SELECT model_id INTO v_model_id
    FROM   pg_model_registry_models
    WHERE  model_name = p_model_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'model_version_add: model "%" not found', p_model_name;
    END IF;

    SELECT COALESCE(MAX(version_num), 0) + 1 INTO v_version
    FROM   pg_model_registry_versions WHERE model_id = v_model_id;

    INSERT INTO pg_model_registry_versions
        (model_id, version_num, model_data, metrics, params,
         input_schema, output_schema, artifact_uri)
    VALUES
        (v_model_id, v_version, p_model_data, p_metrics, p_params,
         p_input_schema, p_output_schema, p_artifact_uri);

    RETURN v_version;
END;
$$;

-- Promote a version to production (archives the current production version)
CREATE FUNCTION model_deploy(
    p_model_name text,
    p_version    int  DEFAULT NULL  -- NULL = latest staged
)
RETURNS void
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    v_model_id    bigint;
    v_version_id  bigint;
    v_prev_id     bigint;
BEGIN
    SELECT model_id INTO v_model_id
    FROM   pg_model_registry_models WHERE model_name = p_model_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'model_deploy: model "%" not found', p_model_name;
    END IF;

    -- Find target version
    SELECT version_id INTO v_version_id
    FROM   pg_model_registry_versions
    WHERE  model_id = v_model_id
      AND  (p_version IS NULL OR version_num = p_version)
      AND  stage IN ('staging', 'production')
    ORDER BY version_num DESC
    LIMIT  1;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'model_deploy: no deployable version found for "%"', p_model_name;
    END IF;

    -- Archive current production (if any)
    SELECT version_id INTO v_prev_id
    FROM   pg_model_registry_versions
    WHERE  model_id = v_model_id AND stage = 'production';

    IF v_prev_id IS NOT NULL AND v_prev_id <> v_version_id THEN
        UPDATE pg_model_registry_versions
        SET    stage = 'archived'
        WHERE  version_id = v_prev_id;

        INSERT INTO pg_model_registry_deployments
            (model_id, version_id, action, notes)
        VALUES
            (v_model_id, v_prev_id, 'archive', 'superseded by deploy');
    END IF;

    -- Promote
    UPDATE pg_model_registry_versions
    SET    stage = 'production'
    WHERE  version_id = v_version_id;

    INSERT INTO pg_model_registry_deployments
        (model_id, version_id, action)
    VALUES
        (v_model_id, v_version_id, 'deploy');

    -- Load into pg_inference cache (if extension is installed)
    BEGIN
        PERFORM pg_load_model(p_model_name,
            (SELECT model_data FROM pg_model_registry_versions
             WHERE version_id = v_version_id));
    EXCEPTION WHEN undefined_function THEN
        NULL;  -- pg_inference not installed; skip
    END;
END;
$$;

-- Roll back to the most-recently-archived version
CREATE FUNCTION model_rollback(p_model_name text)
RETURNS int  -- version number restored
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    v_model_id    bigint;
    v_current_id  bigint;
    v_prev_id     bigint;
    v_prev_version int;
BEGIN
    SELECT model_id INTO v_model_id
    FROM   pg_model_registry_models WHERE model_name = p_model_name;

    SELECT version_id INTO v_current_id
    FROM   pg_model_registry_versions
    WHERE  model_id = v_model_id AND stage = 'production';

    -- Most recently archived version before current
    SELECT version_id, version_num INTO v_prev_id, v_prev_version
    FROM   pg_model_registry_versions
    WHERE  model_id = v_model_id AND stage = 'archived'
    ORDER BY version_num DESC
    LIMIT  1;

    IF v_prev_id IS NULL THEN
        RAISE EXCEPTION 'model_rollback: no archived version to roll back to for "%"',
                        p_model_name;
    END IF;

    IF v_current_id IS NOT NULL THEN
        UPDATE pg_model_registry_versions SET stage = 'archived'
        WHERE  version_id = v_current_id;
        INSERT INTO pg_model_registry_deployments
            (model_id, version_id, action, notes)
        VALUES (v_model_id, v_current_id, 'archive', 'rolled back');
    END IF;

    UPDATE pg_model_registry_versions SET stage = 'production'
    WHERE  version_id = v_prev_id;

    INSERT INTO pg_model_registry_deployments
        (model_id, version_id, action, notes)
    VALUES (v_model_id, v_prev_id, 'rollback', 'restored from archive');

    RETURN v_prev_version;
END;
$$;

-- Get the active (production) version of a model
CREATE FUNCTION model_get_active(p_model_name text)
RETURNS TABLE(
    version_num     int,
    metrics         jsonb,
    params          jsonb,
    input_schema    jsonb,
    output_schema   jsonb,
    created_at      timestamptz
)
LANGUAGE sql STABLE STRICT
AS $$
    SELECT v.version_num, v.metrics, v.params,
           v.input_schema, v.output_schema, v.created_at
    FROM   pg_model_registry_versions v
    JOIN   pg_model_registry_models   m ON m.model_id = v.model_id
    WHERE  m.model_name = p_model_name
      AND  v.stage = 'production';
$$;

-- List all models
CREATE FUNCTION model_list()
RETURNS TABLE(
    model_name      text,
    framework       text,
    active_version  int,
    total_versions  bigint,
    tags            jsonb,
    created_at      timestamptz
)
LANGUAGE sql STABLE
AS $$
    SELECT m.model_name,
           m.framework,
           (SELECT version_num FROM pg_model_registry_versions
            WHERE model_id = m.model_id AND stage = 'production'),
           (SELECT count(*) FROM pg_model_registry_versions
            WHERE model_id = m.model_id),
           m.tags,
           m.created_at
    FROM   pg_model_registry_models m
    ORDER BY m.model_name;
$$;

-- List versions of a model
CREATE FUNCTION model_version_list(p_model_name text)
RETURNS TABLE(
    version_num int,
    stage       text,
    metrics     jsonb,
    params      jsonb,
    created_at  timestamptz
)
LANGUAGE sql STABLE STRICT
AS $$
    SELECT v.version_num, v.stage, v.metrics, v.params, v.created_at
    FROM   pg_model_registry_versions v
    JOIN   pg_model_registry_models   m ON m.model_id = v.model_id
    WHERE  m.model_name = p_model_name
    ORDER BY v.version_num;
$$;

-- Compare metrics of two versions
CREATE FUNCTION model_compare(
    p_model_name text,
    p_version_a  int,
    p_version_b  int
)
RETURNS TABLE(metric text, value_a float8, value_b float8, delta float8)
LANGUAGE sql STABLE STRICT
AS $$
    WITH va AS (
        SELECT v.metrics
        FROM   pg_model_registry_versions v
        JOIN   pg_model_registry_models   m ON m.model_id = v.model_id
        WHERE  m.model_name = p_model_name AND v.version_num = p_version_a
    ),
    vb AS (
        SELECT v.metrics
        FROM   pg_model_registry_versions v
        JOIN   pg_model_registry_models   m ON m.model_id = v.model_id
        WHERE  m.model_name = p_model_name AND v.version_num = p_version_b
    )
    SELECT k,
           (va.metrics->>k)::float8,
           (vb.metrics->>k)::float8,
           (vb.metrics->>k)::float8 - (va.metrics->>k)::float8
    FROM   va, vb,
           jsonb_object_keys(va.metrics) AS k
    WHERE  (va.metrics->>k) ~ '^-?[0-9]+(\.[0-9]+)?$'
    ORDER BY k;
$$;

-- Experiment tracking
CREATE FUNCTION experiment_start(
    p_model_name text,
    p_run_name   text,
    p_params     jsonb DEFAULT '{}'
)
RETURNS bigint
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    v_model_id bigint;
    v_exp_id   bigint;
BEGIN
    SELECT model_id INTO v_model_id
    FROM   pg_model_registry_models WHERE model_name = p_model_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'experiment_start: model "%" not found', p_model_name;
    END IF;

    INSERT INTO pg_model_registry_experiments
        (model_id, run_name, params)
    VALUES
        (v_model_id, p_run_name, p_params)
    RETURNING exp_id INTO v_exp_id;

    RETURN v_exp_id;
END;
$$;

CREATE FUNCTION experiment_end(
    p_exp_id  bigint,
    p_metrics jsonb  DEFAULT '{}',
    p_status  text   DEFAULT 'finished'
)
RETURNS void
LANGUAGE plpgsql VOLATILE STRICT
AS $$
BEGIN
    UPDATE pg_model_registry_experiments
    SET    metrics  = p_metrics,
           status   = p_status,
           ended_at = now()
    WHERE  exp_id = p_exp_id;
END;
$$;

CREATE FUNCTION experiment_list(p_model_name text)
RETURNS TABLE(
    exp_id     bigint,
    run_name   text,
    status     text,
    params     jsonb,
    metrics    jsonb,
    started_at timestamptz,
    ended_at   timestamptz
)
LANGUAGE sql STABLE STRICT
AS $$
    SELECT e.exp_id, e.run_name, e.status, e.params, e.metrics,
           e.started_at, e.ended_at
    FROM   pg_model_registry_experiments e
    JOIN   pg_model_registry_models      m ON m.model_id = e.model_id
    WHERE  m.model_name = p_model_name
    ORDER BY e.exp_id DESC;
$$;
