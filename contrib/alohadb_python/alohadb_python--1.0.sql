\echo Use "CREATE EXTENSION alohadb_python" to load this file. \quit

-- -------------------------------------------------------------------------
-- alohadb_python: Python as a first-class workload
--
-- Execution model: all Python runs inside PostgreSQL's built-in plpython3u
-- language handler (in-process, no subprocess, no new runtime).
-- Python's exec() builtin dispatches user-supplied code strings — a
-- technique with prior art since Python 1.0 (1994), well outside any
-- patent window.
-- -------------------------------------------------------------------------

-- Environment registry: records which packages a script requires.
-- This is metadata only; actual package installation is outside the DB.
CREATE TABLE alohadb_python_envs (
    env_name        text        NOT NULL PRIMARY KEY,
    python_version  text        NOT NULL DEFAULT '3',
    packages        text[]      NOT NULL DEFAULT '{}',
    description     text,
    created_at      timestamptz NOT NULL DEFAULT now()
);

INSERT INTO alohadb_python_envs (env_name, description)
VALUES ('default', 'Default Python environment');

-- Script catalog: stores Python scripts as versioned database objects.
-- Each INSERT via python_register() bumps the version; all versions are kept.
CREATE TABLE alohadb_python_scripts (
    script_name     text        NOT NULL,
    version         int         NOT NULL DEFAULT 1,
    code            text        NOT NULL,
    description     text,
    env_name        text        NOT NULL DEFAULT 'default'
                                REFERENCES alohadb_python_envs(env_name),
    return_type     text        NOT NULL DEFAULT 'jsonb',
    created_by      text        NOT NULL DEFAULT current_user,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (script_name, version)
);

CREATE INDEX alohadb_python_scripts_name_idx
    ON alohadb_python_scripts (script_name);

-- Execution log: every python_eval() / python_exec() call is recorded.
CREATE TABLE alohadb_python_executions (
    exec_id         bigserial   PRIMARY KEY,
    script_name     text,           -- NULL for ad-hoc python_eval calls
    script_version  int,
    exec_mode       text        NOT NULL DEFAULT 'sync',
    args            jsonb,
    result          jsonb,
    error_message   text,
    duration_ms     float8,
    executed_by     text        NOT NULL DEFAULT current_user,
    started_at      timestamptz NOT NULL DEFAULT now(),
    completed_at    timestamptz
);

CREATE INDEX alohadb_python_executions_script_idx
    ON alohadb_python_executions (script_name);
CREATE INDEX alohadb_python_executions_time_idx
    ON alohadb_python_executions (started_at DESC);

-- -------------------------------------------------------------------------
-- Core execution engine: a single plpython3u function that runs arbitrary
-- Python code passed as a string using Python's built-in exec().
--
-- Contract for user code:
--   - The variable `args` is a Python dict populated from the jsonb argument.
--   - User code must assign its output to `_result` (any JSON-serialisable
--     Python value, or None).
--   - import statements and helper definitions are allowed anywhere.
--
-- Patent-mitigation note:
--   Using exec() to run dynamic code strings is a Python core-language
--   feature (Python 1.0, 1994). Delegating to plpython3u avoids any new
--   Python runtime; plpython3u has shipped with PostgreSQL since 8.1 (2005).
--   No Databricks/MLflow/notebook patents apply to this single-node,
--   in-process, SQL-managed execution model.
-- -------------------------------------------------------------------------
CREATE FUNCTION alohadb_python_runner(p_code text, p_args_json text)
RETURNS text
LANGUAGE plpython3u
AS $$
import json
import traceback

args = json.loads(p_args_json) if p_args_json else {}

_exec_globals = {
    '__builtins__': __builtins__,
    'args': args,
    '_result': None,
}

try:
    exec(compile(p_code, '<alohadb_python>', 'exec'), _exec_globals)
    return json.dumps(_exec_globals.get('_result'))
except Exception:
    raise Exception(traceback.format_exc())
$$;

-- -------------------------------------------------------------------------
-- python_eval: evaluate ad-hoc Python code with execution logging.
-- -------------------------------------------------------------------------
CREATE FUNCTION python_eval(
    p_code text,
    p_args jsonb DEFAULT '{}'
)
RETURNS jsonb
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    raw_result  text;
    result      jsonb;
    started     timestamptz := clock_timestamp();
    exec_id     bigint;
BEGIN
    INSERT INTO alohadb_python_executions (exec_mode, args)
    VALUES ('sync', p_args)
    RETURNING alohadb_python_executions.exec_id INTO exec_id;

    BEGIN
        raw_result := alohadb_python_runner(p_code, p_args::text);
        result     := raw_result::jsonb;

        UPDATE alohadb_python_executions
        SET result       = result,
            completed_at = clock_timestamp(),
            duration_ms  = extract(epoch FROM clock_timestamp() - started) * 1000
        WHERE alohadb_python_executions.exec_id = exec_id;

    EXCEPTION WHEN OTHERS THEN
        UPDATE alohadb_python_executions
        SET error_message = SQLERRM,
            completed_at  = clock_timestamp(),
            duration_ms   = extract(epoch FROM clock_timestamp() - started) * 1000
        WHERE alohadb_python_executions.exec_id = exec_id;
        RAISE;
    END;

    RETURN result;
END;
$$;

-- -------------------------------------------------------------------------
-- python_register: add a script to the catalog (auto-increments version).
-- -------------------------------------------------------------------------
CREATE FUNCTION python_register(
    p_script_name   text,
    p_code          text,
    p_description   text   DEFAULT NULL,
    p_env_name      text   DEFAULT 'default',
    p_return_type   text   DEFAULT 'jsonb'
)
RETURNS int  -- version number assigned
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    next_version int;
BEGIN
    SELECT COALESCE(MAX(version), 0) + 1
    INTO   next_version
    FROM   alohadb_python_scripts
    WHERE  script_name = p_script_name;

    INSERT INTO alohadb_python_scripts
        (script_name, version, code, description, env_name, return_type)
    VALUES
        (p_script_name, next_version, p_code, p_description,
         p_env_name, p_return_type);

    RETURN next_version;
END;
$$;

-- -------------------------------------------------------------------------
-- python_exec: run a registered script by name (latest version by default).
-- -------------------------------------------------------------------------
CREATE FUNCTION python_exec(
    p_script_name text,
    p_args        jsonb DEFAULT '{}',
    p_version     int   DEFAULT NULL   -- NULL = latest
)
RETURNS jsonb
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    script_code text;
    script_ver  int;
    raw_result  text;
    result      jsonb;
    started     timestamptz := clock_timestamp();
    exec_id     bigint;
BEGIN
    SELECT code, version
    INTO   script_code, script_ver
    FROM   alohadb_python_scripts
    WHERE  script_name = p_script_name
      AND  (p_version IS NULL OR version = p_version)
    ORDER BY version DESC
    LIMIT  1;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'alohadb_python: script "%" not found', p_script_name;
    END IF;

    INSERT INTO alohadb_python_executions
        (script_name, script_version, exec_mode, args)
    VALUES
        (p_script_name, script_ver, 'sync', p_args)
    RETURNING alohadb_python_executions.exec_id INTO exec_id;

    BEGIN
        raw_result := alohadb_python_runner(script_code, p_args::text);
        result     := raw_result::jsonb;

        UPDATE alohadb_python_executions
        SET result       = result,
            completed_at = clock_timestamp(),
            duration_ms  = extract(epoch FROM clock_timestamp() - started) * 1000
        WHERE alohadb_python_executions.exec_id = exec_id;

    EXCEPTION WHEN OTHERS THEN
        UPDATE alohadb_python_executions
        SET error_message = SQLERRM,
            completed_at  = clock_timestamp(),
            duration_ms   = extract(epoch FROM clock_timestamp() - started) * 1000
        WHERE alohadb_python_executions.exec_id = exec_id;
        RAISE;
    END;

    RETURN result;
END;
$$;

-- Register a Python environment profile (metadata; no package installation)
CREATE FUNCTION python_env_register(
    p_env_name       text,
    p_packages       text[]  DEFAULT '{}',
    p_python_version text    DEFAULT '3',
    p_description    text    DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql VOLATILE
AS $$
BEGIN
    INSERT INTO alohadb_python_envs
        (env_name, python_version, packages, description)
    VALUES
        (p_env_name, p_python_version, p_packages, p_description)
    ON CONFLICT (env_name)
    DO UPDATE SET
        python_version = EXCLUDED.python_version,
        packages       = EXCLUDED.packages,
        description    = EXCLUDED.description;
END;
$$;

-- List all registered scripts (one row per script, latest version)
CREATE FUNCTION python_list(p_env_name text DEFAULT NULL)
RETURNS TABLE(
    script_name text,
    version     int,
    description text,
    env_name    text,
    return_type text,
    created_by  text,
    updated_at  timestamptz
)
LANGUAGE sql STABLE
AS $$
    SELECT DISTINCT ON (script_name)
        script_name, version, description, env_name,
        return_type, created_by, updated_at
    FROM   alohadb_python_scripts
    WHERE  p_env_name IS NULL OR env_name = p_env_name
    ORDER BY script_name, version DESC;
$$;

-- Get the source code of a registered script
CREATE FUNCTION python_get(
    p_script_name text,
    p_version     int DEFAULT NULL
)
RETURNS text
LANGUAGE sql STABLE STRICT
AS $$
    SELECT code
    FROM   alohadb_python_scripts
    WHERE  script_name = p_script_name
      AND  (p_version IS NULL OR version = p_version)
    ORDER BY version DESC
    LIMIT  1;
$$;

-- Execution history
CREATE FUNCTION python_history(
    p_script_name text DEFAULT NULL,
    p_limit       int  DEFAULT 50
)
RETURNS TABLE(
    exec_id       bigint,
    script_name   text,
    script_version int,
    args          jsonb,
    result        jsonb,
    error_message text,
    duration_ms   float8,
    executed_by   text,
    started_at    timestamptz
)
LANGUAGE sql STABLE
AS $$
    SELECT exec_id, script_name, script_version, args, result,
           error_message, duration_ms, executed_by, started_at
    FROM   alohadb_python_executions
    WHERE  p_script_name IS NULL OR script_name = p_script_name
    ORDER BY started_at DESC
    LIMIT  p_limit;
$$;

-- Summary statistics
CREATE FUNCTION python_stats(
    OUT registered_scripts bigint,
    OUT total_executions   bigint,
    OUT failed_executions  bigint,
    OUT avg_duration_ms    float8
)
RETURNS record
LANGUAGE sql STABLE
AS $$
    SELECT
        (SELECT count(DISTINCT script_name) FROM alohadb_python_scripts),
        (SELECT count(*)  FROM alohadb_python_executions),
        (SELECT count(*)  FROM alohadb_python_executions
         WHERE  error_message IS NOT NULL),
        (SELECT avg(duration_ms) FROM alohadb_python_executions
         WHERE  duration_ms IS NOT NULL);
$$;
