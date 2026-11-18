-- pg_python regression test (requires plpython3u)

CREATE EXTENSION pg_python CASCADE;

-- README examples: `return` style, args dict and per-key variables
SELECT python_eval('return {"sum": args["a"] + args["b"]}', '{"a":2,"b":3}');
SELECT python_register('normalize', 'return value.strip().lower()');
SELECT python_exec('normalize', '{"value":"  Hello "}');

-- Module style with _result, and the raw JSON string in _args
SELECT python_eval('_result = 2 + 2') AS four;
SELECT python_eval('import json; _result = json.loads(_args)["x"] + y', '{"x": 10, "y": 32}') AS answer_42;
SELECT python_eval('_result = [x**2 for x in range(5)]') AS squares;
SELECT python_eval('pass') AS none_is_null;
SELECT python_eval('return None') AS json_null;
-- Keys that are not identifiers or are reserved stay only in args
SELECT python_eval('return [args["not an id"], args["args"]]', '{"not an id": 1, "args": 2}');
-- Helper definitions and imports alongside return
SELECT python_eval($py$
import math
def hyp(a, b):
    return math.hypot(a, b)
return hyp(a, b)
$py$, '{"a":3,"b":4}') AS five;

-- Errors surface with a Python traceback
SELECT python_eval('return 1/0');
SELECT python_eval('def (');

-- Versioned registry
SELECT python_register('greet', 'return f"Hello, {name}!"', 'v1 greeter') AS v;
SELECT python_register('greet', 'return f"Aloha, {name}!"', 'v2 greeter') AS v;
SELECT python_exec('greet', '{"name":"AlohaDB"}');
SELECT python_exec('greet', '{"name":"AlohaDB"}', 1);
SELECT python_exec('missing');
SELECT script_name, version, description FROM python_list() ORDER BY 1;
SELECT python_get('greet'), python_get('greet', 1);

-- Execution log and stats (failed calls roll back with the error)
SELECT script_name, script_version, result FROM python_history('greet') ORDER BY script_version;
SELECT registered_scripts, total_executions, failed_executions FROM python_stats();

SELECT python_env_register('ml', ARRAY['numpy'], '3.12', 'ML deps');
SELECT env_name, packages FROM pg_python_envs ORDER BY 1;

-- Registry data is included in dumps
SELECT c.relname
FROM   pg_extension e, unnest(e.extconfig) AS cfg(relid)
JOIN   pg_class c ON c.oid = cfg.relid
WHERE  e.extname = 'pg_python'
ORDER BY 1;

DROP EXTENSION pg_python;
