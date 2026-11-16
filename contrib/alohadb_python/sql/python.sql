-- alohadb_python regression test
-- Tests: python_eval, script registration, execution, history, stats
-- Requires: PostgreSQL built with PL/Python3 support (plpython3u)

CREATE EXTENSION alohadb_python;

-- -----------------------------------------------------------------------
-- python_eval: inline code execution
-- -----------------------------------------------------------------------

-- Arithmetic
SELECT python_eval('_result = 2 + 2') AS four;
SELECT python_eval('_result = str(3.14159)') AS pi_str;

-- String manipulation
SELECT python_eval('_result = "hello world".upper()') AS upper;
SELECT python_eval('_result = ",".join(["a","b","c"])') AS joined;

-- List comprehension
SELECT python_eval('_result = str([x**2 for x in range(5)])') AS squares;

-- Using args_json via python_eval (args available as _args)
SELECT python_eval(
    '_args_data = __import__("json").loads(_args); _result = str(_args_data["x"] + _args_data["y"])',
    '{"x": 10, "y": 32}'
) AS answer_42;

-- -----------------------------------------------------------------------
-- Script registration
-- -----------------------------------------------------------------------

SELECT python_register(
    'greet',
    $py$
import json
args = json.loads(_args) if _args else {}
name = args.get("name", "World")
_result = f"Hello, {name}!"
$py$
) AS version_num;

-- Register a second version
SELECT python_register(
    'greet',
    $py$
import json
args = json.loads(_args) if _args else {}
name = args.get("name", "World")
_result = f"Greetings, {name}! (v2)"
$py$
) AS version_num;

SELECT python_register(
    'sum_list',
    $py$
import json
args = json.loads(_args) if _args else {}
nums = args.get("numbers", [])
_result = str(sum(nums))
$py$
) AS version_num;

-- -----------------------------------------------------------------------
-- List and get scripts
-- -----------------------------------------------------------------------

SELECT script_name, version_num
FROM   python_list()
ORDER BY script_name, version_num;

SELECT script_name, version_num, length(code) AS code_len
FROM   python_get('greet');

-- -----------------------------------------------------------------------
-- Execute scripts
-- -----------------------------------------------------------------------

SELECT python_exec('greet',    '{"name": "AlohaDB"}') AS greeting;
SELECT python_exec('sum_list', '{"numbers": [1, 2, 3, 4, 5]}') AS total;

-- -----------------------------------------------------------------------
-- Execution history
-- -----------------------------------------------------------------------

SELECT script_name, count(*) AS executions
FROM   python_history('greet')
GROUP BY script_name;

-- -----------------------------------------------------------------------
-- Statistics
-- -----------------------------------------------------------------------

SELECT total_scripts, total_executions
FROM   python_stats();

-- -----------------------------------------------------------------------
-- Cleanup
-- -----------------------------------------------------------------------

TRUNCATE alohadb_python_scripts, alohadb_python_executions CASCADE;
DROP EXTENSION alohadb_python;
