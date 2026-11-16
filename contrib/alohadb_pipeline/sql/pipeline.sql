-- alohadb_pipeline regression test
-- Tests: pipeline creation, task dependency graph, run tracking, status

CREATE EXTENSION alohadb_pipeline;

-- -----------------------------------------------------------------------
-- Pipeline creation
-- -----------------------------------------------------------------------

SELECT pipeline_create(
    'etl_daily',
    'Daily ETL pipeline'
);

SELECT pipeline_create(
    'etl_hourly',
    'Hourly aggregation pipeline'
);

SELECT pipeline_name, description
FROM   pipeline_defs
ORDER BY pipeline_name;

-- -----------------------------------------------------------------------
-- Task registration (Kahn's topological sort applies during execution)
-- -----------------------------------------------------------------------

-- Pipeline 1: extract -> transform -> load (linear chain)
SELECT pipeline_task_add('etl_daily', 'extract',   'SELECT 1', 1);
SELECT pipeline_task_add('etl_daily', 'transform', 'SELECT 2', 2);
SELECT pipeline_task_add('etl_daily', 'load',      'SELECT 3', 3);

-- Dependencies: transform depends on extract, load depends on transform
SELECT pipeline_task_depend('etl_daily', 'transform', 'extract');
SELECT pipeline_task_depend('etl_daily', 'load',      'transform');

-- Pipeline 2: two parallel tasks then a merge
SELECT pipeline_task_add('etl_hourly', 'fetch_a', 'SELECT 10', 1);
SELECT pipeline_task_add('etl_hourly', 'fetch_b', 'SELECT 20', 1);
SELECT pipeline_task_add('etl_hourly', 'merge',   'SELECT 30', 2);
SELECT pipeline_task_depend('etl_hourly', 'merge', 'fetch_a');
SELECT pipeline_task_depend('etl_hourly', 'merge', 'fetch_b');

-- -----------------------------------------------------------------------
-- List tasks
-- -----------------------------------------------------------------------

SELECT t.task_name, t.command, t.step_order
FROM   pipeline_tasks t
JOIN   pipeline_defs  d ON d.pipeline_id = t.pipeline_id
WHERE  d.pipeline_name = 'etl_daily'
ORDER BY t.step_order;

-- -----------------------------------------------------------------------
-- Pipeline runs
-- -----------------------------------------------------------------------

SELECT pipeline_run('etl_daily') AS run_id_1;
SELECT pipeline_run('etl_hourly') AS run_id_2;

-- Initial task_run state: root tasks are 'ready', rest are 'waiting'
SELECT t.task_name, tr.status
FROM   pipeline_task_runs  tr
JOIN   pipeline_runs       pr ON pr.run_id = tr.run_id
JOIN   pipeline_defs       d  ON d.pipeline_id = pr.pipeline_id
JOIN   pipeline_tasks      t  ON t.task_id = tr.task_id
WHERE  d.pipeline_name = 'etl_daily'
ORDER BY t.step_order;

-- -----------------------------------------------------------------------
-- Status view
-- -----------------------------------------------------------------------

SELECT pipeline_name, status, total_tasks, completed_tasks
FROM   pipeline_status('etl_daily');

SELECT pipeline_name, status, total_tasks, completed_tasks
FROM   pipeline_status('etl_hourly');

-- -----------------------------------------------------------------------
-- pipeline_list
-- -----------------------------------------------------------------------

SELECT pipeline_name, description
FROM   pipeline_list()
ORDER BY pipeline_name;

-- -----------------------------------------------------------------------
-- Cancel a run
-- -----------------------------------------------------------------------

DO $$
DECLARE v_run_id bigint;
BEGIN
    SELECT pipeline_run('etl_daily') INTO v_run_id;
    PERFORM pipeline_cancel(v_run_id);
END $$;

-- -----------------------------------------------------------------------
-- Cleanup
-- -----------------------------------------------------------------------

DELETE FROM pipeline_defs WHERE pipeline_name IN ('etl_daily', 'etl_hourly');
DROP EXTENSION alohadb_pipeline;
