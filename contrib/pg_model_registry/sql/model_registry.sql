-- pg_model_registry regression test
-- Tests: model registration, versioning, deployment, rollback, experiments

CREATE EXTENSION pg_model_registry;

-- -----------------------------------------------------------------------
-- Model registration
-- -----------------------------------------------------------------------

SELECT model_register('sentiment_v1', 'onnx', 'Sentiment analysis model', '{"team":"nlp"}'::jsonb) AS model_id;
SELECT model_register('fraud_detector', 'sklearn', 'Fraud detection model', '{"team":"risk"}'::jsonb) AS model_id;
-- Upsert: same name updates description
SELECT model_register('sentiment_v1', 'onnx', 'Updated description', '{}'::jsonb) AS model_id;

SELECT model_name, framework, tags
FROM   pg_model_registry_models
ORDER BY model_name;

-- -----------------------------------------------------------------------
-- Version management
-- -----------------------------------------------------------------------

-- Add versions (model_data is the serialized model bytes)
SELECT model_version_add(
    'sentiment_v1',
    '\x000102030405'::bytea,    -- placeholder ONNX bytes
    '{"accuracy": 0.87, "f1": 0.85}'::jsonb,
    '{"learning_rate": 0.001, "epochs": 10}'::jsonb
) AS version_num;

SELECT model_version_add(
    'sentiment_v1',
    '\x000102030406'::bytea,
    '{"accuracy": 0.91, "f1": 0.89}'::jsonb,
    '{"learning_rate": 0.0005, "epochs": 20}'::jsonb
) AS version_num;

SELECT version_num, stage, metrics
FROM   model_version_list('sentiment_v1');

-- -----------------------------------------------------------------------
-- Deploy (promote to production)
-- -----------------------------------------------------------------------

SELECT model_deploy('sentiment_v1', 1);

SELECT version_num, stage
FROM   model_version_list('sentiment_v1')
ORDER BY version_num;

-- Deploy version 2 (should archive version 1)
SELECT model_deploy('sentiment_v1', 2);

SELECT version_num, stage
FROM   model_version_list('sentiment_v1')
ORDER BY version_num;

-- Active version
SELECT version_num, metrics
FROM   model_get_active('sentiment_v1');

-- -----------------------------------------------------------------------
-- Rollback
-- -----------------------------------------------------------------------

SELECT model_rollback('sentiment_v1') AS rolled_back_to;

SELECT version_num, stage
FROM   model_version_list('sentiment_v1')
ORDER BY version_num;

-- -----------------------------------------------------------------------
-- Compare versions
-- -----------------------------------------------------------------------

SELECT metric, round(value_a::numeric, 3), round(value_b::numeric, 3), round(delta::numeric, 3)
FROM   model_compare('sentiment_v1', 1, 2)
ORDER BY metric;

-- -----------------------------------------------------------------------
-- Deployment log
-- -----------------------------------------------------------------------

SELECT action
FROM   pg_model_registry_deployments d
JOIN   pg_model_registry_models      m ON m.model_id = d.model_id
WHERE  m.model_name = 'sentiment_v1'
ORDER BY deploy_id;

-- -----------------------------------------------------------------------
-- Experiment tracking
-- -----------------------------------------------------------------------

DECLARE
DO $$
DECLARE
    v_exp_id bigint;
BEGIN
    SELECT experiment_start('sentiment_v1', 'run_20250101', '{"batch_size": 32}'::jsonb) INTO v_exp_id;
    PERFORM experiment_end(v_exp_id, '{"accuracy": 0.93, "loss": 0.12}'::jsonb, 'finished');
END $$;

SELECT run_name, status, metrics->>'accuracy' AS accuracy
FROM   experiment_list('sentiment_v1');

-- -----------------------------------------------------------------------
-- List all models
-- -----------------------------------------------------------------------

SELECT model_name, framework, active_version, total_versions
FROM   model_list()
ORDER BY model_name;

-- -----------------------------------------------------------------------
-- Cleanup
-- -----------------------------------------------------------------------

DELETE FROM pg_model_registry_models;
DROP EXTENSION pg_model_registry;
