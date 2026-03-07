/* contrib/alohadb_learned_stats/learned_stats--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_learned_stats" to load this file. \quit

-- alohadb_learned_stats_status()
--   Returns current status of the learned cardinality estimation model.
CREATE FUNCTION alohadb_learned_stats_status(
    OUT num_samples int,
    OUT model_trained bool,
    OUT last_train_time timestamptz
)
RETURNS record
AS 'MODULE_PATHNAME', 'alohadb_learned_stats_status'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION alohadb_learned_stats_status() IS
    'Returns status information about the ML cardinality estimation model: '
    'number of collected samples, whether the model is trained, and last training time.';

-- alohadb_learned_stats_train()
--   Manually trigger model training on accumulated query feedback data.
CREATE FUNCTION alohadb_learned_stats_train()
RETURNS void
AS 'MODULE_PATHNAME', 'alohadb_learned_stats_train'
LANGUAGE C STRICT VOLATILE;

COMMENT ON FUNCTION alohadb_learned_stats_train() IS
    'Manually triggers retraining of the ML cardinality estimation model '
    'on accumulated query execution feedback data.';

-- alohadb_learned_stats_reset()
--   Clear all training data and reset the model to untrained state.
CREATE FUNCTION alohadb_learned_stats_reset()
RETURNS void
AS 'MODULE_PATHNAME', 'alohadb_learned_stats_reset'
LANGUAGE C STRICT VOLATILE;

COMMENT ON FUNCTION alohadb_learned_stats_reset() IS
    'Clears all collected training data and resets the ML cardinality '
    'estimation model to its initial untrained state.';
