/* contrib/test_simulation/test_simulation--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_simulation" to load this file. \quit

-- Run a specific simulation scenario with given parameters
CREATE FUNCTION sim_run_scenario(
    scenario text,
    seed int8 DEFAULT 42,
    fault_probability float8 DEFAULT 0.1,
    max_operations int4 DEFAULT 100
)
RETURNS TABLE(passed bool, operations int4, faults_injected int4,
              faults_detected int4, faults_missed int4)
AS 'MODULE_PATHNAME', 'sim_run_scenario'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION sim_run_scenario(text, int8, float8, int4) IS
'Run a simulation scenario. Valid scenarios: checksum_detection, wal_corruption, oom_resilience';

-- Run all simulation scenarios with a given seed
CREATE FUNCTION sim_run_all(seed int8 DEFAULT 42)
RETURNS TABLE(scenario text, passed bool)
AS 'MODULE_PATHNAME', 'sim_run_all'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION sim_run_all(int8) IS
'Run all simulation scenarios with deterministic seed for reproducible testing';
