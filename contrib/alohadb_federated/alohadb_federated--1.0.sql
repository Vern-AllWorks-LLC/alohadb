/* contrib/alohadb_federated/alohadb_federated--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_federated" to load this file. \quit

-- =====================================================================
-- Federated Learning Extension (Phase 6.5)
--
-- Enables federated learning where data stays in the database and only
-- model gradient updates are shared with a Flower aggregation server.
-- =====================================================================

--
-- alohadb_fl_start() RETURNS void
--
-- Starts the federated learning background worker.  The worker connects
-- to the configured Flower server, fetches global model weights, trains
-- on local data using the configured training query, and sends gradient
-- updates back.  Data never leaves the database.
--
-- Prerequisites:
--   SET alohadb.flower_server = 'http://localhost:8080';
--   SET alohadb.fl_training_query = 'SELECT f1::float8, f2::float8, label::float8 FROM training_data';
--
CREATE FUNCTION alohadb_fl_start()
RETURNS void
AS 'MODULE_PATHNAME', 'alohadb_fl_start'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION alohadb_fl_start() IS
    'Start the federated learning background worker';

--
-- alohadb_fl_stop() RETURNS void
--
-- Stops the federated learning background worker.  Sends a shutdown
-- signal and waits for the worker to exit cleanly.
--
CREATE FUNCTION alohadb_fl_stop()
RETURNS void
AS 'MODULE_PATHNAME', 'alohadb_fl_stop'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION alohadb_fl_stop() IS
    'Stop the federated learning background worker';

--
-- alohadb_fl_status()
--     RETURNS TABLE(status text, server text, model text,
--                   last_round int, last_train_time timestamptz)
--
-- Returns the current status of the FL background worker, including
-- the Flower server URL, model name, last completed round number,
-- and timestamp of the last training round.
--
CREATE FUNCTION alohadb_fl_status(
    OUT status text,
    OUT server text,
    OUT model text,
    OUT last_round int,
    OUT last_train_time timestamptz
)
RETURNS record
AS 'MODULE_PATHNAME', 'alohadb_fl_status'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION alohadb_fl_status() IS
    'Query the status of the federated learning background worker';
