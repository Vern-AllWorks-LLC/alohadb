/* contrib/alohadb_inference/alohadb_inference--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_inference" to load this file. \quit

--
-- alohadb_load_model(name text, model_data bytea) RETURNS void
--
-- Loads an ONNX model from bytea data into the in-memory model cache.
-- The model can then be referenced by name in inference calls.
--
CREATE FUNCTION alohadb_load_model(name text, model_data bytea)
RETURNS void
AS 'MODULE_PATHNAME', 'alohadb_load_model'
LANGUAGE C STRICT VOLATILE;

COMMENT ON FUNCTION alohadb_load_model(text, bytea) IS
'Load an ONNX model into the inference cache';

--
-- alohadb_unload_model(name text) RETURNS void
--
-- Removes a previously loaded model from the cache, freeing all
-- associated ONNX Runtime resources.
--
CREATE FUNCTION alohadb_unload_model(name text)
RETURNS void
AS 'MODULE_PATHNAME', 'alohadb_unload_model'
LANGUAGE C STRICT VOLATILE;

COMMENT ON FUNCTION alohadb_unload_model(text) IS
'Unload an ONNX model from the inference cache';

--
-- alohadb_infer(model_name text, input_data vector) RETURNS vector
--
-- Run single-sample inference.  The input vector is fed to the first
-- input tensor of the named model, and the first output tensor is
-- returned as a vector.
--
CREATE FUNCTION alohadb_infer(model_name text, input_data vector)
RETURNS vector
AS 'MODULE_PATHNAME', 'alohadb_infer'
LANGUAGE C STRICT VOLATILE;

COMMENT ON FUNCTION alohadb_infer(text, vector) IS
'Run inference on a single input vector using a cached ONNX model';

--
-- alohadb_batch_infer(model_name text, input_data vector[])
--     RETURNS vector[]
--
-- Run batched inference.  The input is an array of vectors (all must
-- have the same dimension).  Returns an array of output vectors, one
-- per input sample.
--
CREATE FUNCTION alohadb_batch_infer(model_name text, input_data vector[])
RETURNS vector[]
AS 'MODULE_PATHNAME', 'alohadb_batch_infer'
LANGUAGE C STRICT VOLATILE;

COMMENT ON FUNCTION alohadb_batch_infer(text, vector[]) IS
'Run batched inference on an array of input vectors using a cached ONNX model';

--
-- alohadb_list_models()
--     RETURNS TABLE(name text, loaded_at timestamptz,
--                   input_shape text, output_shape text)
--
-- Returns a row for each currently loaded model in the cache.
--
CREATE FUNCTION alohadb_list_models(
    OUT name text,
    OUT loaded_at timestamptz,
    OUT input_shape text,
    OUT output_shape text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'alohadb_list_models'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION alohadb_list_models() IS
'List all ONNX models currently loaded in the inference cache';
