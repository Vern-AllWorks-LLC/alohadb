/* contrib/alohadb_gpu/alohadb_gpu--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_gpu" to load this file. \quit

-- =====================================================================
-- Batch L2 (Euclidean) distance
-- =====================================================================

CREATE FUNCTION alohadb_gpu_batch_l2(query vector, candidates vector[])
RETURNS float4[]
AS 'MODULE_PATHNAME', 'alohadb_gpu_batch_l2'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION alohadb_gpu_batch_l2(vector, vector[]) IS
    'Compute L2 (Euclidean) distance from query to each candidate vector using GPU acceleration';

-- =====================================================================
-- Batch cosine distance
-- =====================================================================

CREATE FUNCTION alohadb_gpu_batch_cosine(query vector, candidates vector[])
RETURNS float4[]
AS 'MODULE_PATHNAME', 'alohadb_gpu_batch_cosine'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION alohadb_gpu_batch_cosine(vector, vector[]) IS
    'Compute cosine distance (1 - cosine_similarity) from query to each candidate vector using GPU acceleration';

-- =====================================================================
-- GPU top-K nearest vectors
-- =====================================================================

CREATE FUNCTION alohadb_gpu_topk(query vector, candidates vector[], k int)
RETURNS TABLE(idx int, distance float4)
AS 'MODULE_PATHNAME', 'alohadb_gpu_topk'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
ROWS 100;

COMMENT ON FUNCTION alohadb_gpu_topk(vector, vector[], int) IS
    'Return the k nearest candidate vectors by L2 distance, computed on GPU';

-- =====================================================================
-- GPU matrix multiplication
-- =====================================================================

CREATE FUNCTION alohadb_gpu_matmul(a float4[][], b float4[][])
RETURNS float4[][]
AS 'MODULE_PATHNAME', 'alohadb_gpu_matmul'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION alohadb_gpu_matmul(float4[][], float4[][]) IS
    'Multiply two 2-D float4 matrices on GPU (or CPU fallback)';

-- =====================================================================
-- GPU availability check
-- =====================================================================

CREATE FUNCTION alohadb_gpu_available()
RETURNS bool
AS 'MODULE_PATHNAME', 'alohadb_gpu_available'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

COMMENT ON FUNCTION alohadb_gpu_available() IS
    'Returns true if a CUDA-capable GPU is available';
