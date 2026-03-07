-- alohadb_vectorize--1.0.sql
-- AlohaDB Vectorize - analytical query acceleration

\echo Use "CREATE EXTENSION alohadb_vectorize" to load this extension.\quit

-- vectorize_query: Execute analytical query with batch processing optimizations
-- Usage: SELECT * FROM vectorize_query('SELECT ...') AS t(col1 type1, col2 type2);
CREATE FUNCTION vectorize_query(sql_text text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'vectorize_query'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION vectorize_query(text) IS
    'Execute an analytical query with vectorized processing hints and parallel-friendly GUCs';

-- vectorize_explain: Show execution plan with vectorization annotations
CREATE FUNCTION vectorize_explain(sql_text text)
RETURNS TABLE(plan_line text)
AS 'MODULE_PATHNAME', 'vectorize_explain'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION vectorize_explain(text) IS
    'Show EXPLAIN ANALYZE output with vectorization annotations for an analytical query';

-- vectorize_status: Show current vectorization configuration
CREATE FUNCTION vectorize_status()
RETURNS TABLE(setting_name text, setting_value text)
AS 'MODULE_PATHNAME', 'vectorize_status'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION vectorize_status() IS
    'Show current GUC settings relevant to vectorized query execution';

-- vectorize_benchmark: Benchmark a query over multiple iterations
-- Note: Do NOT mark STRICT because iterations has a DEFAULT
CREATE FUNCTION vectorize_benchmark(sql_text text, iterations int DEFAULT 10)
RETURNS TABLE(iteration int, execution_time_ms float8)
AS 'MODULE_PATHNAME', 'vectorize_benchmark'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION vectorize_benchmark(text, int) IS
    'Benchmark an analytical query over multiple iterations, returning per-iteration timing';
