/* contrib/alohadb_nl2sql/alohadb_nl2sql--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_nl2sql" to load this file. \quit

-- =====================================================================
-- NL2SQL: translate natural language to SQL via LLM
-- =====================================================================

--
-- alohadb_nl2sql(question text) RETURNS text
--
-- Translates a natural language question into a SQL query using the
-- configured LLM endpoint.  Returns the generated SQL as text.
--
CREATE FUNCTION alohadb_nl2sql(question text)
RETURNS text
AS 'MODULE_PATHNAME', 'alohadb_nl2sql'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION alohadb_nl2sql(text) IS
    'Translate a natural language question to a SQL query via LLM';

--
-- alohadb_nl2sql_execute(question text) RETURNS SETOF record
--
-- Translates a natural language question into SQL, then executes it
-- in a read-only context.  The caller must specify the expected
-- result columns via the AS clause.
--
-- Example:
--   SELECT * FROM alohadb_nl2sql_execute('list all active users')
--     AS t(id int, name text, active boolean);
--
CREATE FUNCTION alohadb_nl2sql_execute(question text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'alohadb_nl2sql_execute'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION alohadb_nl2sql_execute(text) IS
    'Translate a natural language question to SQL and execute it (read-only)';

--
-- alohadb_explain_query(sql text) RETURNS text
--
-- Uses the LLM to provide a plain-English explanation of the given
-- SQL query in the context of the current database schema.
--
CREATE FUNCTION alohadb_explain_query(sql text)
RETURNS text
AS 'MODULE_PATHNAME', 'alohadb_explain_query'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION alohadb_explain_query(text) IS
    'Use LLM to explain a SQL query in plain English';
