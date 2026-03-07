/* contrib/alohadb_http/alohadb_http--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_http" to load this file.\quit

-- ================================================================
-- http_get
--
-- Perform an HTTP GET request.
-- ================================================================

CREATE FUNCTION http_get(
    url text,
    headers jsonb DEFAULT NULL,
    timeout_ms int DEFAULT 30000
)
RETURNS TABLE(status int, response_headers jsonb, body text)
AS 'MODULE_PATHNAME', 'http_get'
LANGUAGE C VOLATILE PARALLEL UNSAFE;

COMMENT ON FUNCTION http_get(text, jsonb, int) IS
    'Perform an HTTP GET request and return status, headers, and body';

-- ================================================================
-- http_post
--
-- Perform an HTTP POST request.
-- ================================================================

CREATE FUNCTION http_post(
    url text,
    body text DEFAULT NULL,
    headers jsonb DEFAULT NULL,
    timeout_ms int DEFAULT 30000
)
RETURNS TABLE(status int, response_headers jsonb, body text)
AS 'MODULE_PATHNAME', 'http_post'
LANGUAGE C VOLATILE PARALLEL UNSAFE;

COMMENT ON FUNCTION http_post(text, text, jsonb, int) IS
    'Perform an HTTP POST request and return status, headers, and body';

-- ================================================================
-- http_put
--
-- Perform an HTTP PUT request.
-- ================================================================

CREATE FUNCTION http_put(
    url text,
    body text DEFAULT NULL,
    headers jsonb DEFAULT NULL,
    timeout_ms int DEFAULT 30000
)
RETURNS TABLE(status int, response_headers jsonb, body text)
AS 'MODULE_PATHNAME', 'http_put'
LANGUAGE C VOLATILE PARALLEL UNSAFE;

COMMENT ON FUNCTION http_put(text, text, jsonb, int) IS
    'Perform an HTTP PUT request and return status, headers, and body';

-- ================================================================
-- http_delete
--
-- Perform an HTTP DELETE request.
-- ================================================================

CREATE FUNCTION http_delete(
    url text,
    headers jsonb DEFAULT NULL,
    timeout_ms int DEFAULT 30000
)
RETURNS TABLE(status int, response_headers jsonb, body text)
AS 'MODULE_PATHNAME', 'http_delete'
LANGUAGE C VOLATILE PARALLEL UNSAFE;

COMMENT ON FUNCTION http_delete(text, jsonb, int) IS
    'Perform an HTTP DELETE request and return status, headers, and body';

-- ================================================================
-- http_request
--
-- Perform an HTTP request with an arbitrary method.
-- ================================================================

CREATE FUNCTION http_request(
    method text,
    url text,
    body text DEFAULT NULL,
    headers jsonb DEFAULT NULL,
    timeout_ms int DEFAULT 30000
)
RETURNS TABLE(status int, response_headers jsonb, body text)
AS 'MODULE_PATHNAME', 'http_request'
LANGUAGE C VOLATILE PARALLEL UNSAFE;

COMMENT ON FUNCTION http_request(text, text, text, jsonb, int) IS
    'Perform an HTTP request with the specified method and return status, headers, and body';
