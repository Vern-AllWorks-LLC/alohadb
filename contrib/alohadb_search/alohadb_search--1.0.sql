/* contrib/alohadb_search/alohadb_search--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_search" to load this file. \quit

-- ================================================================
-- Synonyms table
-- ================================================================

CREATE TABLE alohadb_search_synonyms (
    term     text NOT NULL,
    synonyms text[] NOT NULL,
    config   text NOT NULL DEFAULT 'english',
    PRIMARY KEY (term, config)
);

COMMENT ON TABLE alohadb_search_synonyms IS
'Synonym dictionary for search_expand_synonyms()';

COMMENT ON COLUMN alohadb_search_synonyms.term IS
'The word to look up';

COMMENT ON COLUMN alohadb_search_synonyms.synonyms IS
'Array of synonyms for the term';

COMMENT ON COLUMN alohadb_search_synonyms.config IS
'Text search configuration name (e.g. english)';

-- ================================================================
-- BM25 ranking (search_bm25.c)
-- ================================================================

CREATE FUNCTION search_bm25(
    document  tsvector,
    query     tsquery,
    k1        float8 DEFAULT 1.2,
    b         float8 DEFAULT 0.75
)
RETURNS float8
AS 'MODULE_PATHNAME', 'search_bm25'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

COMMENT ON FUNCTION search_bm25(tsvector, tsquery, float8, float8) IS
'Compute BM25 relevance score for a document against a query';

-- ================================================================
-- Fuzzy matching (search_fuzzy.c)
-- ================================================================

CREATE FUNCTION search_edit_distance(
    a text,
    b text
)
RETURNS int
AS 'MODULE_PATHNAME', 'search_edit_distance'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION search_edit_distance(text, text) IS
'Compute Levenshtein edit distance between two strings';

CREATE FUNCTION search_fuzzy_match(
    input        text,
    target       text,
    max_distance int DEFAULT 2
)
RETURNS boolean
AS 'MODULE_PATHNAME', 'search_fuzzy_match'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION search_fuzzy_match(text, text, int) IS
'Return true if edit distance between input and target is within max_distance';

CREATE FUNCTION search_phonetic(
    input     text,
    algorithm text DEFAULT 'double_metaphone'
)
RETURNS text
AS 'MODULE_PATHNAME', 'search_phonetic'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION search_phonetic(text, text) IS
'Compute Double Metaphone phonetic code for a string';

-- ================================================================
-- Suggestions and analysis (search_suggest.c)
-- ================================================================

CREATE FUNCTION search_autocomplete(
    prefix        text,
    source_table  text,
    source_column text,
    lim           int DEFAULT 10
)
RETURNS TABLE(suggestion text, score float8)
AS 'MODULE_PATHNAME', 'search_autocomplete'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION search_autocomplete(text, text, text, int) IS
'Find autocomplete suggestions by prefix search against a table column';

CREATE FUNCTION search_expand_synonyms(
    query  text,
    config text DEFAULT 'english'
)
RETURNS tsquery
AS 'MODULE_PATHNAME', 'search_expand_synonyms'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION search_expand_synonyms(text, text) IS
'Expand query terms with synonyms from alohadb_search_synonyms table';

CREATE FUNCTION search_analyze(
    input  text,
    config text DEFAULT 'english'
)
RETURNS TABLE(token text, type text, "position" int)
AS 'MODULE_PATHNAME', 'search_analyze'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION search_analyze(text, text) IS
'Analyze text using ts_debug and return tokens with types and positions';

-- ================================================================
-- Geographic search (search_geo.c)
-- ================================================================

CREATE FUNCTION search_haversine(
    lat1 float8,
    lon1 float8,
    lat2 float8,
    lon2 float8
)
RETURNS float8
AS 'MODULE_PATHNAME', 'search_haversine'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION search_haversine(float8, float8, float8, float8) IS
'Compute Haversine great-circle distance in meters between two lat/lon points';

CREATE FUNCTION search_nearby(
    lat           float8,
    lon           float8,
    radius_meters float8,
    source_table  text,
    lat_column    text,
    lon_column    text,
    lim           int DEFAULT 100
)
RETURNS TABLE(id text, distance float8)
AS 'MODULE_PATHNAME', 'search_nearby'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION search_nearby(float8, float8, float8, text, text, text, int) IS
'Find rows within radius_meters of a point, returning id and distance';
