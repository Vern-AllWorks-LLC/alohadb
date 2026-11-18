/* contrib/pg_unified/pg_unified--1.0--1.1.sql */

\echo Use "ALTER EXTENSION pg_unified UPDATE TO '1.1'" to load this file. \quit

-- unified_flush() was STRICT although p_limit defaults to NULL, so calling
-- it without a limit (the documented "flush everything" form) silently did
-- nothing and returned NULL.
ALTER FUNCTION unified_flush(text, int) CALLED ON NULL INPUT;
