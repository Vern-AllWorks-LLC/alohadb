-- alohadb_parquet v1.1 regression test
-- Tests: extension upgrade, function signatures, path validation

CREATE EXTENSION alohadb_parquet VERSION '1.0';

-- Verify v1.0 read_parquet has 1 argument
SELECT proname, pronargs
FROM   pg_proc
WHERE  proname = 'read_parquet'
  AND  pronargs = 1;

-- Upgrade to 1.1
ALTER EXTENSION alohadb_parquet UPDATE TO '1.1';

-- After upgrade: read_parquet now has 2 arguments (file_path, options)
SELECT proname, pronargs
FROM   pg_proc
WHERE  proname = 'read_parquet';

-- Old 1-arg variant is gone
SELECT count(*) AS one_arg_count
FROM   pg_proc
WHERE  proname = 'read_parquet' AND pronargs = 1;

-- read_csv and read_json still present (unchanged)
SELECT proname, pronargs
FROM   pg_proc
WHERE  proname IN ('read_csv', 'read_json')
ORDER BY proname;

-- -----------------------------------------------------------------------
-- Path validation errors
-- -----------------------------------------------------------------------

\set ON_ERROR_STOP 0
-- Non-existent file
SELECT * FROM read_parquet('/tmp/does_not_exist_alohadb_test.parquet');
\set ON_ERROR_STOP 1

\set ON_ERROR_STOP 0
-- Path not in allowed directories
SELECT * FROM read_parquet('/etc/passwd');
\set ON_ERROR_STOP 1

\set ON_ERROR_STOP 0
-- Valid path but not a Parquet file
SELECT * FROM read_parquet('/dev/null');
\set ON_ERROR_STOP 1

-- -----------------------------------------------------------------------
-- read_csv still works (smoke test with a temp file)
-- -----------------------------------------------------------------------

-- Write a small CSV to /tmp using pg_file_write (if available)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'pg_file_write') THEN
        PERFORM pg_file_write('/tmp/alohadb_test.csv',
            'name,age,city' || chr(10) ||
            'alice,30,NYC'  || chr(10) ||
            'bob,25,LA'     || chr(10),
            false);
    END IF;
END $$;

-- Only run if file was written
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'pg_file_write') THEN
        PERFORM count(*) FROM read_csv('/tmp/alohadb_test.csv');
    END IF;
END $$;

-- -----------------------------------------------------------------------
-- Cleanup
-- -----------------------------------------------------------------------

DROP EXTENSION alohadb_parquet;
