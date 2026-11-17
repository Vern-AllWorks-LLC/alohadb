--
-- alohadb_measure: provenance
--
CREATE TABLE samples (id int PRIMARY KEY, mass measurement);
CREATE TABLE results ("Result ID" text PRIMARY KEY, density measurement);

-- manual records
SELECT prov_record('samples', '1', 'weighing', agent => 'lab-robot-7',
                   attrs => '{"instrument": "balance-3"}') > 0 AS recorded;
SELECT tbl, row_key, column_name, entity, activity, agent, used, attrs
  FROM measure_provenance ORDER BY id;
SELECT prov_record('samples', '1', 'calibration', column_name => 'mass') > 0 AS recorded;
SELECT entity, activity, agent = current_user AS agent_is_me
  FROM measure_provenance WHERE column_name = 'mass';
SELECT prov_record('samples', '1', 'x', used => '{"not": "array"}');
SELECT prov_record('samples', NULL, 'x');
SELECT prov_entity('samples', '7'), prov_entity('results', 'r1', 'density');

-- automatic tracking
SELECT prov_track('samples', 'id', 'ingest');
SELECT prov_track('results', 'Result ID');
SELECT prov_track('samples', 'nope');
SELECT tgname FROM pg_trigger WHERE tgrelid IN ('samples'::regclass, 'results'::regclass)
 ORDER BY tgrelid::regclass::text;

SET application_name = 'measure_test';
INSERT INTO samples VALUES (1, '12.5 ± 0.1 g'), (2, '13.1 ± 0.1 g');
SET alohadb_measure.activity = 'recalibration';
SET alohadb_measure.source = 'balance-3 run 42';
UPDATE samples SET mass = '12.6 ± 0.1 g' WHERE id = 1;
RESET alohadb_measure.activity;
RESET alohadb_measure.source;

SET alohadb_measure.used = '[{"table": "samples", "row_key": "1"}, "public.samples:2"]';
INSERT INTO results VALUES ('r-1', '1.05 ± 0.01 g/mL');
RESET alohadb_measure.used;

SELECT tbl, row_key, entity, activity, agent = current_user AS agent_ok, used,
       attrs->>'operation' AS op, attrs->>'application_name' AS app,
       attrs->>'source' AS source, attrs->>'session_user' = session_user AS su_ok,
       (attrs->>'backend_pid')::int = pg_backend_pid() AS pid_ok,
       attrs ? 'xid' AS has_xid
  FROM measure_provenance
 WHERE activity <> 'weighing' AND column_name IS NULL ORDER BY id;

-- lineage: results r-1 <- samples 1 (3 records) and samples 2
SELECT depth, rel, key, column_name, activity
  FROM prov_lineage('results', 'r-1') ORDER BY depth, id;
SELECT count(*) FROM prov_lineage('results', 'r-1', 0);

-- cycle safety: samples 2 claims to use results r-1
SELECT prov_record('samples', '2', 'feedback', used => '["public.results:r-1"]') > 0;
SELECT depth, rel, key, activity FROM prov_lineage('results', 'r-1') ORDER BY depth, id;

SELECT prov_untrack('samples');
INSERT INTO samples VALUES (3, '1 g');
SELECT count(*) FROM measure_provenance WHERE tbl = 'samples'::regclass AND row_key = '3';

-- row-level security: an unprivileged role sees and adds provenance only for
-- tables it can access, and only as itself
CREATE ROLE measure_prov_user;
GRANT SELECT, INSERT ON results TO measure_prov_user;
SET ROLE measure_prov_user;
SELECT count(*) > 0 AS sees_results, bool_and(tbl = 'results'::regclass) AS only_results
  FROM measure_provenance;
INSERT INTO results VALUES ('r-2', '2 g/mL');
SELECT count(*) AS r2_records FROM measure_provenance WHERE row_key = 'r-2';
SELECT prov_record('samples', '1', 'forged') > 0;        -- no write access: rejected
SELECT prov_record('results', 'r-1', 'forged', agent => 'someone_else') > 0;  -- rejected
UPDATE measure_provenance SET activity = 'tampered';     -- no UPDATE privilege
RESET ROLE;
DROP OWNED BY measure_prov_user;
DROP ROLE measure_prov_user;

DROP TABLE samples, results;
DROP EXTENSION alohadb_measure;
