--
-- alohadb_lineage on alohadb_bitemporal tables (optional integration)
--
CREATE EXTENSION IF NOT EXISTS alohadb_lineage;
CREATE EXTENSION alohadb_bitemporal;

CREATE TABLE fx_rate (
    ccy        text,
    rate       numeric NOT NULL,
    valid_from timestamptz,
    valid_to   timestamptz,
    PRIMARY KEY (ccy, valid_from)
);
SELECT bitemporal_enable('fx_rate', ARRAY['ccy']);
INSERT INTO fx_rate (ccy, rate, valid_from) VALUES
    ('EUR', 1.10, '2024-01-01 00:00:00+00'),
    ('GBP', 1.25, '2024-01-01 00:00:00+00');

SELECT table_name, kind, row_count FROM lineage_snapshot('fx_train', ARRAY['fx_rate'::regclass]);

-- retroactive correction after the snapshot
SELECT bitemporal_update('fx_rate', '{"ccy": "EUR"}', '2024-02-01 00:00:00+00', NULL, '{"rate": 1.12}');
SELECT ccy, rate, valid_from AT TIME ZONE 'UTC' AS vf, valid_to AT TIME ZONE 'UTC' AS vt
  FROM fx_rate ORDER BY ccy, valid_from;

SELECT table_name, row_count_then, row_count_now, matches FROM lineage_verify('fx_train');
SELECT ccy, rate, valid_from AT TIME ZONE 'UTC' AS vf, valid_to AT TIME ZONE 'UTC' AS vt
  FROM lineage_read_snapshot(NULL::fx_rate, 'fx_train') ORDER BY ccy, valid_from;

-- branch of a bitemporal table; merge goes through the bitemporal triggers
SELECT lineage_branch_create('whatif', 'fx_rate');
UPDATE fx_rate__whatif SET rate = 1.30 WHERE ccy = 'GBP';
INSERT INTO fx_rate__whatif (ccy, rate, valid_from) VALUES ('JPY', 0.0068, '2024-01-01 00:00:00+00');
SELECT ccy, rate FROM fx_rate__whatif ORDER BY ccy, valid_from;
SELECT ccy, rate FROM fx_rate ORDER BY ccy, valid_from;
SELECT change, key - 'valid_from' AS key, branch_row ->> 'rate' AS rate
  FROM lineage_branch_diff('whatif', 'fx_rate');
SELECT * FROM lineage_branch_merge('whatif', 'fx_rate');
SELECT ccy, rate FROM fx_rate ORDER BY ccy, valid_from;
-- the superseded GBP version is in the bitemporal history
SELECT ccy, rate FROM fx_rate_history WHERE ccy = 'GBP';
SELECT lineage_branch_drop('whatif', 'fx_rate');

-- snapshot still reproducible
SELECT table_name, matches FROM lineage_verify('fx_train');
