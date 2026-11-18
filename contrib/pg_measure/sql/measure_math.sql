--
-- pg_measure: uncertainty propagation, comparison, aggregates
--
SET extra_float_digits = 0;

-- helper: value and uncertainty rounded to 6 decimals in the display unit
CREATE FUNCTION show(m measurement) RETURNS text LANGUAGE sql IMMUTABLE STRICT
RETURN round(m_value(m)::numeric, 6) || ' ± ' || round(m_uncertainty(m)::numeric, 6)
       || ' ' || m_unit(m);

-- addition / subtraction: u = sqrt(ua^2 + ub^2)
SELECT show('10 ± 0.3 m'::measurement + '5 ± 0.4 m') AS add;          -- 15 ± 0.5
SELECT show('10 ± 0.3 m'::measurement - '5 ± 0.4 m') AS sub;          -- 5 ± 0.5
SELECT show('1 ± 0.003 km'::measurement + '500 ± 4 m') AS mixed_units; -- 1.5 ± 0.005 km
SELECT show('20 ± 0.3 °C'::measurement - '10 ± 0.4 °C') AS affine_diff; -- 10 ± 0.5 K
SELECT show('20 °C'::measurement + '10 K') AS affine_plus;             -- 303.15 K
SELECT show(-'5 ± 1 m'::measurement) AS neg;

-- multiplication / division: relative uncertainties in quadrature
-- (3 ± 0.03 m)*(4 ± 0.12 m) = 12 m^2, rel = sqrt(0.01^2+0.03^2) -> u = 0.379473
SELECT show('3 ± 0.03 m'::measurement * '4 ± 0.12 m') AS mul;
-- (100 ± 1 m)/(20 ± 0.4 s) = 5 m/s, rel = sqrt(0.01^2+0.02^2) -> u = 0.111803
SELECT show('100 ± 1 m'::measurement / '20 ± 0.4 s') AS div;
SELECT show('2 km'::measurement * '3 km') AS km2, show('10 kWh'::measurement / '2 h') AS power;
SELECT show('5 ± 0.1 kg'::measurement * '9.81 ± 0.02 m/s^2') AS force;
SELECT show('0 ± 1 m'::measurement * '2 ± 0.5 m') AS zero_factor;     -- u = 2
SELECT show('6 m'::measurement / '2 m') AS ratio, m_dimension('6 m'::measurement / '2 m');
SELECT show('20 °C'::measurement * '2 m') AS affine_mul;               -- 586.3 K*m
SELECT show('2 ± 0.1 m'::measurement * 3) AS scale_r, show(-2 * '2 ± 0.1 m'::measurement) AS scale_l,
       show('2 ± 0.1 m'::measurement / 4) AS scale_div;
SELECT '1 m'::measurement / '0 s';
SELECT '1 m'::measurement / 0::float8;

-- powers: u_rel = |p| * u/|x|
SELECT show(m_pow('2 ± 0.1 m', 2)) AS sq;                             -- 4 ± 0.4 m^2
SELECT show('2 ± 0.1 m'::measurement ^ 3) AS cube;                    -- 8 ± 1.2 m^3
SELECT show(m_sqrt('16 ± 0.8 m^2')) AS root;                          -- 4 ± 0.1 m
SELECT show(m_pow('4 ± 0.2 s', -1)) AS inv;                           -- 0.25 ± 0.0125 s^-1
SELECT show(m_pow('9 ± 0.9', 0.5)) AS half;                           -- 3 ± 0.15
SELECT show(m_pow('0 ± 0.1 m', 2)) AS zero_sq;
SELECT show(m_pow('2 km', 0)) AS pow0;
SELECT m_sqrt('2 m');
SELECT m_pow('-4 m^2', 0.5);
SELECT m_pow('0 ± 1', -1);
SELECT show(m_exp('0 ± 0.1')) AS e, show(m_ln('2.718281828459045 ± 0.27')) AS l;
SELECT show(m_ln('500 ± 5 mm/m')) AS ln_ratio;
SELECT m_exp('1 m');
SELECT m_ln('0');
SELECT '1e200 m'::measurement * '1e200 m';

-- dimension mismatches
SELECT '1 m'::measurement + '1 s';
SELECT '1 m'::measurement < '1 kg';
SELECT m_consistent('1 m', '1 s');

-- comparisons and sorting (SI values)
SELECT '1 km'::measurement = '1000 m' AS eq, '1 mi'::measurement > '1.6 km' AS gt,
       '0 °C'::measurement < '273.2 K' AS lt, '32 degF'::measurement = '0 degC' AS f_c,
       '1 in'::measurement <> '2.54 cm' AS ne, '5 ± 1 m'::measurement <= '5 ± 9 m' AS le;
SELECT m_compatible('1 J', '1 N*m') AS j_nm, m_compatible('1 Hz', '1 m') AS hz_m;
SELECT m_consistent('10 ± 0.1 m', '10.3 ± 0.1 m') AS k2,
       m_consistent('10 ± 0.1 m', '10.3 ± 0.1 m', 3) AS k3,
       m_consistent('10 m', '10 m', 0) AS exact;
SELECT m_consistent('1 m', '1 m', -1);

CREATE TABLE lengths (id int PRIMARY KEY, len measurement);
INSERT INTO lengths VALUES (1, '1 mi'), (2, '1500 ± 2 m'), (3, '1.7 km'), (4, '5000 ft'),
	(5, '100000 cm'), (6, NULL);
SELECT id, len FROM lengths ORDER BY len, id;
SELECT id, len FROM lengths ORDER BY len DESC NULLS LAST, id;
CREATE INDEX lengths_len_idx ON lengths (len);
SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT id FROM lengths WHERE len > '1.6 km';
SELECT id FROM lengths WHERE len > '1.6 km' ORDER BY len;
RESET enable_seqscan;
SELECT max(len), min(len) FROM lengths;
SELECT show(m_convert('98.6 ± 0.2 degF', 'degC')) AS body_temp, m_convert('-40 °F', '°C') AS minus40;
INSERT INTO lengths VALUES (7, '3 s');
SELECT count(DISTINCT len) FROM (VALUES ('1 km'::measurement), ('1000 m'), ('2 km')) v(len);

-- aggregates
CREATE TABLE obs (g text, m measurement);
INSERT INTO obs VALUES ('a', '10 ± 1 m'), ('a', '12 ± 2 m'),
	('b', '1 ± 0.1 km'), ('b', '1100 ± 100 m'), ('b', '0.9 ± 0.05 km'),
	('c', '5 m'), ('c', '7 ± 1 m'),
	('d', NULL);
SELECT g, show(m_wmean(m)) AS wmean, show(m_mean(m)) AS mean, show(m_sum(m)) AS sum
  FROM obs GROUP BY g ORDER BY g;
-- 10±1 and 12±2: w = 1, 0.25 -> 13/1.25 = 10.4, u = 1/sqrt(1.25) = 0.894427
SELECT show(m_wmean(m)) FROM obs WHERE g = 'a';
SELECT m_wmean(m) IS NULL AS empty_null, m_mean(m) IS NULL, m_sum(m) IS NULL FROM obs WHERE false;
SELECT show(m_mean(m)) FROM (VALUES ('20 ± 0.2 °C'::measurement), ('22 ± 0.2 °C')) v(m);
SELECT m_mean(m) FROM (VALUES ('1 m'::measurement), ('1 s')) v(m);

DROP TABLE lengths, obs;
DROP FUNCTION show(measurement);
