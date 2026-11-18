--
-- pg_measure: parsing, output, conversion, binary I/O
--
CREATE EXTENSION pg_measure;
SET extra_float_digits = 0;

-- notations
SELECT '9.81 ± 0.02 m/s^2'::measurement AS pm;
SELECT '9.81 +/- 0.02 m*s^-2'::measurement AS ascii_pm;
SELECT '9.81+-0.02 m/s²'::measurement AS compact;
SELECT '(9.81 ± 0.02) m/s^2'::measurement AS parenthesized;
SELECT '12.5(3) mm'::measurement AS concise;
SELECT '1.2345(12)e3 g'::measurement AS concise_exp;
SELECT '300 K'::measurement, '5 kg'::measurement, '1.2e3 ± 5 J'::measurement;
SELECT '42'::measurement AS dimensionless, '-3.5 ± 0.1'::measurement AS negative;
SELECT '5eV'::measurement AS no_space, '  7.5   ±   .5   N·m  '::measurement AS spaces;

-- prefixes and symbols
SELECT x::measurement AS input, m_value(x::measurement, 'm') AS in_metres
  FROM (VALUES ('1 µm'), ('1 μm'), ('1 um'), ('1 mm'), ('1 cm'), ('1 dam'),
               ('1 km'), ('1 Mm'), ('1 nm'), ('1 pm'), ('1 Å'), ('1 angstrom'),
               ('1 in'), ('1 ft'), ('1 mi')) v(x);
SELECT x::measurement AS input, m_dimension(x::measurement) AS dim
  FROM (VALUES ('1 Hz'), ('1 kN'), ('1 MPa'), ('1 hPa'), ('1 mbar'), ('1 GW'),
               ('1 kWh'), ('1 keV'), ('1 kcal'), ('1 mA'), ('1 mV'), ('1 kΩ'),
               ('1 kohm'), ('1 µF'), ('1 mT'), ('1 Wb'), ('1 mH'), ('1 C'),
               ('1 lm'), ('1 lx'), ('1 mGy'), ('1 mSv'), ('1 kat'), ('1 mmol'),
               ('1 cd'), ('1 mL'), ('1 L'), ('1 kDa'), ('1 atm'), ('1 mmHg'),
               ('1 lb'), ('1 mph'), ('1 min'), ('1 h'), ('1 d'), ('1 ms'),
               ('1 °C'), ('1 degF'), ('1 ℃'), ('-40 °F'), ('0.1 °C'), ('1 kBq'), ('1 S'), ('1 Qg')) v(x);

-- conversion values
SELECT m_value('1 mi', 'm') AS mi_in_m,
       m_value('0 °C', 'K') AS zero_c_in_k,
       m_value('1 kWh', 'J') AS kwh_in_j,
       m_value('212 degF', 'degC') AS boiling_c,
       round(m_value('32 °F', 'K')::numeric, 10) AS freezing_k,
       m_value('1 atm', 'kPa') AS atm_kpa,
       m_value('1 lb', 'g') AS lb_g,
       m_value('60 mph', 'km/h') AS mph_kmh,
       round(m_value('1 eV', 'J')::numeric * 1e19, 9) AS ev_e19;
SELECT m_convert('1 mi', 'm') AS mi, m_convert('25 °C', 'K') AS t,
       m_convert('3.6 ± 0.1 MJ', 'kWh') AS energy,
       m_convert('100 ± 2 km/h', 'm/s') AS speed,
       m_convert('5 kg', 'g') AS mass;
SELECT m_value('1 km'), m_uncertainty('3 ± 0.2 km'), m_uncertainty('3 ± 0.2 km', 'm'),
       m_value(NULL::measurement), m_value('2 m', NULL);
SELECT m_unit('9.81 ± 0.02 m*s^-2'), m_unit('1 kg*m^2/s^2'), m_unit('1 s^-1'),
       m_unit('1 mm/m'), m_unit('1 m/m'), m_unit('5'), m_unit('1 °C');
SELECT m_dimension('9.81 m/s^2'), m_dimension('1 V'), m_dimension('1 K'), m_dimension('3');
SELECT measurement(9.81, 0.02, 'm/s^2'), measurement(20, 0.5, 'degC'),
       measurement(1, 0, ''), measurement(2.5, 0.1, 'kWh');
SELECT round(m_relative_uncertainty('10 ± 0.2 m')::numeric, 6) AS rel,
       m_relative_uncertainty('0 ± 1 m') AS rel_zero;
SELECT m_interval('9.81 ± 0.02 m/s^2') AS k2, m_interval('10 ± 1 mm', 3) AS k3,
       m_interval_lower('10 ± 1 mm', 1), m_interval_upper('10 ± 1 mm');

-- casts
SELECT '1.5 km'::measurement::float8 AS si_value, 2.5::float8::measurement AS from_float;
SELECT '1 km'::measurement::text AS as_text;

-- invalid inputs
SELECT ''::measurement;
SELECT 'abc'::measurement;
SELECT '5 furlongs'::measurement;
SELECT '5 ± -1 m'::measurement;
SELECT '5 ± m'::measurement;
SELECT '5 m/'::measurement;
SELECT '5 m^'::measurement;
SELECT '5 m^0'::measurement;
SELECT '5 m^1000'::measurement;
SELECT '5 m + 3'::measurement;
SELECT '1.2.3 m'::measurement;
SELECT '12.5(3) ± 1 mm'::measurement;
SELECT '12.5(3x) mm'::measurement;
SELECT '(5 ± 1 m'::measurement;
SELECT '1e999 m'::measurement;
SELECT '1e9999999 m'::measurement;
SELECT 'nan'::measurement;
SELECT 'inf m'::measurement;
SELECT '5 °C^2'::measurement;
SELECT '5 °C/s'::measurement;
SELECT '5 m/°C'::measurement;
SELECT '5 mdegC'::measurement;
SELECT '5 kmin'::measurement;
SELECT '5 m*m*m*m*s*A*K*mol*cd*g'::measurement;
SELECT '1 m*s*A*K*mol*cd*g*N*J'::measurement;
SELECT '1 m^100*m^100'::measurement;
SELECT '1 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'::measurement;
SELECT ('1' || repeat('0', 200) || ' m')::measurement;
SELECT m_convert('5 m', 's');
SELECT m_value('5 m', 'kg');
SELECT m_convert('5 m', 'bogus');
SELECT measurement(1, -1, 'm');
SELECT measurement('NaN', 0, 'm');
SELECT pg_input_is_valid('9.81 ± 0.02 m/s^2', 'measurement') AS ok,
       pg_input_is_valid('9.81 ± 0.02 furlong', 'measurement') AS bad;
SELECT * FROM pg_input_error_info('5 °C^2', 'measurement');

-- binary send/recv round trip
CREATE TEMP TABLE bin_src (m measurement);
INSERT INTO bin_src VALUES ('9.81 ± 0.02 m/s^2'), ('12.5(3) µm'), ('25 °C'),
	('1 kWh'), ('42'), ('-1.5e-3 ± 1e-5 kg*m^2/s^3/A');
\getenv abs_builddir PG_ABS_BUILDDIR
\set binfile :abs_builddir '/results/measure_bin.dat'
COPY bin_src TO :'binfile' WITH (FORMAT binary);
CREATE TEMP TABLE bin_dst (m measurement);
COPY bin_dst FROM :'binfile' WITH (FORMAT binary);
SELECT s.m AS original, d.m AS restored,
       measurement_send(s.m) = measurement_send(d.m) AS bytes_equal,
       m_unit(s.m) = m_unit(d.m) AS unit_equal
  FROM (SELECT m, row_number() OVER () rn FROM bin_src) s
  JOIN (SELECT m, row_number() OVER () rn FROM bin_dst) d USING (rn);
SELECT encode(measurement_send('2 ± 0.5 km'), 'hex') AS wire;
