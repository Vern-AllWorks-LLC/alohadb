/* contrib/pg_measure/pg_measure--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_measure" to load this file. \quit

--------------------------------------------------------------------------
-- The measurement type
--------------------------------------------------------------------------

CREATE TYPE measurement;

CREATE FUNCTION measurement_in(cstring) RETURNS measurement
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION measurement_out(measurement) RETURNS cstring
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION measurement_recv(internal) RETURNS measurement
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION measurement_send(measurement) RETURNS bytea
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE measurement (
	INTERNALLENGTH = 72,
	INPUT = measurement_in,
	OUTPUT = measurement_out,
	RECEIVE = measurement_recv,
	SEND = measurement_send,
	ALIGNMENT = double,
	STORAGE = plain
);

COMMENT ON TYPE measurement IS
  'physical quantity: value, standard uncertainty and SI unit';

--------------------------------------------------------------------------
-- Constructors, accessors, conversion
--------------------------------------------------------------------------

CREATE FUNCTION measurement(value float8, uncertainty float8, unit text)
RETURNS measurement
AS 'MODULE_PATHNAME', 'measurement_make' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- not STRICT: unit defaults to NULL (= the display unit)
CREATE FUNCTION m_value(m measurement, unit text DEFAULT NULL) RETURNS float8
AS 'MODULE_PATHNAME', 'm_value' LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION m_uncertainty(m measurement, unit text DEFAULT NULL) RETURNS float8
AS 'MODULE_PATHNAME', 'm_uncertainty' LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION m_relative_uncertainty(m measurement) RETURNS float8
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION m_unit(m measurement) RETURNS text
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION m_dimension(m measurement) RETURNS text
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION m_convert(m measurement, unit text) RETURNS measurement
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION m_interval_lower(m measurement, k float8 DEFAULT 2) RETURNS float8
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION m_interval_upper(m measurement, k float8 DEFAULT 2) RETURNS float8
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- coverage interval [x - k*u, x + k*u] in the display unit
CREATE FUNCTION m_interval(m measurement, k float8 DEFAULT 2) RETURNS numrange
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
RETURN pg_catalog.numrange(@extschema@.m_interval_lower(m, k)::numeric,
                           @extschema@.m_interval_upper(m, k)::numeric, '[]');

CREATE FUNCTION m_compatible(a measurement, b measurement) RETURNS bool
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION m_consistent(a measurement, b measurement, k float8 DEFAULT 2)
RETURNS bool
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION measurement_to_float8(measurement) RETURNS float8
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION measurement_from_float8(float8) RETURNS measurement
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE CAST (measurement AS float8) WITH FUNCTION measurement_to_float8(measurement);
CREATE CAST (float8 AS measurement) WITH FUNCTION measurement_from_float8(float8);

--------------------------------------------------------------------------
-- Arithmetic
--------------------------------------------------------------------------

CREATE FUNCTION measurement_add(measurement, measurement) RETURNS measurement
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION measurement_sub(measurement, measurement) RETURNS measurement
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION measurement_mul(measurement, measurement) RETURNS measurement
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION measurement_div(measurement, measurement) RETURNS measurement
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION measurement_neg(measurement) RETURNS measurement
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION measurement_mul_float8(measurement, float8) RETURNS measurement
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION float8_mul_measurement(float8, measurement) RETURNS measurement
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION measurement_div_float8(measurement, float8) RETURNS measurement
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION m_pow(m measurement, p float8) RETURNS measurement
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION m_sqrt(m measurement) RETURNS measurement
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION m_exp(m measurement) RETURNS measurement
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION m_ln(m measurement) RETURNS measurement
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR + (LEFTARG = measurement, RIGHTARG = measurement,
	FUNCTION = measurement_add, COMMUTATOR = +);
CREATE OPERATOR - (LEFTARG = measurement, RIGHTARG = measurement,
	FUNCTION = measurement_sub);
CREATE OPERATOR * (LEFTARG = measurement, RIGHTARG = measurement,
	FUNCTION = measurement_mul, COMMUTATOR = *);
CREATE OPERATOR / (LEFTARG = measurement, RIGHTARG = measurement,
	FUNCTION = measurement_div);
CREATE OPERATOR - (RIGHTARG = measurement, FUNCTION = measurement_neg);
CREATE OPERATOR * (LEFTARG = measurement, RIGHTARG = float8,
	FUNCTION = measurement_mul_float8, COMMUTATOR = *);
CREATE OPERATOR * (LEFTARG = float8, RIGHTARG = measurement,
	FUNCTION = float8_mul_measurement, COMMUTATOR = *);
CREATE OPERATOR / (LEFTARG = measurement, RIGHTARG = float8,
	FUNCTION = measurement_div_float8);
CREATE OPERATOR ^ (LEFTARG = measurement, RIGHTARG = float8, FUNCTION = m_pow);

--------------------------------------------------------------------------
-- Comparison and btree opclass (on the SI value; dimensions must match)
--------------------------------------------------------------------------

CREATE FUNCTION measurement_eq(measurement, measurement) RETURNS bool
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION measurement_ne(measurement, measurement) RETURNS bool
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION measurement_lt(measurement, measurement) RETURNS bool
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION measurement_le(measurement, measurement) RETURNS bool
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION measurement_gt(measurement, measurement) RETURNS bool
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION measurement_ge(measurement, measurement) RETURNS bool
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION measurement_cmp(measurement, measurement) RETURNS int4
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR = (LEFTARG = measurement, RIGHTARG = measurement,
	FUNCTION = measurement_eq, COMMUTATOR = =, NEGATOR = <>,
	RESTRICT = eqsel, JOIN = eqjoinsel, MERGES);
CREATE OPERATOR <> (LEFTARG = measurement, RIGHTARG = measurement,
	FUNCTION = measurement_ne, COMMUTATOR = <>, NEGATOR = =,
	RESTRICT = neqsel, JOIN = neqjoinsel);
CREATE OPERATOR < (LEFTARG = measurement, RIGHTARG = measurement,
	FUNCTION = measurement_lt, COMMUTATOR = >, NEGATOR = >=,
	RESTRICT = scalarltsel, JOIN = scalarltjoinsel);
CREATE OPERATOR <= (LEFTARG = measurement, RIGHTARG = measurement,
	FUNCTION = measurement_le, COMMUTATOR = >=, NEGATOR = >,
	RESTRICT = scalarlesel, JOIN = scalarlejoinsel);
CREATE OPERATOR > (LEFTARG = measurement, RIGHTARG = measurement,
	FUNCTION = measurement_gt, COMMUTATOR = <, NEGATOR = <=,
	RESTRICT = scalargtsel, JOIN = scalargtjoinsel);
CREATE OPERATOR >= (LEFTARG = measurement, RIGHTARG = measurement,
	FUNCTION = measurement_ge, COMMUTATOR = <=, NEGATOR = <,
	RESTRICT = scalargesel, JOIN = scalargejoinsel);

CREATE OPERATOR CLASS measurement_ops
DEFAULT FOR TYPE measurement USING btree AS
	OPERATOR 1 <,
	OPERATOR 2 <=,
	OPERATOR 3 =,
	OPERATOR 4 >=,
	OPERATOR 5 >,
	FUNCTION 1 measurement_cmp(measurement, measurement);

--------------------------------------------------------------------------
-- Aggregates
--------------------------------------------------------------------------

CREATE FUNCTION measurement_larger(measurement, measurement) RETURNS measurement
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION measurement_smaller(measurement, measurement) RETURNS measurement
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE AGGREGATE max(measurement) (
	SFUNC = measurement_larger, STYPE = measurement,
	COMBINEFUNC = measurement_larger, SORTOP = >, PARALLEL = SAFE
);
CREATE AGGREGATE min(measurement) (
	SFUNC = measurement_smaller, STYPE = measurement,
	COMBINEFUNC = measurement_smaller, SORTOP = <, PARALLEL = SAFE
);

CREATE AGGREGATE m_sum(measurement) (
	SFUNC = measurement_add,
	STYPE = measurement,
	COMBINEFUNC = measurement_add,
	PARALLEL = SAFE
);

CREATE FUNCTION m_agg_trans(internal, measurement) RETURNS internal
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE PARALLEL SAFE;
CREATE FUNCTION m_mean_final(internal) RETURNS measurement
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION m_wmean_final(internal) RETURNS measurement
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- arithmetic mean; u = sqrt(sum u_i^2) / n
CREATE AGGREGATE m_mean(measurement) (
	SFUNC = m_agg_trans,
	STYPE = internal,
	FINALFUNC = m_mean_final
);

-- inverse-variance weighted mean; u = 1/sqrt(sum 1/u_i^2).
-- Falls back to m_mean semantics if any input has zero uncertainty.
CREATE AGGREGATE m_wmean(measurement) (
	SFUNC = m_agg_trans,
	STYPE = internal,
	FINALFUNC = m_wmean_final
);

--------------------------------------------------------------------------
-- Provenance (small subset of W3C PROV: entity, activity, agent, used)
--------------------------------------------------------------------------

CREATE TABLE measure_provenance (
	id           bigserial PRIMARY KEY,
	tbl          regclass NOT NULL,
	row_key      text NOT NULL,
	column_name  text,
	entity       text NOT NULL,
	activity     text NOT NULL,
	agent        text NOT NULL,
	used         jsonb NOT NULL DEFAULT '[]',
	generated_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
	attrs        jsonb NOT NULL DEFAULT '{}'
);
CREATE INDEX measure_provenance_row_idx ON measure_provenance (tbl, row_key);
CREATE INDEX measure_provenance_entity_idx ON measure_provenance (entity);

SELECT pg_catalog.pg_extension_config_dump('measure_provenance', '');
SELECT pg_catalog.pg_extension_config_dump('measure_provenance_id_seq', '');

-- Any role may read and append provenance, but row-level security limits it
-- to tables the role itself can access: records are visible only for tables
-- the role can SELECT, and a role can only add records (as itself) for tables
-- it can INSERT into or UPDATE.  Tracking triggers run as the writing role,
-- which by definition passes the INSERT check.  Records cannot be modified.
ALTER TABLE measure_provenance ENABLE ROW LEVEL SECURITY;
CREATE POLICY measure_provenance_read ON measure_provenance FOR SELECT
	USING (pg_catalog.has_table_privilege(tbl, 'SELECT'));
CREATE POLICY measure_provenance_append ON measure_provenance FOR INSERT
	WITH CHECK (agent = current_user::text
	            AND pg_catalog.has_table_privilege(tbl, 'INSERT, UPDATE'));
GRANT SELECT, INSERT ON measure_provenance TO PUBLIC;
GRANT USAGE ON SEQUENCE measure_provenance_id_seq TO PUBLIC;

-- Stable entity identifier: "schema.table:row_key[#column]"
CREATE FUNCTION prov_entity(tbl regclass, row_key text, column_name text DEFAULT NULL)
RETURNS text
LANGUAGE sql STABLE PARALLEL SAFE
SET search_path = pg_catalog, pg_temp
AS $$
	SELECT format('%I.%I', n.nspname, c.relname) || ':' || $2
	       || coalesce('#' || $3, '')
	  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
	 WHERE c.oid = $1
$$;

CREATE FUNCTION prov_record(tbl regclass, row_key text, activity text,
                            agent text DEFAULT current_user,
                            used jsonb DEFAULT '[]',
                            attrs jsonb DEFAULT '{}',
                            column_name text DEFAULT NULL)
RETURNS bigint
LANGUAGE plpgsql VOLATILE
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	v_entity text;
	v_id     bigint;
BEGIN
	IF $1 IS NULL OR $2 IS NULL OR $3 IS NULL THEN
		RAISE EXCEPTION 'prov_record: tbl, row_key and activity must not be NULL'
			USING ERRCODE = 'null_value_not_allowed';
	END IF;
	IF $5 IS NOT NULL AND jsonb_typeof($5) <> 'array' THEN
		RAISE EXCEPTION 'prov_record: "used" must be a JSON array'
			USING ERRCODE = 'invalid_parameter_value';
	END IF;
	IF $6 IS NOT NULL AND jsonb_typeof($6) <> 'object' THEN
		RAISE EXCEPTION 'prov_record: "attrs" must be a JSON object'
			USING ERRCODE = 'invalid_parameter_value';
	END IF;
	v_entity := @extschema@.prov_entity($1, $2, $7);
	IF v_entity IS NULL THEN
		RAISE EXCEPTION 'prov_record: relation % does not exist', $1::oid
			USING ERRCODE = 'undefined_table';
	END IF;
	INSERT INTO @extschema@.measure_provenance
		(tbl, row_key, column_name, entity, activity, agent, used, attrs)
	VALUES ($1, $2, $7, v_entity, $3, coalesce($4, current_user::text),
	        coalesce($5, '[]'::jsonb), coalesce($6, '{}'::jsonb))
	RETURNING measure_provenance.id INTO v_id;
	RETURN v_id;
END
$$;

-- Row trigger installed by prov_track(): TG_ARGV = (key_column, activity)
CREATE FUNCTION measure_prov_trigger() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	v_key      text;
	v_activity text;
	v_source   text;
	v_used     jsonb;
	v_attrs    jsonb;
BEGIN
	v_key := to_jsonb(NEW) ->> TG_ARGV[0];
	IF v_key IS NULL THEN
		RAISE EXCEPTION 'pg_measure: key column "%" of %.% is NULL',
			TG_ARGV[0], TG_TABLE_SCHEMA, TG_TABLE_NAME
			USING ERRCODE = 'null_value_not_allowed';
	END IF;
	v_activity := coalesce(nullif(current_setting('pg_measure.activity', true), ''),
	                       TG_ARGV[1]);
	v_source := nullif(current_setting('pg_measure.source', true), '');
	v_used := coalesce(nullif(current_setting('pg_measure.used', true), '')::jsonb,
	                   '[]'::jsonb);
	IF jsonb_typeof(v_used) <> 'array' THEN
		v_used := jsonb_build_array(v_used);
	END IF;
	v_attrs := jsonb_strip_nulls(jsonb_build_object(
		'operation', TG_OP,
		'session_user', session_user::text,
		'application_name', nullif(current_setting('application_name', true), ''),
		'backend_pid', pg_backend_pid(),
		'xid', pg_current_xact_id()::text,
		'source', v_source));
	INSERT INTO @extschema@.measure_provenance
		(tbl, row_key, column_name, entity, activity, agent, used, attrs)
	VALUES (TG_RELID::regclass, v_key, NULL,
	        @extschema@.prov_entity(TG_RELID::regclass, v_key),
	        v_activity, current_user::text, v_used, v_attrs);
	RETURN NULL;
END
$$;

CREATE FUNCTION prov_track(tbl regclass, key_column text, activity text DEFAULT 'write')
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
	IF $1 IS NULL OR $2 IS NULL OR $3 IS NULL THEN
		RAISE EXCEPTION 'prov_track: arguments must not be NULL'
			USING ERRCODE = 'null_value_not_allowed';
	END IF;
	IF NOT EXISTS (SELECT 1 FROM pg_attribute a
	                WHERE a.attrelid = $1 AND a.attname = $2
	                  AND a.attnum > 0 AND NOT a.attisdropped) THEN
		RAISE EXCEPTION 'column "%" of relation % does not exist', $2, $1
			USING ERRCODE = 'undefined_column';
	END IF;
	IF EXISTS (SELECT 1 FROM pg_trigger t
	            WHERE t.tgrelid = $1 AND t.tgname = 'pg_measure_prov') THEN
		EXECUTE format('DROP TRIGGER pg_measure_prov ON %s', $1);
	END IF;
	EXECUTE format('CREATE TRIGGER pg_measure_prov AFTER INSERT OR UPDATE ON %s '
	               'FOR EACH ROW EXECUTE FUNCTION %s(%L, %L)',
	               $1, '@extschema@.measure_prov_trigger', $2, $3);
END
$$;

CREATE FUNCTION prov_untrack(tbl regclass) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_trigger t
	            WHERE t.tgrelid = $1 AND t.tgname = 'pg_measure_prov') THEN
		EXECUTE format('DROP TRIGGER pg_measure_prov ON %s', $1);
	END IF;
END
$$;

-- Lineage: the provenance records of a row, then (recursively) those of
-- the entities listed in their "used" arrays.  "used" elements may be
-- entity strings, {"entity": "..."} or {"table": "...", "row_key": "..."}
-- (table names resolved with the caller's search_path).
CREATE FUNCTION prov_lineage(tbl regclass, row_key text, max_depth int DEFAULT 10)
RETURNS TABLE (depth int, id bigint, rel regclass, key text, column_name text,
               entity text, activity text, agent text, used jsonb,
               generated_at timestamptz, attrs jsonb)
LANGUAGE sql STABLE STRICT
AS $$
	WITH RECURSIVE lineage(depth, id, path) AS (
		SELECT 0, p.id, ARRAY[p.id]
		  FROM @extschema@.measure_provenance p
		 WHERE p.tbl = $1 AND p.row_key = $2
		UNION ALL
		SELECT l.depth + 1, p.id, l.path || p.id
		  FROM lineage l
		  JOIN @extschema@.measure_provenance c ON c.id = l.id
		  CROSS JOIN LATERAL pg_catalog.jsonb_array_elements(
		        CASE WHEN pg_catalog.jsonb_typeof(c.used) = 'array'
		             THEN c.used ELSE '[]'::jsonb END) AS u(elem)
		  JOIN @extschema@.measure_provenance p ON
		       (pg_catalog.jsonb_typeof(u.elem) = 'string'
		        AND p.entity = u.elem #>> '{}')
		    OR (pg_catalog.jsonb_typeof(u.elem) = 'object'
		        AND p.entity = u.elem ->> 'entity')
		    OR (pg_catalog.jsonb_typeof(u.elem) = 'object'
		        AND p.tbl = pg_catalog.to_regclass(u.elem ->> 'table')
		        AND p.row_key = u.elem ->> 'row_key')
		 WHERE l.depth < $3 AND p.id <> ALL (l.path)
	), firsts AS (
		SELECT DISTINCT ON (l.id) l.depth, l.id
		  FROM lineage l ORDER BY l.id, l.depth
	)
	SELECT f.depth, p.id, p.tbl, p.row_key, p.column_name, p.entity,
	       p.activity, p.agent, p.used, p.generated_at, p.attrs
	  FROM firsts f JOIN @extschema@.measure_provenance p ON p.id = f.id
	 ORDER BY f.depth, p.id
$$;
