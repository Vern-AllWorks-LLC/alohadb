/* contrib/alohadb_bitemporal/alohadb_bitemporal--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_bitemporal" to load this file. \quit

-- ================================================================
-- alohadb_bitemporal -- SQL:2011-style bitemporal tables
--
-- Every registered table carries two half-open periods:
--   * application ("valid") time  [valid_from, valid_to)  -- user supplied
--   * system ("transaction") time [sys_from,  sys_to)     -- trigger managed
-- Open ends are represented by 'infinity' (never NULL), so all period
-- arithmetic is plain comparison.  The current table holds the rows whose
-- system period is still open (sys_to = 'infinity'); every superseded
-- version is moved to ONE companion table <tbl>_history with its system
-- period closed.  Both tables carry both periods.
--
-- Prior art / patent notes:
--   * Design follows ISO/IEC 9075:2011 (SQL:2011) application-time and
--     system-versioned tables, including UPDATE/DELETE ... FOR PORTION OF
--     semantics, and R. T. Snodgrass, "Developing Time-Oriented Database
--     Applications in SQL" (Morgan Kaufmann, 1999), ch. 10 (sequenced
--     modifications on bitemporal tables).
--   * Storage is deliberately one current table + one history table, each
--     row holding its full key, data, valid period and system period.  It
--     does NOT use a three-table summary/event/key layout in which
--     validity start and end are held as separate event records (the
--     structure claimed by US 8,812,512).
--   * Only the two standard time dimensions exist; there is no third
--     "visibility" / publication time domain (cf. US 9,262,491).
-- ================================================================

-- ----------------------------------------------------------------
-- Registry
-- ----------------------------------------------------------------
CREATE TABLE @extschema@.bitemporal_tables (
    table_name      regclass    PRIMARY KEY,
    history_table   regclass    NOT NULL,
    key_columns     text[]      NOT NULL,
    valid_from_col  text        NOT NULL,
    valid_to_col    text        NOT NULL,
    enabled_at      timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE @extschema@.bitemporal_tables IS
'Registry of tables with bitemporal (application + system time) versioning';

SELECT pg_catalog.pg_extension_config_dump('@extschema@.bitemporal_tables', '');

-- ----------------------------------------------------------------
-- Internal helpers
-- ----------------------------------------------------------------

-- System time used for this statement.  Normally the transaction start
-- time (SQL:2011: all changes of a transaction share one system time).
-- The custom setting alohadb_bitemporal.system_time can override it for
-- back-loading historical data and for deterministic tests.
CREATE FUNCTION @extschema@._bt_system_time()
RETURNS timestamptz
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    v text := current_setting('alohadb_bitemporal.system_time', true);
BEGIN
    IF v IS NULL OR v = '' THEN
        RETURN now();
    END IF;
    RETURN v::timestamptz;
END;
$$;

-- Registry row of a table, or an error if the table is not bitemporal.
CREATE FUNCTION @extschema@._bt_conf(p_tbl regclass)
RETURNS @extschema@.bitemporal_tables
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    c @extschema@.bitemporal_tables;
BEGIN
    SELECT * INTO c FROM @extschema@.bitemporal_tables WHERE table_name = p_tbl;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'table % is not bitemporal', p_tbl
            USING HINT = 'Call bitemporal_enable() first.';
    END IF;
    RETURN c;
END;
$$;

-- Column names of a table in attnum order (optionally only the ones that
-- can be written by INSERT, i.e. not generated).
CREATE FUNCTION @extschema@._bt_columns(p_tbl regclass, p_insertable boolean)
RETURNS text[]
LANGUAGE sql STABLE
AS $$
    SELECT coalesce(array_agg(a.attname::text ORDER BY a.attnum), '{}')
    FROM pg_catalog.pg_attribute a
    WHERE a.attrelid = p_tbl AND a.attnum > 0 AND NOT a.attisdropped
      AND (NOT p_insertable OR a.attgenerated = '');
$$;

-- Comma separated, quoted column list (optionally prefixed by an alias).
CREATE FUNCTION @extschema@._bt_collist(p_cols text[], p_alias text)
RETURNS text
LANGUAGE sql IMMUTABLE
AS $$
    SELECT string_agg(CASE WHEN p_alias IS NULL THEN format('%I', c)
                           ELSE format('%I.%I', p_alias, c) END, ', ' ORDER BY o)
    FROM unnest(p_cols) WITH ORDINALITY AS u(c, o);
$$;

-- Serialize-per-key lock: concurrent writers of the same key in the same
-- table queue up, which makes the trigger-based overlap check safe.
CREATE FUNCTION @extschema@._bt_lock_key(p_tbl regclass, p_keys text[], p_row jsonb)
RETURNS void
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    k text;
    v jsonb := '[]';
BEGIN
    FOREACH k IN ARRAY p_keys LOOP
        v := v || jsonb_build_array(p_row -> k);
    END LOOP;
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtext('alohadb_bitemporal'),
        pg_catalog.hashtext(p_tbl::oid::text || ':' || v::text));
END;
$$;

-- Validate a key object and normalise its values through the table's
-- column types; returns {key_col: value, ...} with only key columns.
CREATE FUNCTION @extschema@._bt_norm_key(c @extschema@.bitemporal_tables, p_key jsonb)
RETURNS jsonb
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    k text;
    full_row jsonb;
    res jsonb := '{}';
BEGIN
    IF p_key IS NULL OR jsonb_typeof(p_key) <> 'object' THEN
        RAISE EXCEPTION 'key must be a JSON object naming the key columns %', c.key_columns;
    END IF;
    FOR k IN SELECT jsonb_object_keys(p_key) LOOP
        IF NOT k = ANY (c.key_columns) THEN
            RAISE EXCEPTION '"%" is not a key column of % (key columns: %)',
                k, c.table_name, c.key_columns;
        END IF;
    END LOOP;
    EXECUTE format('SELECT to_jsonb(r) FROM jsonb_populate_record(NULL::%s, $1) r',
                   c.table_name)
        INTO full_row USING p_key;
    FOREACH k IN ARRAY c.key_columns LOOP
        IF full_row -> k IS NULL OR full_row -> k = 'null'::jsonb THEN
            RAISE EXCEPTION 'key column "%" missing from key %', k, p_key;
        END IF;
        res := res || jsonb_build_object(k, full_row -> k);
    END LOOP;
    RETURN res;
END;
$$;

-- Insert one row given as jsonb (all writable columns).
CREATE FUNCTION @extschema@._bt_insert_json(p_tbl regclass, p_cols text[], p_row jsonb)
RETURNS void
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    cl text := @extschema@._bt_collist(p_cols, NULL);
BEGIN
    EXECUTE format('INSERT INTO %s (%s) OVERRIDING SYSTEM VALUE '
                   'SELECT %s FROM jsonb_populate_record(NULL::%s, $1)',
                   p_tbl, cl, cl, p_tbl)
        USING p_row;
END;
$$;

-- ----------------------------------------------------------------
-- Triggers
-- ----------------------------------------------------------------

-- BEFORE INSERT OR UPDATE OR DELETE, FOR EACH ROW: maintains the system
-- period and moves superseded versions to the history table.
CREATE FUNCTION @extschema@.bitemporal_versioning_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    c   @extschema@.bitemporal_tables;
    st  timestamptz;
    cl  text;
BEGIN
    c := @extschema@._bt_conf(TG_RELID::regclass);
    st := @extschema@._bt_system_time();

    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        IF OLD.sys_from > st THEN
            RAISE EXCEPTION 'system time % precedes the start % of the row version being superseded in %',
                st, OLD.sys_from, TG_RELID::regclass
                USING ERRCODE = 'serialization_failure',
                      HINT = 'A later transaction already modified this row; retry the transaction.';
        END IF;
        PERFORM @extschema@._bt_lock_key(TG_RELID::regclass, c.key_columns, to_jsonb(OLD));
        -- A version created and superseded at the same system instant
        -- (same transaction) never existed for anyone: no history row.
        IF OLD.sys_from < st THEN
            OLD.sys_to := st;
            cl := @extschema@._bt_collist(@extschema@._bt_columns(TG_RELID::regclass, false), NULL);
            EXECUTE format('INSERT INTO %s (%s) SELECT %s FROM (SELECT ($1).*) s',
                           c.history_table, cl, cl)
                USING OLD;
        END IF;
    END IF;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;

    PERFORM @extschema@._bt_lock_key(TG_RELID::regclass, c.key_columns, to_jsonb(NEW));
    NEW.sys_from := st;
    NEW.sys_to := 'infinity';
    RETURN NEW;
END;
$$;

-- AFTER INSERT OR UPDATE, FOR EACH ROW: no two current rows with the same
-- key may have overlapping valid periods (the new row overlaps itself, so
-- more than one match means a conflict).
CREATE FUNCTION @extschema@.bitemporal_overlap_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    c    @extschema@.bitemporal_tables;
    cond text;
    n    bigint;
    j    jsonb;
BEGIN
    c := @extschema@._bt_conf(TG_RELID::regclass);
    SELECT string_agg(format('t.%I = ($1).%I', k, k), ' AND ')
      INTO cond FROM unnest(c.key_columns) AS k;
    EXECUTE format('SELECT count(*) FROM %s t WHERE %s AND t.%I < ($1).%I AND t.%I > ($1).%I',
                   TG_RELID::regclass, cond,
                   c.valid_from_col, c.valid_to_col,
                   c.valid_to_col, c.valid_from_col)
        INTO n USING NEW;
    IF n > 1 THEN
        j := to_jsonb(NEW);
        RAISE EXCEPTION 'overlapping valid periods for key % in %',
            (SELECT jsonb_object_agg(k, j -> k) FROM unnest(c.key_columns) AS k),
            TG_RELID::regclass
            USING ERRCODE = 'exclusion_violation',
                  DETAIL = format('New period [%s, %s) overlaps an existing current row.',
                                  j ->> c.valid_from_col, j ->> c.valid_to_col);
    END IF;
    RETURN NULL;
END;
$$;

-- BEFORE TRUNCATE: truncation would silently discard system history.
CREATE FUNCTION @extschema@.bitemporal_no_truncate_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'cannot TRUNCATE bitemporal table %', TG_RELID::regclass
        USING HINT = 'Use DELETE (history is kept) or bitemporal_disable() first.';
END;
$$;

-- ----------------------------------------------------------------
-- bitemporal_enable(tbl, key_columns, valid_from, valid_to)
-- ----------------------------------------------------------------
CREATE FUNCTION @extschema@.bitemporal_enable(
    tbl         regclass,
    key_columns text[],
    valid_from  text DEFAULT 'valid_from',
    valid_to    text DEFAULT 'valid_to')
RETURNS regclass
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    nsp      name;
    rel      name;
    relkind  char;
    hist     text;
    k        text;
    typ      regtype;
    st       timestamptz := @extschema@._bt_system_time();
    bad      text;
    cond     text;
    keylist  text;
    n        bigint;
BEGIN
    IF tbl IS NULL OR key_columns IS NULL OR cardinality(key_columns) = 0
       OR valid_from IS NULL OR valid_to IS NULL THEN
        RAISE EXCEPTION 'tbl, key_columns, valid_from and valid_to must not be NULL or empty';
    END IF;
    IF EXISTS (SELECT 1 FROM @extschema@.bitemporal_tables WHERE table_name = tbl) THEN
        RAISE EXCEPTION 'table % is already bitemporal', tbl;
    END IF;
    SELECT n.nspname, c.relname, c.relkind INTO nsp, rel, relkind
      FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     WHERE c.oid = tbl;
    IF relkind NOT IN ('r', 'p') THEN
        RAISE EXCEPTION '% is not a table', tbl;
    END IF;
    IF valid_from = valid_to OR valid_from IN ('sys_from', 'sys_to')
       OR valid_to IN ('sys_from', 'sys_to') THEN
        RAISE EXCEPTION 'invalid valid-time column names "%" / "%"', valid_from, valid_to;
    END IF;

    -- key columns must exist and be distinct from the period columns
    FOREACH k IN ARRAY key_columns LOOP
        IF k IN (valid_from, valid_to, 'sys_from', 'sys_to') THEN
            RAISE EXCEPTION 'period column "%" cannot be a key column', k;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                        WHERE attrelid = tbl AND attname = k AND attnum > 0 AND NOT attisdropped) THEN
            RAISE EXCEPTION 'column "%" of key does not exist in %', k, tbl;
        END IF;
    END LOOP;

    -- system-time columns: may pre-exist only as timestamptz
    FOREACH k IN ARRAY ARRAY['sys_from', 'sys_to', valid_from, valid_to] LOOP
        SELECT atttypid::regtype INTO typ FROM pg_catalog.pg_attribute
         WHERE attrelid = tbl AND attname = k AND attnum > 0 AND NOT attisdropped;
        IF FOUND AND typ <> 'timestamptz'::regtype THEN
            RAISE EXCEPTION 'column "%" of % must be of type timestamptz, not %', k, tbl, typ;
        END IF;
    END LOOP;

    -- a unique constraint on the key alone would forbid several versions
    SELECT string_agg(i.indexrelid::regclass::text, ', ') INTO bad
      FROM pg_catalog.pg_index i
     WHERE i.indrelid = tbl AND i.indisunique
       AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute a
                        WHERE a.attrelid = tbl AND a.attname = valid_from
                          AND a.attnum = ANY (i.indkey::int2[]));
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'unique index % on % does not include column "%"', bad, tbl, valid_from
            USING HINT = 'A bitemporal table stores several versions per key; include the valid_from column in primary keys / unique constraints.';
    END IF;

    hist := format('%I.%I', nsp, rel || '_history');
    IF to_regclass(hist) IS NOT NULL THEN
        RAISE EXCEPTION 'history table % already exists', hist;
    END IF;

    -- application-time columns
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                    WHERE attrelid = tbl AND attname = valid_from AND attnum > 0 AND NOT attisdropped) THEN
        EXECUTE format('ALTER TABLE %s ADD COLUMN %I timestamptz NOT NULL DEFAULT %L',
                       tbl, valid_from, '-infinity');
        EXECUTE format('ALTER TABLE %s ALTER COLUMN %I DROP DEFAULT', tbl, valid_from);
    ELSE
        EXECUTE format('ALTER TABLE %s ALTER COLUMN %I SET NOT NULL', tbl, valid_from);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                    WHERE attrelid = tbl AND attname = valid_to AND attnum > 0 AND NOT attisdropped) THEN
        EXECUTE format('ALTER TABLE %s ADD COLUMN %I timestamptz NOT NULL DEFAULT %L',
                       tbl, valid_to, 'infinity');
    ELSE
        EXECUTE format('UPDATE %s SET %I = %L WHERE %I IS NULL', tbl, valid_to, 'infinity', valid_to);
        EXECUTE format('ALTER TABLE %s ALTER COLUMN %I SET NOT NULL', tbl, valid_to);
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_attrdef d JOIN pg_catalog.pg_attribute a
                         ON a.attrelid = d.adrelid AND a.attnum = d.adnum
                        WHERE d.adrelid = tbl AND a.attname = valid_to) THEN
            EXECUTE format('ALTER TABLE %s ALTER COLUMN %I SET DEFAULT %L', tbl, valid_to, 'infinity');
        END IF;
    END IF;
    EXECUTE format('ALTER TABLE %s ADD CHECK (%I < %I)', tbl, valid_from, valid_to);

    -- system-time columns (existing rows become versions starting now)
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                    WHERE attrelid = tbl AND attname = 'sys_from' AND attnum > 0 AND NOT attisdropped) THEN
        EXECUTE format('ALTER TABLE %s ADD COLUMN sys_from timestamptz NOT NULL DEFAULT %L', tbl, st);
        EXECUTE format('ALTER TABLE %s ALTER COLUMN sys_from DROP DEFAULT', tbl);
    ELSE
        EXECUTE format('UPDATE %s SET sys_from = %L WHERE sys_from IS NULL', tbl, st);
        EXECUTE format('ALTER TABLE %s ALTER COLUMN sys_from SET NOT NULL', tbl);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                    WHERE attrelid = tbl AND attname = 'sys_to' AND attnum > 0 AND NOT attisdropped) THEN
        EXECUTE format('ALTER TABLE %s ADD COLUMN sys_to timestamptz NOT NULL DEFAULT %L', tbl, 'infinity');
    ELSE
        EXECUTE format('UPDATE %s SET sys_to = %L', tbl, 'infinity');
        EXECUTE format('ALTER TABLE %s ALTER COLUMN sys_to SET NOT NULL', tbl);
    END IF;

    -- key columns are mandatory
    FOREACH k IN ARRAY key_columns LOOP
        EXECUTE format('ALTER TABLE %s ALTER COLUMN %I SET NOT NULL', tbl, k);
    END LOOP;

    -- existing data must already be free of overlaps
    SELECT string_agg(format('a.%I = b.%I', x, x), ' AND ') INTO cond FROM unnest(key_columns) AS x;
    EXECUTE format('SELECT count(*) FROM %s a JOIN %s b ON %s AND a.ctid <> b.ctid '
                   'AND a.%I < b.%I AND a.%I > b.%I',
                   tbl, tbl, cond, valid_from, valid_to, valid_to, valid_from) INTO n;
    IF n > 0 THEN
        RAISE EXCEPTION 'existing rows of % have overlapping valid periods for the same key', tbl
            USING ERRCODE = 'exclusion_violation';
    END IF;

    -- history table: same columns, no constraints besides NOT NULL
    EXECUTE format('CREATE TABLE %s (LIKE %s INCLUDING DEFAULTS)', hist, tbl);
    keylist := @extschema@._bt_collist(key_columns, NULL);
    EXECUTE format('CREATE INDEX ON %s (%s, %I)', tbl, keylist, valid_from);
    EXECUTE format('CREATE INDEX ON %s (%s, sys_from)', hist, keylist);
    EXECUTE format('CREATE INDEX ON %s (sys_from, sys_to)', hist);

    EXECUTE format('CREATE TRIGGER bitemporal_versioning BEFORE INSERT OR UPDATE OR DELETE ON %s '
                   'FOR EACH ROW EXECUTE FUNCTION @extschema@.bitemporal_versioning_trigger()', tbl);
    EXECUTE format('CREATE TRIGGER bitemporal_overlap AFTER INSERT OR UPDATE ON %s '
                   'FOR EACH ROW EXECUTE FUNCTION @extschema@.bitemporal_overlap_trigger()', tbl);
    EXECUTE format('CREATE TRIGGER bitemporal_no_truncate BEFORE TRUNCATE ON %s '
                   'FOR EACH STATEMENT EXECUTE FUNCTION @extschema@.bitemporal_no_truncate_trigger()', tbl);

    INSERT INTO @extschema@.bitemporal_tables
        (table_name, history_table, key_columns, valid_from_col, valid_to_col)
    VALUES (tbl, hist::regclass, key_columns, valid_from, valid_to);

    RETURN hist::regclass;
END;
$$;

COMMENT ON FUNCTION @extschema@.bitemporal_enable(regclass, text[], text, text) IS
'Make a table bitemporal: add valid/system period columns, create <tbl>_history and install triggers';

-- ----------------------------------------------------------------
-- bitemporal_disable(tbl, drop_history)
-- ----------------------------------------------------------------
CREATE FUNCTION @extschema@.bitemporal_disable(tbl regclass, drop_history boolean DEFAULT false)
RETURNS void
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    c @extschema@.bitemporal_tables;
BEGIN
    c := @extschema@._bt_conf(tbl);
    EXECUTE format('DROP TRIGGER IF EXISTS bitemporal_versioning ON %s', tbl);
    EXECUTE format('DROP TRIGGER IF EXISTS bitemporal_overlap ON %s', tbl);
    EXECUTE format('DROP TRIGGER IF EXISTS bitemporal_no_truncate ON %s', tbl);
    DELETE FROM @extschema@.bitemporal_tables WHERE table_name = tbl;
    IF coalesce(drop_history, false) THEN
        EXECUTE format('DROP TABLE %s', c.history_table);
    END IF;
END;
$$;

COMMENT ON FUNCTION @extschema@.bitemporal_disable(regclass, boolean) IS
'Stop bitemporal versioning of a table (columns are kept; history optionally dropped)';

-- ----------------------------------------------------------------
-- Sequenced (FOR PORTION OF) modification core
-- ----------------------------------------------------------------
CREATE FUNCTION @extschema@._bt_portion(
    p_tbl regclass, p_key jsonb, p_from timestamptz, p_to timestamptz,
    p_set jsonb, p_delete boolean)
RETURNS integer
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    c     @extschema@.bitemporal_tables;
    nkey  jsonb;
    cols  text[];
    cond  text;
    k     text;
    r     record;
    rvf   timestamptz;
    rvt   timestamptz;
    n     integer := 0;
BEGIN
    c := @extschema@._bt_conf(p_tbl);
    nkey := @extschema@._bt_norm_key(c, p_key);
    p_from := coalesce(p_from, '-infinity');
    p_to := coalesce(p_to, 'infinity');
    IF p_from >= p_to THEN
        RAISE EXCEPTION 'empty period: valid_from (%) must be earlier than valid_to (%)', p_from, p_to;
    END IF;
    cols := @extschema@._bt_columns(p_tbl, true);

    IF NOT p_delete THEN
        IF p_set IS NULL OR jsonb_typeof(p_set) <> 'object' THEN
            RAISE EXCEPTION 'set_values must be a JSON object of column/value pairs';
        END IF;
        FOR k IN SELECT jsonb_object_keys(p_set) LOOP
            IF k = ANY (c.key_columns) OR k IN (c.valid_from_col, c.valid_to_col, 'sys_from', 'sys_to') THEN
                RAISE EXCEPTION 'column "%" cannot be changed by bitemporal_update', k
                    USING HINT = 'Key and period columns are managed by the sequenced update itself.';
            END IF;
            IF NOT k = ANY (cols) THEN
                RAISE EXCEPTION 'column "%" does not exist or is not writable in %', k, p_tbl;
            END IF;
        END LOOP;
    END IF;

    PERFORM @extschema@._bt_lock_key(p_tbl, c.key_columns, nkey);

    SELECT string_agg(format('t.%I = k.%I', x, x), ' AND ') INTO cond FROM unnest(c.key_columns) AS x;
    FOR r IN EXECUTE format(
        'SELECT t.ctid AS tid, to_jsonb(t) AS j FROM %s t, jsonb_populate_record(NULL::%s, $1) k '
        'WHERE %s AND t.%I < $3 AND t.%I > $2 ORDER BY t.%I FOR UPDATE OF t',
        p_tbl, p_tbl, cond, c.valid_from_col, c.valid_to_col, c.valid_from_col)
        USING nkey, p_from, p_to
    LOOP
        rvf := (r.j ->> c.valid_from_col)::timestamptz;
        rvt := (r.j ->> c.valid_to_col)::timestamptz;
        -- the whole old version leaves the current table (-> history)
        EXECUTE format('DELETE FROM %s WHERE ctid = $1', p_tbl) USING r.tid;
        -- untouched leading part
        IF rvf < p_from THEN
            PERFORM @extschema@._bt_insert_json(p_tbl, cols,
                r.j || jsonb_build_object(c.valid_to_col, p_from));
        END IF;
        -- untouched trailing part
        IF rvt > p_to THEN
            PERFORM @extschema@._bt_insert_json(p_tbl, cols,
                r.j || jsonb_build_object(c.valid_from_col, p_to));
        END IF;
        -- the portion inside [p_from, p_to), with new values
        IF NOT p_delete THEN
            PERFORM @extschema@._bt_insert_json(p_tbl, cols,
                r.j || p_set || jsonb_build_object(c.valid_from_col, greatest(rvf, p_from),
                                                   c.valid_to_col, least(rvt, p_to)));
        END IF;
        n := n + 1;
    END LOOP;
    RETURN n;
END;
$$;

CREATE FUNCTION @extschema@.bitemporal_update(
    tbl regclass, key jsonb, valid_from timestamptz, valid_to timestamptz, set_values jsonb)
RETURNS integer
LANGUAGE sql VOLATILE
AS $$ SELECT @extschema@._bt_portion(tbl, key, valid_from, valid_to, set_values, false) $$;

COMMENT ON FUNCTION @extschema@.bitemporal_update(regclass, jsonb, timestamptz, timestamptz, jsonb) IS
'UPDATE ... FOR PORTION OF valid time: set columns for one key within [valid_from, valid_to), splitting versions as needed';

CREATE FUNCTION @extschema@.bitemporal_delete(
    tbl regclass, key jsonb, valid_from timestamptz, valid_to timestamptz)
RETURNS integer
LANGUAGE sql VOLATILE
AS $$ SELECT @extschema@._bt_portion(tbl, key, valid_from, valid_to, NULL, true) $$;

COMMENT ON FUNCTION @extschema@.bitemporal_delete(regclass, jsonb, timestamptz, timestamptz) IS
'DELETE ... FOR PORTION OF valid time: remove one key''s validity within [valid_from, valid_to), trimming/splitting versions';

-- ----------------------------------------------------------------
-- Queries
-- ----------------------------------------------------------------
CREATE FUNCTION @extschema@.bitemporal_as_of(
    tbl anyelement, valid_at timestamptz, system_at timestamptz DEFAULT NULL)
RETURNS SETOF anyelement
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    rel  regclass;
    c    @extschema@.bitemporal_tables;
    cl   text;
    vcond text;
    q    text;
BEGIN
    SELECT t.typrelid::regclass INTO rel FROM pg_catalog.pg_type t
     WHERE t.oid = pg_catalog.pg_typeof(tbl) AND t.typrelid <> 0;
    IF rel IS NULL THEN
        RAISE EXCEPTION 'first argument must be a table row type, e.g. NULL::my_table';
    END IF;
    c := @extschema@._bt_conf(rel);
    cl := @extschema@._bt_collist(@extschema@._bt_columns(rel, false), 't');
    vcond := format('($1::timestamptz IS NULL OR (t.%I <= $1 AND t.%I > $1))',
                    c.valid_from_col, c.valid_to_col);
    IF system_at IS NULL THEN
        q := format('SELECT %s FROM %s t WHERE %s', cl, rel, vcond);
    ELSE
        q := format('SELECT %s FROM %s t WHERE t.sys_from <= $2 AND t.sys_to > $2 AND %s '
                    'UNION ALL '
                    'SELECT %s FROM %s t WHERE t.sys_from <= $2 AND t.sys_to > $2 AND %s',
                    cl, rel, vcond, cl, c.history_table, vcond);
    END IF;
    RETURN QUERY EXECUTE q USING valid_at, system_at;
END;
$$;

COMMENT ON FUNCTION @extschema@.bitemporal_as_of(anyelement, timestamptz, timestamptz) IS
'Rows valid at valid_at (NULL = all valid periods) as recorded at system_at (NULL = now); call as bitemporal_as_of(NULL::tbl, ...)';

CREATE FUNCTION @extschema@.bitemporal_history(tbl regclass, key jsonb DEFAULT NULL)
RETURNS TABLE (entity_key jsonb, valid_from timestamptz, valid_to timestamptz,
               sys_from timestamptz, sys_to timestamptz, is_current boolean, row_data jsonb)
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    c     @extschema@.bitemporal_tables;
    nkey  jsonb;
    strip text[];
    q     text;
BEGIN
    c := @extschema@._bt_conf(tbl);
    IF key IS NOT NULL THEN
        nkey := @extschema@._bt_norm_key(c, key);
    END IF;
    strip := c.key_columns || ARRAY[c.valid_from_col, c.valid_to_col, 'sys_from', 'sys_to'];
    q := format(
        'SELECT (SELECT jsonb_object_agg(k, v.j -> k) FROM unnest($2::text[]) k), '
        '       (v.j ->> %L)::timestamptz, (v.j ->> %L)::timestamptz, '
        '       (v.j ->> ''sys_from'')::timestamptz, (v.j ->> ''sys_to'')::timestamptz, '
        '       v.cur, v.j - $3::text[] '
        'FROM (SELECT to_jsonb(t) AS j, true AS cur FROM %s t '
        '      UNION ALL SELECT to_jsonb(h), false FROM %s h) v '
        'WHERE $1::jsonb IS NULL OR v.j @> $1 '
        'ORDER BY 1, 2, 4',
        c.valid_from_col, c.valid_to_col, tbl, c.history_table);
    RETURN QUERY EXECUTE q USING nkey, c.key_columns, strip;
END;
$$;

COMMENT ON FUNCTION @extschema@.bitemporal_history(regclass, jsonb) IS
'Full bitemporal timeline (current + history versions) of one key, or of all keys when key is NULL';
