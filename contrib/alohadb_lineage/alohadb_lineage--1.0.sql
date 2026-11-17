/* contrib/alohadb_lineage/alohadb_lineage--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION alohadb_lineage" to load this file. \quit

-- ================================================================
-- alohadb_lineage -- dataset snapshots, model lineage, zero-copy branches
--
-- Works on tables whose past states are recoverable from a history table:
--   * system-versioned tables of alohadb_temporal (row_start/row_end,
--     history in <tbl>_history), and
--   * bitemporal tables of alohadb_bitemporal (sys_from/sys_to), when that
--     extension is installed (detected at run time, never required).
-- A snapshot is only a name + a timestamp + per-table fingerprints; the
-- data itself is reconstructed "AS OF" that timestamp from current +
-- history rows, so taking a snapshot copies nothing.
--
-- Content hash (order independent): for every row visible AS OF the
-- snapshot time, h(row) = first 64 bits of md5(ROW(c1, c2, ...)::text)
-- over all columns except the system-time columns, read as a signed
-- 64-bit integer; the table hash is (sum of h(row)) mod 2^64, printed as
-- 16 hex digits.  Summation (unlike XOR) keeps duplicate rows visible.
--
-- Branches use a logical overlay: the base table AS OF the branch point,
-- minus keys present in a per-branch tombstone table or overridden in a
-- per-branch delta table, UNION ALL the delta rows.  Writes to the branch
-- view are redirected by INSTEAD OF triggers to delta/tombstones.
--
-- Prior art / patent notes:
--   * Snapshots and time travel rely only on ISO/IEC 9075:2011 (SQL:2011)
--     system versioning (current table + history table, period columns).
--   * Branches are "differential files" / overlay deltas as described by
--     D. G. Severance and G. M. Lohman, "Differential files: their
--     application to the maintenance of large databases", ACM TODS 1(3),
--     1976: a read-only base plus a small file of changes and deletions
--     consulted first.  They are implemented purely logically with
--     ordinary tables, views and triggers.
--   * No metadata-level cloning of physical storage (no copy-on-write of
--     micro-partitions, pages, segments or page-server layers) is used.
--   * The order-independent row hash is a plain multiset sum of
--     per-row digests (incremental/multiset hashing literature, e.g.
--     Bellare & Micciancio 1997; Clarke et al. 2003).
-- ================================================================

-- ----------------------------------------------------------------
-- Catalog
-- ----------------------------------------------------------------
CREATE TABLE @extschema@.lineage_snapshot_catalog (
    snapshot_name text        PRIMARY KEY,
    taken_at      timestamptz NOT NULL,
    note          text,
    created_by    name        NOT NULL DEFAULT current_user
);

CREATE TABLE @extschema@.lineage_snapshot_tables (
    snapshot_name text     NOT NULL
        REFERENCES @extschema@.lineage_snapshot_catalog ON DELETE CASCADE,
    table_name    regclass NOT NULL,
    kind          text     NOT NULL CHECK (kind IN ('temporal', 'bitemporal')),
    hash_columns  text[]   NOT NULL,
    row_count     bigint   NOT NULL,
    content_hash  text     NOT NULL,
    PRIMARY KEY (snapshot_name, table_name)
);

CREATE TABLE @extschema@.lineage_model_links (
    model         text        NOT NULL,
    version       text        NOT NULL,
    snapshot_name text        NOT NULL
        REFERENCES @extschema@.lineage_snapshot_catalog ON DELETE RESTRICT,
    metadata      jsonb,
    linked_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (model, version)
);

CREATE TABLE @extschema@.lineage_branch_catalog (
    branch          text        NOT NULL,
    base_table      regclass    NOT NULL,
    branch_point    timestamptz NOT NULL,
    from_snapshot   text
        REFERENCES @extschema@.lineage_snapshot_catalog ON DELETE SET NULL,
    view_name       regclass    NOT NULL UNIQUE,
    delta_table     regclass    NOT NULL,
    tombstone_table regclass    NOT NULL,
    pk_columns      text[]      NOT NULL,
    status          text        NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'merged')),
    created_at      timestamptz NOT NULL DEFAULT now(),
    merged_at       timestamptz,
    PRIMARY KEY (branch, base_table)
);

COMMENT ON TABLE @extschema@.lineage_snapshot_catalog IS 'Named dataset snapshots (a point in system time)';
COMMENT ON TABLE @extschema@.lineage_snapshot_tables IS 'Per-table row counts and content hashes of each snapshot';
COMMENT ON TABLE @extschema@.lineage_model_links IS 'Which snapshot each model version was trained on';
COMMENT ON TABLE @extschema@.lineage_branch_catalog IS 'Zero-copy table branches (overlay delta + tombstones)';

SELECT pg_catalog.pg_extension_config_dump('@extschema@.lineage_snapshot_catalog', '');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.lineage_snapshot_tables', '');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.lineage_model_links', '');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.lineage_branch_catalog', '');

-- ----------------------------------------------------------------
-- Internal helpers
-- ----------------------------------------------------------------

-- How a table is versioned: kind, history table and system-time columns.
CREATE FUNCTION @extschema@._lin_versioning(
    p_tbl regclass,
    OUT kind text, OUT history_table regclass, OUT sys_start text, OUT sys_end text)
RETURNS record
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    ns   text;
    h    text;
BEGIN
    -- alohadb_bitemporal (optional)
    SELECT e.extnamespace::regnamespace::text INTO ns
      FROM pg_catalog.pg_extension e WHERE e.extname = 'alohadb_bitemporal';
    IF ns IS NOT NULL THEN
        EXECUTE format('SELECT history_table::oid::regclass FROM %s.bitemporal_tables WHERE table_name = $1', ns)
            INTO history_table USING p_tbl;
        IF history_table IS NOT NULL THEN
            kind := 'bitemporal'; sys_start := 'sys_from'; sys_end := 'sys_to';
            RETURN;
        END IF;
    END IF;
    -- alohadb_temporal (optional)
    ns := NULL;
    SELECT e.extnamespace::regnamespace::text INTO ns
      FROM pg_catalog.pg_extension e WHERE e.extname = 'alohadb_temporal';
    IF ns IS NOT NULL THEN
        EXECUTE format('SELECT history_table FROM %s.alohadb_temporal_tables WHERE table_name = $1', ns)
            INTO h USING p_tbl;
        IF h IS NOT NULL THEN
            SELECT to_regclass(format('%I.%I', n.nspname, h)) INTO history_table
              FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             WHERE c.oid = p_tbl;
            IF history_table IS NULL THEN
                RAISE EXCEPTION 'history table "%" of % is missing', h, p_tbl;
            END IF;
            kind := 'temporal'; sys_start := 'row_start'; sys_end := 'row_end';
            RETURN;
        END IF;
    END IF;
    RAISE EXCEPTION 'table % is not system-versioned', p_tbl
        USING HINT = 'Enable alohadb_temporal system versioning (alohadb_enable_system_versioning) or alohadb_bitemporal (bitemporal_enable) on it first.';
END;
$$;

-- Column names (attnum order); optionally without the system-time columns
-- and/or only INSERT-able (non-generated) ones.
CREATE FUNCTION @extschema@._lin_columns(p_tbl regclass, p_exclude text[], p_insertable boolean)
RETURNS text[]
LANGUAGE sql STABLE
AS $$
    SELECT coalesce(array_agg(a.attname::text ORDER BY a.attnum), '{}')
    FROM pg_catalog.pg_attribute a
    WHERE a.attrelid = p_tbl AND a.attnum > 0 AND NOT a.attisdropped
      AND NOT (a.attname::text = ANY (coalesce(p_exclude, '{}')))
      AND (NOT p_insertable OR a.attgenerated = '');
$$;

CREATE FUNCTION @extschema@._lin_collist(p_cols text[], p_alias text)
RETURNS text
LANGUAGE sql IMMUTABLE
AS $$
    SELECT string_agg(CASE WHEN p_alias IS NULL THEN format('%I', c)
                           ELSE format('%I.%I', p_alias, c) END, ', ' ORDER BY o)
    FROM unnest(p_cols) WITH ORDINALITY AS u(c, o);
$$;

-- jsonb_build_object('k1', a.k1, ...) expression for key columns.
CREATE FUNCTION @extschema@._lin_keyexpr(p_cols text[], p_alias text)
RETURNS text
LANGUAGE sql IMMUTABLE
AS $$
    SELECT 'jsonb_build_object(' ||
           string_agg(format('%L, %I.%I', c, p_alias, c), ', ' ORDER BY o) || ')'
    FROM unnest(p_cols) WITH ORDINALITY AS u(c, o);
$$;

-- a.k1 = b.k1 AND ... ; b may be a parameter reference like "($1)".
CREATE FUNCTION @extschema@._lin_keycond(p_cols text[], p_left text, p_right text)
RETURNS text
LANGUAGE sql IMMUTABLE
AS $$
    SELECT string_agg(format('%s.%I = %s.%I', p_left, c, p_right, c), ' AND ' ORDER BY o)
    FROM unnest(p_cols) WITH ORDINALITY AS u(c, o);
$$;

-- SELECT <cols> of p_tbl as it was at p_ts (current + history rows whose
-- system period contains p_ts).
CREATE FUNCTION @extschema@._lin_asof_sql(p_tbl regclass, p_ts timestamptz, p_cols text[])
RETURNS text
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    v  record;
    cl text := @extschema@._lin_collist(p_cols, 't');
BEGIN
    v := @extschema@._lin_versioning(p_tbl);
    RETURN format('SELECT %s FROM %s t WHERE t.%I <= %L::timestamptz AND t.%I > %L::timestamptz '
                  'UNION ALL '
                  'SELECT %s FROM %s t WHERE t.%I <= %L::timestamptz AND t.%I > %L::timestamptz',
                  cl, p_tbl, v.sys_start, p_ts, v.sys_end, p_ts,
                  cl, v.history_table, v.sys_start, p_ts, v.sys_end, p_ts);
END;
$$;

-- Row count and order-independent content hash of p_tbl AS OF p_ts.
CREATE FUNCTION @extschema@._lin_fingerprint(
    p_tbl regclass, p_ts timestamptz, p_cols text[],
    OUT row_count bigint, OUT content_hash text)
RETURNS record
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    s  numeric;
    m  numeric := 18446744073709551616;   -- 2^64
BEGIN
    EXECUTE format('SELECT count(*), coalesce(sum((''x'' || substr(md5(ROW(%s)::text), 1, 16))'
                   '::bit(64)::bigint::numeric), 0) FROM (%s) t',
                   @extschema@._lin_collist(p_cols, 't'),
                   @extschema@._lin_asof_sql(p_tbl, p_ts, p_cols))
        INTO row_count, s;
    s := s - floor(s / m) * m;
    IF s >= m / 2 THEN
        s := s - m;
    END IF;
    content_hash := lpad(to_hex(s::bigint), 16, '0');
END;
$$;

CREATE FUNCTION @extschema@._lin_snapshot_time(p_snapshot text)
RETURNS timestamptz
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    ts timestamptz;
BEGIN
    SELECT taken_at INTO ts FROM @extschema@.lineage_snapshot_catalog WHERE snapshot_name = p_snapshot;
    IF ts IS NULL THEN
        RAISE EXCEPTION 'snapshot "%" does not exist', p_snapshot;
    END IF;
    RETURN ts;
END;
$$;

-- ----------------------------------------------------------------
-- Snapshots
-- ----------------------------------------------------------------
CREATE FUNCTION @extschema@.lineage_snapshot(name text, tables regclass[], note text DEFAULT NULL)
RETURNS TABLE (table_name regclass, kind text, row_count bigint, content_hash text)
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    ts   timestamptz := clock_timestamp();
    t    regclass;
    v    record;
    cols text[];
    f    record;
BEGIN
    IF name IS NULL OR name = '' THEN
        RAISE EXCEPTION 'snapshot name must not be empty';
    END IF;
    IF tables IS NULL OR cardinality(tables) = 0 THEN
        RAISE EXCEPTION 'snapshot "%" must include at least one table', name;
    END IF;
    IF EXISTS (SELECT 1 FROM @extschema@.lineage_snapshot_catalog c WHERE c.snapshot_name = name) THEN
        RAISE EXCEPTION 'snapshot "%" already exists', name;
    END IF;
    INSERT INTO @extschema@.lineage_snapshot_catalog (snapshot_name, taken_at, note)
    VALUES (name, ts, note);
    FOR t IN SELECT DISTINCT u FROM unnest(tables) AS u LOOP
        v := @extschema@._lin_versioning(t);
        cols := @extschema@._lin_columns(t, ARRAY[v.sys_start, v.sys_end], false);
        f := @extschema@._lin_fingerprint(t, ts, cols);
        INSERT INTO @extschema@.lineage_snapshot_tables
            (snapshot_name, table_name, kind, hash_columns, row_count, content_hash)
        VALUES (name, t, v.kind, cols, f.row_count, f.content_hash);
    END LOOP;
    RETURN QUERY
        SELECT s.table_name, s.kind, s.row_count, s.content_hash
          FROM @extschema@.lineage_snapshot_tables s
         WHERE s.snapshot_name = name
         ORDER BY s.table_name::text;
END;
$$;

COMMENT ON FUNCTION @extschema@.lineage_snapshot(text, regclass[], text) IS
'Record a named snapshot (current time + per-table row count and content hash) of system-versioned tables';

CREATE FUNCTION @extschema@.lineage_snapshots()
RETURNS TABLE (snapshot_name text, taken_at timestamptz, note text, tables regclass[],
               total_rows bigint, models text[])
LANGUAGE sql STABLE
AS $$
    SELECT c.snapshot_name, c.taken_at, c.note,
           (SELECT array_agg(t.table_name ORDER BY t.table_name::text)
              FROM @extschema@.lineage_snapshot_tables t WHERE t.snapshot_name = c.snapshot_name),
           (SELECT sum(t.row_count)::bigint
              FROM @extschema@.lineage_snapshot_tables t WHERE t.snapshot_name = c.snapshot_name),
           (SELECT array_agg(m.model || ':' || m.version ORDER BY m.model, m.version)
              FROM @extschema@.lineage_model_links m WHERE m.snapshot_name = c.snapshot_name)
    FROM @extschema@.lineage_snapshot_catalog c
    ORDER BY c.taken_at, c.snapshot_name;
$$;

COMMENT ON FUNCTION @extschema@.lineage_snapshots() IS 'List snapshots with their tables and linked models';

CREATE FUNCTION @extschema@.lineage_snapshot_drop(name text)
RETURNS void
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    m text;
BEGIN
    PERFORM @extschema@._lin_snapshot_time(name);
    SELECT string_agg(l.model || ':' || l.version, ', ' ORDER BY l.model, l.version) INTO m
      FROM @extschema@.lineage_model_links l WHERE l.snapshot_name = name;
    IF m IS NOT NULL THEN
        RAISE EXCEPTION 'snapshot "%" is linked to model version(s) %', name, m
            USING HINT = 'Unlink the models first (lineage_unlink_model).';
    END IF;
    DELETE FROM @extschema@.lineage_snapshot_catalog c WHERE c.snapshot_name = name;
END;
$$;

COMMENT ON FUNCTION @extschema@.lineage_snapshot_drop(text) IS 'Forget a snapshot (data history is untouched)';

CREATE FUNCTION @extschema@.lineage_verify(name text)
RETURNS TABLE (table_name regclass, row_count_then bigint, row_count_now bigint,
               hash_then text, hash_now text, matches boolean)
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    ts timestamptz := @extschema@._lin_snapshot_time(name);
    s  record;
    f  record;
BEGIN
    FOR s IN SELECT * FROM @extschema@.lineage_snapshot_tables t
              WHERE t.snapshot_name = name ORDER BY t.table_name::text
    LOOP
        f := @extschema@._lin_fingerprint(s.table_name, ts, s.hash_columns);
        table_name := s.table_name;
        row_count_then := s.row_count;
        row_count_now := f.row_count;
        hash_then := s.content_hash;
        hash_now := f.content_hash;
        matches := s.row_count = f.row_count AND s.content_hash = f.content_hash;
        RETURN NEXT;
    END LOOP;
END;
$$;

COMMENT ON FUNCTION @extschema@.lineage_verify(text) IS
'Recompute each table''s fingerprint AS OF the snapshot time and compare with the recorded one';

-- ----------------------------------------------------------------
-- Model linkage
-- ----------------------------------------------------------------
CREATE FUNCTION @extschema@.lineage_link_model(
    model text, version text, snapshot text, metadata jsonb DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    cur text;
BEGIN
    IF model IS NULL OR version IS NULL THEN
        RAISE EXCEPTION 'model and version must not be NULL';
    END IF;
    PERFORM @extschema@._lin_snapshot_time(snapshot);
    SELECT l.snapshot_name INTO cur FROM @extschema@.lineage_model_links l
     WHERE l.model = lineage_link_model.model AND l.version = lineage_link_model.version;
    IF cur IS NOT NULL AND cur <> snapshot THEN
        RAISE EXCEPTION 'model % version % is already linked to snapshot "%"', model, version, cur
            USING HINT = 'A trained model version has exactly one training snapshot; use a new version.';
    END IF;
    INSERT INTO @extschema@.lineage_model_links AS l (model, version, snapshot_name, metadata)
    VALUES (model, version, snapshot, metadata)
    ON CONFLICT ON CONSTRAINT lineage_model_links_pkey
    DO UPDATE SET metadata = EXCLUDED.metadata;
END;
$$;

COMMENT ON FUNCTION @extschema@.lineage_link_model(text, text, text, jsonb) IS
'Record that a model version was trained on a snapshot';

CREATE FUNCTION @extschema@.lineage_unlink_model(model text, version text)
RETURNS boolean
LANGUAGE sql VOLATILE
AS $$
    WITH d AS (DELETE FROM @extschema@.lineage_model_links l
                WHERE l.model = lineage_unlink_model.model
                  AND l.version = lineage_unlink_model.version RETURNING 1)
    SELECT count(*) > 0 FROM d;
$$;

COMMENT ON FUNCTION @extschema@.lineage_unlink_model(text, text) IS 'Remove a model-to-snapshot link';

CREATE FUNCTION @extschema@.lineage_model_snapshot(model text, version text)
RETURNS text
LANGUAGE sql STABLE
AS $$
    SELECT l.snapshot_name FROM @extschema@.lineage_model_links l
     WHERE l.model = lineage_model_snapshot.model AND l.version = lineage_model_snapshot.version;
$$;

COMMENT ON FUNCTION @extschema@.lineage_model_snapshot(text, text) IS
'Snapshot a model version was trained on (NULL if not linked)';

CREATE FUNCTION @extschema@.lineage_read_snapshot(tbl anyelement, snapshot text)
RETURNS SETOF anyelement
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    rel regclass;
    ts  timestamptz := @extschema@._lin_snapshot_time(snapshot);
BEGIN
    SELECT t.typrelid::regclass INTO rel FROM pg_catalog.pg_type t
     WHERE t.oid = pg_catalog.pg_typeof(tbl) AND t.typrelid <> 0;
    IF rel IS NULL THEN
        RAISE EXCEPTION 'first argument must be a table row type, e.g. NULL::my_table';
    END IF;
    RETURN QUERY EXECUTE @extschema@._lin_asof_sql(rel, ts, @extschema@._lin_columns(rel, NULL, false));
END;
$$;

COMMENT ON FUNCTION @extschema@.lineage_read_snapshot(anyelement, text) IS
'Rows of a table exactly as they were at a snapshot; call as lineage_read_snapshot(NULL::tbl, ''name'')';

CREATE FUNCTION @extschema@.lineage_read(tbl regclass, model text, version text)
RETURNS SETOF jsonb
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    snap text := @extschema@.lineage_model_snapshot(model, version);
    v    record;
    cols text[];
BEGIN
    IF snap IS NULL THEN
        RAISE EXCEPTION 'model % version % is not linked to a snapshot', model, version;
    END IF;
    v := @extschema@._lin_versioning(tbl);
    cols := @extschema@._lin_columns(tbl, ARRAY[v.sys_start, v.sys_end], false);
    RETURN QUERY EXECUTE format('SELECT to_jsonb(t) FROM (%s) t',
        @extschema@._lin_asof_sql(tbl, @extschema@._lin_snapshot_time(snap), cols));
END;
$$;

COMMENT ON FUNCTION @extschema@.lineage_read(regclass, text, text) IS
'Rows (as jsonb, without system-time columns) of a table as they were when a model version was trained';

-- ----------------------------------------------------------------
-- Zero-copy branches
-- ----------------------------------------------------------------
CREATE FUNCTION @extschema@._lin_branch(p_branch text, p_tbl regclass)
RETURNS @extschema@.lineage_branch_catalog
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    b @extschema@.lineage_branch_catalog;
BEGIN
    SELECT * INTO b FROM @extschema@.lineage_branch_catalog c
     WHERE c.branch = p_branch AND c.base_table = p_tbl;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'branch "%" of % does not exist', p_branch, p_tbl;
    END IF;
    RETURN b;
END;
$$;

-- INSTEAD OF INSERT/UPDATE/DELETE on a branch view.
CREATE FUNCTION @extschema@.lineage_branch_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    b      @extschema@.lineage_branch_catalog;
    v      record;
    exists_ boolean;
    same_key boolean := false;
    pkcols text;
BEGIN
    SELECT * INTO b FROM @extschema@.lineage_branch_catalog c WHERE c.view_name = TG_RELID;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'view % is not a registered branch', TG_RELID::regclass;
    END IF;
    IF b.status <> 'active' THEN
        RAISE EXCEPTION 'branch "%" of % is % and read-only', b.branch, b.base_table, b.status;
    END IF;
    v := @extschema@._lin_versioning(b.base_table);
    pkcols := @extschema@._lin_collist(b.pk_columns, NULL);

    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        same_key := TG_OP = 'UPDATE' AND
            (SELECT jsonb_object_agg(k, to_jsonb(OLD) -> k) FROM unnest(b.pk_columns) k)
              = (SELECT jsonb_object_agg(k, to_jsonb(NEW) -> k) FROM unnest(b.pk_columns) k);
        -- the old image leaves the branch: drop its delta row ...
        EXECUTE format('DELETE FROM %s d WHERE %s', b.delta_table,
                       @extschema@._lin_keycond(b.pk_columns, 'd', '($1)'))
            USING OLD;
        -- ... and hide the base row, unless it is replaced under the same key
        IF NOT same_key THEN
            EXECUTE format('SELECT EXISTS (SELECT 1 FROM (%s) bp WHERE %s)',
                           @extschema@._lin_asof_sql(b.base_table, b.branch_point, b.pk_columns),
                           @extschema@._lin_keycond(b.pk_columns, 'bp', '($1)'))
                INTO exists_ USING OLD;
            IF exists_ THEN
                EXECUTE format('INSERT INTO %s (%s) SELECT %s FROM (SELECT ($1).*) s ON CONFLICT DO NOTHING',
                               b.tombstone_table, pkcols, pkcols)
                    USING OLD;
            END IF;
        END IF;
        IF TG_OP = 'DELETE' THEN
            RETURN OLD;
        END IF;
    END IF;

    -- INSERT, or the new image of an UPDATE that changes the key
    IF NOT same_key THEN
        EXECUTE format('SELECT EXISTS (SELECT 1 FROM %s x WHERE %s)', b.view_name,
                       @extschema@._lin_keycond(b.pk_columns, 'x', '($1)'))
            INTO exists_ USING NEW;
        IF exists_ THEN
            RAISE EXCEPTION 'duplicate key % in branch "%" of %',
                (SELECT jsonb_object_agg(k, to_jsonb(NEW) -> k) FROM unnest(b.pk_columns) k),
                b.branch, b.base_table
                USING ERRCODE = 'unique_violation';
        END IF;
        EXECUTE format('DELETE FROM %s x WHERE %s', b.tombstone_table,
                       @extschema@._lin_keycond(b.pk_columns, 'x', '($1)'))
            USING NEW;
    END IF;
    NEW := jsonb_populate_record(NEW, jsonb_build_object(v.sys_start, clock_timestamp(),
                                                         v.sys_end, 'infinity'));
    EXECUTE format('INSERT INTO %s SELECT ($1).*', b.delta_table) USING NEW;
    RETURN NEW;
END;
$$;

CREATE FUNCTION @extschema@.lineage_branch_create(
    branch text, tbl regclass, from_snapshot text DEFAULT NULL)
RETURNS regclass
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    v       record;
    nsp     name;
    rel     name;
    pk      text[];
    bp      timestamptz;
    vname   text;
    dname   text;
    tname   text;
    cols    text[];
    d       record;
    keyc    text;
BEGIN
    IF branch IS NULL OR branch = '' OR tbl IS NULL THEN
        RAISE EXCEPTION 'branch name and table must not be empty';
    END IF;
    IF EXISTS (SELECT 1 FROM @extschema@.lineage_branch_catalog c
                WHERE c.branch = lineage_branch_create.branch AND c.base_table = tbl) THEN
        RAISE EXCEPTION 'branch "%" of % already exists', branch, tbl;
    END IF;
    v := @extschema@._lin_versioning(tbl);
    SELECT array_agg(a.attname::text ORDER BY k.ord) INTO pk
      FROM pg_catalog.pg_index i
      CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
      JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
     WHERE i.indrelid = tbl AND i.indisprimary;
    IF pk IS NULL THEN
        RAISE EXCEPTION 'table % has no primary key', tbl
            USING HINT = 'Branches identify rows by primary key.';
    END IF;
    IF from_snapshot IS NOT NULL THEN
        bp := @extschema@._lin_snapshot_time(from_snapshot);
    ELSE
        bp := clock_timestamp();
    END IF;

    SELECT n.nspname, c.relname INTO nsp, rel
      FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     WHERE c.oid = tbl;
    IF octet_length(rel || '__' || branch || '__delta') > 63 THEN
        RAISE EXCEPTION 'branch name "%" is too long for table %', branch, tbl
            USING DETAIL = 'Generated object names must fit in 63 bytes.';
    END IF;
    vname := format('%I.%I', nsp, rel || '__' || branch);
    dname := format('%I.%I', nsp, rel || '__' || branch || '__delta');
    tname := format('%I.%I', nsp, rel || '__' || branch || '__tomb');

    cols := @extschema@._lin_columns(tbl, NULL, false);
    keyc := @extschema@._lin_collist(pk, NULL);

    -- delta: full rows written on the branch; tombstones: deleted keys
    EXECUTE format('CREATE TABLE %s (LIKE %s INCLUDING DEFAULTS)', dname, tbl);
    EXECUTE format('ALTER TABLE %s ADD PRIMARY KEY (%s)', dname, keyc);
    EXECUTE format('CREATE TABLE %s AS SELECT %s FROM %s WITH NO DATA', tname, keyc, tbl);
    EXECUTE format('ALTER TABLE %s ADD PRIMARY KEY (%s)', tname, keyc);

    EXECUTE format(
        'CREATE VIEW %s AS '
        'SELECT %s FROM (%s) b '
        ' WHERE NOT EXISTS (SELECT 1 FROM %s x WHERE %s) '
        '   AND NOT EXISTS (SELECT 1 FROM %s d WHERE %s) '
        'UNION ALL '
        'SELECT %s FROM %s d',
        vname,
        @extschema@._lin_collist(cols, 'b'),
        @extschema@._lin_asof_sql(tbl, bp, cols),
        tname, @extschema@._lin_keycond(pk, 'x', 'b'),
        dname, @extschema@._lin_keycond(pk, 'd', 'b'),
        @extschema@._lin_collist(cols, 'd'), dname);

    -- column defaults of the base apply to INSERTs into the branch
    FOR d IN SELECT a.attname, pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) AS expr
               FROM pg_catalog.pg_attrdef ad
               JOIN pg_catalog.pg_attribute a ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
              WHERE ad.adrelid = tbl AND a.attgenerated = ''
    LOOP
        EXECUTE format('ALTER VIEW %s ALTER COLUMN %I SET DEFAULT %s', vname, d.attname, d.expr);
    END LOOP;

    EXECUTE format('CREATE TRIGGER lineage_branch_write INSTEAD OF INSERT OR UPDATE OR DELETE ON %s '
                   'FOR EACH ROW EXECUTE FUNCTION @extschema@.lineage_branch_trigger()', vname);

    INSERT INTO @extschema@.lineage_branch_catalog
        (branch, base_table, branch_point, from_snapshot, view_name, delta_table,
         tombstone_table, pk_columns)
    VALUES (branch, tbl, bp, from_snapshot, vname::regclass, dname::regclass,
            tname::regclass, pk);
    RETURN vname::regclass;
END;
$$;

COMMENT ON FUNCTION @extschema@.lineage_branch_create(text, regclass, text) IS
'Create a writable zero-copy branch <tbl>__<branch> of a system-versioned table (overlay delta + tombstones)';

CREATE FUNCTION @extschema@.lineage_branch_diff(branch text, tbl regclass)
RETURNS TABLE (change text, key jsonb, base_row jsonb, branch_row jsonb)
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    b    @extschema@.lineage_branch_catalog := @extschema@._lin_branch(branch, tbl);
    v    record := @extschema@._lin_versioning(tbl);
    cols text[] := @extschema@._lin_columns(tbl, NULL, false);
BEGIN
    RETURN QUERY EXECUTE format(
        'WITH bp AS (SELECT %s AS k, to_jsonb(t) - $1 AS j FROM (%s) t), '
        '     d  AS (SELECT %s AS k, to_jsonb(t) - $1 AS j FROM %s t), '
        '     x  AS (SELECT %s AS k FROM %s t) '
        'SELECT ''added''::text, d.k, NULL::jsonb, d.j FROM d '
        ' WHERE NOT EXISTS (SELECT 1 FROM bp WHERE bp.k = d.k) '
        'UNION ALL '
        'SELECT ''changed'', d.k, bp.j, d.j FROM d JOIN bp ON bp.k = d.k WHERE bp.j IS DISTINCT FROM d.j '
        'UNION ALL '
        'SELECT ''deleted'', x.k, bp.j, NULL FROM x JOIN bp ON bp.k = x.k '
        'ORDER BY 1, 2',
        @extschema@._lin_keyexpr(b.pk_columns, 't'), @extschema@._lin_asof_sql(tbl, b.branch_point, cols),
        @extschema@._lin_keyexpr(b.pk_columns, 't'), b.delta_table,
        @extschema@._lin_keyexpr(b.pk_columns, 't'), b.tombstone_table)
        USING ARRAY[v.sys_start, v.sys_end];
END;
$$;

COMMENT ON FUNCTION @extschema@.lineage_branch_diff(text, regclass) IS
'Keys added, changed or deleted on a branch relative to the base at the branch point';

CREATE FUNCTION @extschema@.lineage_branch_merge(branch text, tbl regclass, force boolean DEFAULT false)
RETURNS TABLE (inserted bigint, updated bigint, deleted bigint)
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    b      @extschema@.lineage_branch_catalog := @extschema@._lin_branch(branch, tbl);
    v      record := @extschema@._lin_versioning(tbl);
    cols   text[] := @extschema@._lin_columns(tbl, NULL, false);
    sysc   text[];
    data   text[];
    upd    text;
    nconf  bigint;
    keys   text;
    cl     text;
BEGIN
    IF b.status <> 'active' THEN
        RAISE EXCEPTION 'branch "%" of % is already %', b.branch, tbl, b.status;
    END IF;
    PERFORM 1 FROM @extschema@.lineage_branch_catalog c
      WHERE c.branch = b.branch AND c.base_table = tbl FOR UPDATE;
    -- no concurrent base writes between conflict check and apply
    EXECUTE format('LOCK TABLE %s IN SHARE ROW EXCLUSIVE MODE', tbl);

    sysc := ARRAY[v.sys_start, v.sys_end];
    -- conflict: a key touched on the branch whose base row (data columns)
    -- differs now from what it was at the branch point
    EXECUTE format(
        'WITH touched AS (SELECT %s AS k FROM %s t UNION SELECT %s FROM %s t), '
        '     bp AS (SELECT %s AS k, to_jsonb(t) - $1 AS j FROM (%s) t), '
        '     cur AS (SELECT %s AS k, to_jsonb(t) - $1 AS j FROM %s t) '
        'SELECT count(*), string_agg(u.k::text, '', '' ORDER BY u.k) '
        '  FROM touched u LEFT JOIN bp ON bp.k = u.k LEFT JOIN cur ON cur.k = u.k '
        ' WHERE bp.j IS DISTINCT FROM cur.j',
        @extschema@._lin_keyexpr(b.pk_columns, 't'), b.delta_table,
        @extschema@._lin_keyexpr(b.pk_columns, 't'), b.tombstone_table,
        @extschema@._lin_keyexpr(b.pk_columns, 't'), @extschema@._lin_asof_sql(tbl, b.branch_point, cols),
        @extschema@._lin_keyexpr(b.pk_columns, 't'), tbl)
        INTO nconf, keys USING sysc;
    IF nconf > 0 AND NOT coalesce(force, false) THEN
        RAISE EXCEPTION 'merge conflict: % key(s) of % changed since the branch point of "%": %',
            nconf, tbl, b.branch, keys
            USING HINT = 'Inspect the changes, then merge with force => true to let the branch win.';
    END IF;

    -- counts
    EXECUTE format('SELECT count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM %s c WHERE %s)), '
                   '       count(*) FILTER (WHERE EXISTS (SELECT 1 FROM %s c WHERE %s)) FROM %s d',
                   tbl, @extschema@._lin_keycond(b.pk_columns, 'c', 'd'),
                   tbl, @extschema@._lin_keycond(b.pk_columns, 'c', 'd'), b.delta_table)
        INTO inserted, updated;

    -- deletions first (a key may be tombstoned and never re-added)
    EXECUTE format('DELETE FROM %s c USING %s x WHERE %s', tbl, b.tombstone_table,
                   @extschema@._lin_keycond(b.pk_columns, 'c', 'x'));
    GET DIAGNOSTICS deleted = ROW_COUNT;

    -- upsert delta rows; system-time columns are left to the base's triggers
    data := @extschema@._lin_columns(tbl, sysc, true);
    cl := @extschema@._lin_collist(data, NULL);
    SELECT string_agg(format('%I = EXCLUDED.%I', c, c), ', ') INTO upd
      FROM unnest(data) AS c WHERE NOT c = ANY (b.pk_columns);
    EXECUTE format('INSERT INTO %s (%s) OVERRIDING SYSTEM VALUE SELECT %s FROM %s d '
                   'ON CONFLICT (%s) DO %s',
                   tbl, cl, cl, b.delta_table,
                   @extschema@._lin_collist(b.pk_columns, NULL),
                   CASE WHEN upd IS NULL THEN 'NOTHING' ELSE 'UPDATE SET ' || upd END);

    UPDATE @extschema@.lineage_branch_catalog c
       SET status = 'merged', merged_at = clock_timestamp()
     WHERE c.branch = b.branch AND c.base_table = tbl;
    RETURN NEXT;
END;
$$;

COMMENT ON FUNCTION @extschema@.lineage_branch_merge(text, regclass, boolean) IS
'Apply a branch''s delta rows and tombstones to the base table in one transaction (conflicts raise unless force)';

CREATE FUNCTION @extschema@.lineage_branch_drop(branch text, tbl regclass)
RETURNS void
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    b @extschema@.lineage_branch_catalog := @extschema@._lin_branch(branch, tbl);
BEGIN
    EXECUTE format('DROP VIEW %s', b.view_name);
    EXECUTE format('DROP TABLE %s, %s', b.delta_table, b.tombstone_table);
    DELETE FROM @extschema@.lineage_branch_catalog c
     WHERE c.branch = b.branch AND c.base_table = tbl;
END;
$$;

COMMENT ON FUNCTION @extschema@.lineage_branch_drop(text, regclass) IS
'Drop a branch view and its delta/tombstone tables';

CREATE FUNCTION @extschema@.lineage_branches()
RETURNS TABLE (branch text, base_table regclass, view_name regclass, branch_point timestamptz,
               from_snapshot text, status text, delta_rows bigint, tombstones bigint)
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    b @extschema@.lineage_branch_catalog;
BEGIN
    FOR b IN SELECT * FROM @extschema@.lineage_branch_catalog c
              ORDER BY c.base_table::text, c.branch
    LOOP
        branch := b.branch; base_table := b.base_table; view_name := b.view_name;
        branch_point := b.branch_point; from_snapshot := b.from_snapshot; status := b.status;
        EXECUTE format('SELECT count(*) FROM %s', b.delta_table) INTO delta_rows;
        EXECUTE format('SELECT count(*) FROM %s', b.tombstone_table) INTO tombstones;
        RETURN NEXT;
    END LOOP;
END;
$$;

COMMENT ON FUNCTION @extschema@.lineage_branches() IS 'List branches with their delta and tombstone sizes';
