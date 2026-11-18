/* contrib/pg_graph/pg_graph--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_graph" to load this file. \quit

-- ================================================================
-- Catalog
--
-- A graph is a set of vertex labels (each backed by an ordinary table
-- with an integer key column) and edge labels (each backed by an
-- ordinary table with src/dst columns referencing vertex labels).
-- Vertex keys are read as bigint.  Keys should be unique across all
-- vertex labels of one graph (e.g. share a sequence); graph_validate()
-- reports collisions.
-- ================================================================

CREATE TABLE @extschema@.pg_graph_graphs (
    graph_name  text        PRIMARY KEY,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE @extschema@.pg_graph_vertex_labels (
    graph_name  text     NOT NULL
                REFERENCES @extschema@.pg_graph_graphs (graph_name)
                ON DELETE CASCADE,
    label       text     NOT NULL,
    table_name  regclass NOT NULL,
    key_column  name     NOT NULL,
    PRIMARY KEY (graph_name, label)
);

CREATE TABLE @extschema@.pg_graph_edge_labels (
    graph_name        text     NOT NULL,
    label             text     NOT NULL,
    table_name        regclass NOT NULL,
    src_column        name     NOT NULL,
    src_label         text     NOT NULL,
    dst_column        name     NOT NULL,
    dst_label         text     NOT NULL,
    weight_column     name,
    valid_from_column name,
    valid_to_column   name,
    directed          boolean  NOT NULL DEFAULT true,
    enforced          boolean  NOT NULL DEFAULT true,
    on_delete_cascade boolean  NOT NULL DEFAULT false,
    constraint_names  text[]   NOT NULL DEFAULT '{}',
    PRIMARY KEY (graph_name, label),
    FOREIGN KEY (graph_name)
        REFERENCES @extschema@.pg_graph_graphs (graph_name)
        ON DELETE CASCADE,
    FOREIGN KEY (graph_name, src_label)
        REFERENCES @extschema@.pg_graph_vertex_labels (graph_name, label)
        ON DELETE CASCADE,
    FOREIGN KEY (graph_name, dst_label)
        REFERENCES @extschema@.pg_graph_vertex_labels (graph_name, label)
        ON DELETE CASCADE
);

CREATE TABLE @extschema@.pg_graph_property_rules (
    graph_name      text    NOT NULL
                    REFERENCES @extschema@.pg_graph_graphs (graph_name)
                    ON DELETE CASCADE,
    label           text    NOT NULL,
    column_name     name    NOT NULL,
    required        boolean NOT NULL,
    check_expr      text,
    constraint_name text,
    PRIMARY KEY (graph_name, label, column_name)
);

COMMENT ON TABLE @extschema@.pg_graph_graphs IS
'pg_graph: registered property graphs';
COMMENT ON TABLE @extschema@.pg_graph_vertex_labels IS
'pg_graph: vertex labels (table + integer key column)';
COMMENT ON TABLE @extschema@.pg_graph_edge_labels IS
'pg_graph: edge labels (table + src/dst columns, optional weight and validity interval)';
COMMENT ON TABLE @extschema@.pg_graph_property_rules IS
'pg_graph: required-property / CHECK rules on labels';

SELECT pg_catalog.pg_extension_config_dump('@extschema@.pg_graph_graphs', '');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.pg_graph_vertex_labels', '');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.pg_graph_edge_labels', '');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.pg_graph_property_rules', '');

-- Everybody may read graph definitions (traversals read them as the
-- calling user); changing them requires explicit grants.
REVOKE ALL ON @extschema@.pg_graph_graphs, @extschema@.pg_graph_vertex_labels,
    @extschema@.pg_graph_edge_labels, @extschema@.pg_graph_property_rules FROM PUBLIC;
GRANT SELECT ON @extschema@.pg_graph_graphs, @extschema@.pg_graph_vertex_labels,
    @extschema@.pg_graph_edge_labels, @extschema@.pg_graph_property_rules TO PUBLIC;

-- ================================================================
-- Internal helpers
-- ================================================================

-- Return the type of a column, raising a clear error if it is missing.
CREATE FUNCTION @extschema@._graph_column_type(tbl regclass, col name)
RETURNS regtype
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    t regtype;
BEGIN
    SELECT a.atttypid::regtype INTO t
      FROM pg_catalog.pg_attribute a
     WHERE a.attrelid = tbl AND a.attname = col
       AND a.attnum > 0 AND NOT a.attisdropped;
    IF t IS NULL THEN
        RAISE EXCEPTION 'column "%" does not exist in table %', col, tbl;
    END IF;
    RETURN t;
END;
$$;

-- Build a constraint name that fits NAMEDATALEN.
CREATE FUNCTION @extschema@._graph_constraint_name(graph text, label text, suffix text)
RETURNS text
LANGUAGE sql IMMUTABLE
AS $$
    SELECT pg_catalog.left('graph_' || graph || '_' || label, 63 - pg_catalog.length(suffix) - 1)
           || '_' || suffix
$$;

-- ================================================================
-- Graph definition API
-- ================================================================

CREATE FUNCTION @extschema@.graph_create(name text)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF name IS NULL OR name = '' THEN
        RAISE EXCEPTION 'graph name must not be NULL or empty';
    END IF;
    IF EXISTS (SELECT 1 FROM @extschema@.pg_graph_graphs g WHERE g.graph_name = name) THEN
        RAISE EXCEPTION 'graph "%" already exists', name;
    END IF;
    INSERT INTO @extschema@.pg_graph_graphs (graph_name) VALUES (name);
END;
$$;

COMMENT ON FUNCTION @extschema@.graph_create(text) IS
'Create an empty property graph definition';

CREATE FUNCTION @extschema@.graph_add_vertex_label(
    graph      text,
    label      text,
    table_name regclass,
    key_column name DEFAULT 'id')
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    ktype regtype;
    attno int2;
BEGIN
    IF graph IS NULL OR label IS NULL OR table_name IS NULL OR key_column IS NULL THEN
        RAISE EXCEPTION 'graph, label, table_name and key_column must not be NULL';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM @extschema@.pg_graph_graphs g WHERE g.graph_name = graph) THEN
        RAISE EXCEPTION 'graph "%" does not exist', graph;
    END IF;
    IF EXISTS (SELECT 1 FROM @extschema@.pg_graph_vertex_labels v
                WHERE v.graph_name = graph AND v.label = graph_add_vertex_label.label)
       OR EXISTS (SELECT 1 FROM @extschema@.pg_graph_edge_labels e
                WHERE e.graph_name = graph AND e.label = graph_add_vertex_label.label) THEN
        RAISE EXCEPTION 'label "%" already exists in graph "%"', label, graph;
    END IF;

    ktype := @extschema@._graph_column_type(table_name, key_column);
    IF ktype NOT IN ('bigint'::regtype, 'integer'::regtype, 'smallint'::regtype) THEN
        RAISE EXCEPTION 'vertex key column %.% must be bigint, integer or smallint (is %)',
            table_name, key_column, ktype;
    END IF;

    SELECT a.attnum INTO attno FROM pg_catalog.pg_attribute a
     WHERE a.attrelid = table_name AND a.attname = key_column;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint c
                    WHERE c.conrelid = table_name AND c.contype IN ('p', 'u')
                      AND c.conkey = ARRAY[attno]) THEN
        RAISE EXCEPTION 'vertex key column %.% must have a PRIMARY KEY or UNIQUE constraint',
            table_name, key_column
            USING HINT = 'Edges reference vertex keys through foreign keys.';
    END IF;

    INSERT INTO @extschema@.pg_graph_vertex_labels
        VALUES (graph, label, table_name, key_column);
END;
$$;

COMMENT ON FUNCTION @extschema@.graph_add_vertex_label(text, text, regclass, name) IS
'Register a table as a vertex label; the key column must be an integer type with a PK/UNIQUE constraint';

CREATE FUNCTION @extschema@.graph_add_edge_label(
    graph             text,
    label             text,
    table_name        regclass,
    src_column        name,
    src_label         text,
    dst_column        name,
    dst_label         text,
    weight_column     name    DEFAULT NULL,
    valid_from_column name    DEFAULT NULL,
    valid_to_column   name    DEFAULT NULL,
    directed          boolean DEFAULT true,
    enforce           boolean DEFAULT true,
    on_delete_cascade boolean DEFAULT false)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    sv     record;
    dv     record;
    t      regtype;
    cname  text;
    cnames text[] := '{}';
    del    text;
BEGIN
    IF graph IS NULL OR label IS NULL OR table_name IS NULL OR src_column IS NULL
       OR src_label IS NULL OR dst_column IS NULL OR dst_label IS NULL THEN
        RAISE EXCEPTION 'graph, label, table_name, src_column, src_label, dst_column and dst_label must not be NULL';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM @extschema@.pg_graph_graphs g WHERE g.graph_name = graph) THEN
        RAISE EXCEPTION 'graph "%" does not exist', graph;
    END IF;
    IF EXISTS (SELECT 1 FROM @extschema@.pg_graph_vertex_labels v
                WHERE v.graph_name = graph AND v.label = graph_add_edge_label.label)
       OR EXISTS (SELECT 1 FROM @extschema@.pg_graph_edge_labels e
                WHERE e.graph_name = graph AND e.label = graph_add_edge_label.label) THEN
        RAISE EXCEPTION 'label "%" already exists in graph "%"', label, graph;
    END IF;

    SELECT * INTO sv FROM @extschema@.pg_graph_vertex_labels v
     WHERE v.graph_name = graph AND v.label = src_label;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'vertex label "%" does not exist in graph "%"', src_label, graph;
    END IF;
    SELECT * INTO dv FROM @extschema@.pg_graph_vertex_labels v
     WHERE v.graph_name = graph AND v.label = dst_label;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'vertex label "%" does not exist in graph "%"', dst_label, graph;
    END IF;

    FOREACH t IN ARRAY ARRAY[@extschema@._graph_column_type(table_name, src_column),
                             @extschema@._graph_column_type(table_name, dst_column)] LOOP
        IF t NOT IN ('bigint'::regtype, 'integer'::regtype, 'smallint'::regtype) THEN
            RAISE EXCEPTION 'edge endpoint columns must be bigint, integer or smallint (found %)', t;
        END IF;
    END LOOP;
    IF weight_column IS NOT NULL THEN
        t := @extschema@._graph_column_type(table_name, weight_column);
        IF (SELECT typcategory FROM pg_catalog.pg_type WHERE oid = t) <> 'N' THEN
            RAISE EXCEPTION 'weight column must be numeric (found %)', t;
        END IF;
    END IF;
    IF valid_from_column IS NOT NULL THEN
        t := @extschema@._graph_column_type(table_name, valid_from_column);
        IF t NOT IN ('timestamptz'::regtype, 'timestamp'::regtype, 'date'::regtype) THEN
            RAISE EXCEPTION 'valid_from column must be timestamptz, timestamp or date (found %)', t;
        END IF;
    END IF;
    IF valid_to_column IS NOT NULL THEN
        t := @extschema@._graph_column_type(table_name, valid_to_column);
        IF t NOT IN ('timestamptz'::regtype, 'timestamp'::regtype, 'date'::regtype) THEN
            RAISE EXCEPTION 'valid_to column must be timestamptz, timestamp or date (found %)', t;
        END IF;
    END IF;

    IF coalesce(enforce, true) THEN
        del := CASE WHEN coalesce(on_delete_cascade, false) THEN 'CASCADE' ELSE 'NO ACTION' END;

        EXECUTE format('ALTER TABLE %s ALTER COLUMN %I SET NOT NULL, ALTER COLUMN %I SET NOT NULL',
                       table_name, src_column, dst_column);

        cname := @extschema@._graph_constraint_name(graph, label, 'src_fk');
        EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %I FOREIGN KEY (%I) REFERENCES %s (%I) ON DELETE %s',
                       table_name, cname, src_column, sv.table_name, sv.key_column, del);
        cnames := cnames || cname;

        cname := @extschema@._graph_constraint_name(graph, label, 'dst_fk');
        EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %I FOREIGN KEY (%I) REFERENCES %s (%I) ON DELETE %s',
                       table_name, cname, dst_column, dv.table_name, dv.key_column, del);
        cnames := cnames || cname;

        IF valid_from_column IS NOT NULL AND valid_to_column IS NOT NULL THEN
            cname := @extschema@._graph_constraint_name(graph, label, 'valid_ck');
            EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %I CHECK (%I < %I)',
                           table_name, cname, valid_from_column, valid_to_column);
            cnames := cnames || cname;
        END IF;
    END IF;

    INSERT INTO @extschema@.pg_graph_edge_labels
        VALUES (graph, label, table_name, src_column, src_label, dst_column, dst_label,
                weight_column, valid_from_column, valid_to_column,
                coalesce(directed, true), coalesce(enforce, true),
                coalesce(on_delete_cascade, false), cnames);
END;
$$;

COMMENT ON FUNCTION @extschema@.graph_add_edge_label(text, text, regclass, name, text, name, text, name, name, name, boolean, boolean, boolean) IS
'Register a table as an edge label; with enforce => true installs NOT NULL + FOREIGN KEY constraints to the vertex tables';

CREATE FUNCTION @extschema@.graph_add_property_rule(
    graph       text,
    label       text,
    column_name name,
    required    boolean DEFAULT true,
    check_expr  text    DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    tbl   regclass;
    cname text;
BEGIN
    IF graph IS NULL OR label IS NULL OR column_name IS NULL THEN
        RAISE EXCEPTION 'graph, label and column_name must not be NULL';
    END IF;
    SELECT v.table_name INTO tbl FROM @extschema@.pg_graph_vertex_labels v
     WHERE v.graph_name = graph AND v.label = graph_add_property_rule.label;
    IF tbl IS NULL THEN
        SELECT e.table_name INTO tbl FROM @extschema@.pg_graph_edge_labels e
         WHERE e.graph_name = graph AND e.label = graph_add_property_rule.label;
    END IF;
    IF tbl IS NULL THEN
        RAISE EXCEPTION 'label "%" does not exist in graph "%"', label, graph;
    END IF;
    PERFORM @extschema@._graph_column_type(tbl, column_name);

    IF check_expr IS NOT NULL AND pg_catalog.strpos(check_expr, ';') > 0 THEN
        RAISE EXCEPTION 'check_expr must be a single SQL expression (no ";")';
    END IF;

    IF coalesce(required, false) THEN
        EXECUTE format('ALTER TABLE %s ALTER COLUMN %I SET NOT NULL', tbl, column_name);
    END IF;
    IF check_expr IS NOT NULL THEN
        cname := @extschema@._graph_constraint_name(graph, label || '_' || column_name, 'ck');
        EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %I CHECK (%s)', tbl, cname, check_expr);
    END IF;

    INSERT INTO @extschema@.pg_graph_property_rules
        VALUES (graph, label, column_name, coalesce(required, false), check_expr, cname);
END;
$$;

COMMENT ON FUNCTION @extschema@.graph_add_property_rule(text, text, name, boolean, text) IS
'Require a property (NOT NULL) and/or install a CHECK constraint on a vertex or edge label';

CREATE FUNCTION @extschema@.graph_drop(name text, keep_constraints boolean DEFAULT false)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    r record;
    c text;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM @extschema@.pg_graph_graphs g WHERE g.graph_name = name) THEN
        RAISE EXCEPTION 'graph "%" does not exist', name;
    END IF;
    IF NOT coalesce(keep_constraints, false) THEN
        FOR r IN SELECT e.table_name, e.constraint_names
                   FROM @extschema@.pg_graph_edge_labels e WHERE e.graph_name = name LOOP
            FOREACH c IN ARRAY r.constraint_names LOOP
                EXECUTE format('ALTER TABLE %s DROP CONSTRAINT IF EXISTS %I', r.table_name, c);
            END LOOP;
        END LOOP;
        FOR r IN SELECT coalesce(v.table_name, e.table_name) AS table_name, p.constraint_name
                   FROM @extschema@.pg_graph_property_rules p
                   LEFT JOIN @extschema@.pg_graph_vertex_labels v
                          ON v.graph_name = p.graph_name AND v.label = p.label
                   LEFT JOIN @extschema@.pg_graph_edge_labels e
                          ON e.graph_name = p.graph_name AND e.label = p.label
                  WHERE p.graph_name = name AND p.constraint_name IS NOT NULL LOOP
            IF r.table_name IS NOT NULL THEN
                EXECUTE format('ALTER TABLE %s DROP CONSTRAINT IF EXISTS %I',
                               r.table_name, r.constraint_name);
            END IF;
        END LOOP;
    END IF;
    DELETE FROM @extschema@.pg_graph_graphs g WHERE g.graph_name = name;
END;
$$;

COMMENT ON FUNCTION @extschema@.graph_drop(text, boolean) IS
'Drop a graph definition and (unless keep_constraints) the FK/CHECK constraints it installed; data tables are untouched';

CREATE FUNCTION @extschema@.graph_describe(name text)
RETURNS TABLE(kind text, label text, table_name regclass, definition jsonb)
LANGUAGE plpgsql STABLE
AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM @extschema@.pg_graph_graphs g WHERE g.graph_name = name) THEN
        RAISE EXCEPTION 'graph "%" does not exist', name;
    END IF;
    RETURN QUERY
    SELECT 'vertex'::text, v.label, v.table_name,
           jsonb_build_object('key_column', v.key_column)
      FROM @extschema@.pg_graph_vertex_labels v
     WHERE v.graph_name = name
    UNION ALL
    SELECT 'edge'::text, e.label, e.table_name,
           jsonb_strip_nulls(jsonb_build_object(
               'src_column', e.src_column, 'src_label', e.src_label,
               'dst_column', e.dst_column, 'dst_label', e.dst_label,
               'weight_column', e.weight_column,
               'valid_from_column', e.valid_from_column,
               'valid_to_column', e.valid_to_column,
               'directed', e.directed, 'enforced', e.enforced,
               'on_delete_cascade', e.on_delete_cascade,
               'constraints', to_jsonb(e.constraint_names)))
      FROM @extschema@.pg_graph_edge_labels e
     WHERE e.graph_name = name
    UNION ALL
    SELECT 'property_rule'::text, p.label,
           coalesce(v.table_name, e.table_name),
           jsonb_strip_nulls(jsonb_build_object(
               'column', p.column_name, 'required', p.required,
               'check', p.check_expr, 'constraint', p.constraint_name))
      FROM @extschema@.pg_graph_property_rules p
      LEFT JOIN @extschema@.pg_graph_vertex_labels v
             ON v.graph_name = p.graph_name AND v.label = p.label
      LEFT JOIN @extschema@.pg_graph_edge_labels e
             ON e.graph_name = p.graph_name AND e.label = p.label
     WHERE p.graph_name = name
    ORDER BY 1 DESC, 2;
END;
$$;

COMMENT ON FUNCTION @extschema@.graph_describe(text) IS
'List the vertex labels, edge labels and property rules of a graph';

CREATE FUNCTION @extschema@.graph_validate(name text)
RETURNS TABLE(kind text, label text, detail text, violations bigint)
LANGUAGE plpgsql
AS $$
DECLARE
    e     record;
    v     record;
    v2    record;
    p     record;
    tbl   regclass;
    n     bigint;
    n2    bigint;
    endp  record;
    other text;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM @extschema@.pg_graph_graphs g WHERE g.graph_name = name) THEN
        RAISE EXCEPTION 'graph "%" does not exist', name;
    END IF;

    -- Vertex labels: NULL keys and key collisions between labels.
    FOR v IN SELECT * FROM @extschema@.pg_graph_vertex_labels x
              WHERE x.graph_name = name ORDER BY x.label LOOP
        EXECUTE format('SELECT count(*) FROM %s WHERE %I IS NULL', v.table_name, v.key_column) INTO n;
        IF n > 0 THEN
            kind := 'null_key'; label := v.label;
            detail := format('%s.%I is NULL', v.table_name, v.key_column);
            violations := n; RETURN NEXT;
        END IF;
        FOR v2 IN SELECT * FROM @extschema@.pg_graph_vertex_labels x
                   WHERE x.graph_name = name AND x.label > v.label ORDER BY x.label LOOP
            EXECUTE format('SELECT count(*) FROM %s a JOIN %s b ON a.%I::int8 = b.%I::int8',
                           v.table_name, v2.table_name, v.key_column, v2.key_column) INTO n;
            IF n > 0 THEN
                kind := 'key_collision'; label := v.label;
                detail := format('keys shared with vertex label %s', v2.label);
                violations := n; RETURN NEXT;
            END IF;
        END LOOP;
    END LOOP;

    -- Edge labels: NULL / dangling / wrongly-labelled endpoints, bad intervals, negative weights.
    FOR e IN SELECT * FROM @extschema@.pg_graph_edge_labels x
              WHERE x.graph_name = name ORDER BY x.label LOOP
        FOR endp IN SELECT 'src' AS side, e.src_column AS col, e.src_label AS vlabel
                    UNION ALL
                    SELECT 'dst', e.dst_column, e.dst_label LOOP
            EXECUTE format('SELECT count(*) FROM %s WHERE %I IS NULL', e.table_name, endp.col) INTO n;
            IF n > 0 THEN
                kind := 'null_' || endp.side; label := e.label;
                detail := format('%s.%I is NULL', e.table_name, endp.col);
                violations := n; RETURN NEXT;
            END IF;

            SELECT * INTO v FROM @extschema@.pg_graph_vertex_labels x
             WHERE x.graph_name = name AND x.label = endp.vlabel;

            -- expression that is true when the endpoint exists under another label
            SELECT coalesce(string_agg(format('EXISTS (SELECT 1 FROM %s o WHERE o.%I::int8 = t.%I::int8)',
                                              x.table_name, x.key_column, endp.col), ' OR '), 'false')
              INTO other
              FROM @extschema@.pg_graph_vertex_labels x
             WHERE x.graph_name = name AND x.label <> endp.vlabel;

            EXECUTE format('SELECT count(*) FILTER (WHERE NOT (%s)), count(*) FILTER (WHERE %s) '
                           'FROM %s t WHERE t.%I IS NOT NULL AND NOT EXISTS '
                           '(SELECT 1 FROM %s k WHERE k.%I::int8 = t.%I::int8)',
                           other, other, e.table_name, endp.col,
                           v.table_name, v.key_column, endp.col)
               INTO n, n2;
            IF n > 0 THEN
                kind := 'dangling_' || endp.side; label := e.label;
                detail := format('%s.%I not found in any vertex label', e.table_name, endp.col);
                violations := n; RETURN NEXT;
            END IF;
            IF n2 > 0 THEN
                kind := 'wrong_label_' || endp.side; label := e.label;
                detail := format('%s.%I found under another vertex label, expected %s',
                                 e.table_name, endp.col, endp.vlabel);
                violations := n2; RETURN NEXT;
            END IF;
        END LOOP;

        IF e.valid_from_column IS NOT NULL AND e.valid_to_column IS NOT NULL THEN
            EXECUTE format('SELECT count(*) FROM %s WHERE %I >= %I',
                           e.table_name, e.valid_from_column, e.valid_to_column) INTO n;
            IF n > 0 THEN
                kind := 'invalid_interval'; label := e.label;
                detail := format('%I >= %I', e.valid_from_column, e.valid_to_column);
                violations := n; RETURN NEXT;
            END IF;
        END IF;
        IF e.weight_column IS NOT NULL THEN
            EXECUTE format('SELECT count(*) FROM %s WHERE %I < 0', e.table_name, e.weight_column) INTO n;
            IF n > 0 THEN
                kind := 'negative_weight'; label := e.label;
                detail := format('%I < 0 (shortest paths require non-negative weights)', e.weight_column);
                violations := n; RETURN NEXT;
            END IF;
        END IF;
    END LOOP;

    -- Property rules.
    FOR p IN SELECT r.*, coalesce(vl.table_name, el.table_name) AS tbl
               FROM @extschema@.pg_graph_property_rules r
               LEFT JOIN @extschema@.pg_graph_vertex_labels vl
                      ON vl.graph_name = r.graph_name AND vl.label = r.label
               LEFT JOIN @extschema@.pg_graph_edge_labels el
                      ON el.graph_name = r.graph_name AND el.label = r.label
              WHERE r.graph_name = name ORDER BY r.label, r.column_name LOOP
        IF p.required THEN
            EXECUTE format('SELECT count(*) FROM %s WHERE %I IS NULL', p.tbl, p.column_name) INTO n;
            IF n > 0 THEN
                kind := 'missing_required'; label := p.label;
                detail := format('%I is NULL', p.column_name);
                violations := n; RETURN NEXT;
            END IF;
        END IF;
        IF p.check_expr IS NOT NULL THEN
            EXECUTE format('SELECT count(*) FROM %s WHERE NOT (%s)', p.tbl, p.check_expr) INTO n;
            IF n > 0 THEN
                kind := 'check_failed'; label := p.label;
                detail := format('%I: %s', p.column_name, p.check_expr);
                violations := n; RETURN NEXT;
            END IF;
        END IF;
    END LOOP;
END;
$$;

COMMENT ON FUNCTION @extschema@.graph_validate(text) IS
'Report schema violations in existing data (NULL/dangling/wrong-label endpoints, key collisions, bad intervals, negative weights, property rules)';

-- ================================================================
-- Traversal and algorithms (C)
--
-- None of these are STRICT: NULL optional arguments mean "all edge
-- labels" / "no point-in-time filter" / "random seed".
-- ================================================================

CREATE FUNCTION @extschema@.graph_neighbors(
    graph       text,
    vertex      bigint,
    edge_labels text[]      DEFAULT NULL,
    direction   text        DEFAULT 'out',
    as_of       timestamptz DEFAULT NULL)
RETURNS TABLE(neighbor bigint, edge_label text, edge_direction text,
              weight float8, valid_from timestamptz, valid_to timestamptz)
AS 'MODULE_PATHNAME', 'graph_neighbors'
LANGUAGE C STABLE PARALLEL RESTRICTED;

CREATE FUNCTION @extschema@.graph_bfs(
    graph       text,
    start       bigint,
    max_depth   int         DEFAULT 3,
    edge_labels text[]      DEFAULT NULL,
    as_of       timestamptz DEFAULT NULL,
    direction   text        DEFAULT 'out')
RETURNS TABLE(vertex bigint, depth int, path bigint[])
AS 'MODULE_PATHNAME', 'graph_bfs'
LANGUAGE C STABLE PARALLEL RESTRICTED;

CREATE FUNCTION @extschema@.graph_shortest_path(
    graph       text,
    src         bigint,
    dst         bigint,
    edge_labels text[]      DEFAULT NULL,
    as_of       timestamptz DEFAULT NULL,
    direction   text        DEFAULT 'out')
RETURNS TABLE(step int, vertex bigint, cost float8)
AS 'MODULE_PATHNAME', 'graph_shortest_path'
LANGUAGE C STABLE PARALLEL RESTRICTED;

CREATE FUNCTION @extschema@.graph_temporal_paths(
    graph       text,
    start       bigint,
    start_time  timestamptz,
    max_hops    int         DEFAULT 10,
    edge_labels text[]      DEFAULT NULL,
    direction   text        DEFAULT 'out')
RETURNS TABLE(vertex bigint, arrival timestamptz, hops int, path bigint[])
AS 'MODULE_PATHNAME', 'graph_temporal_paths'
LANGUAGE C STABLE PARALLEL RESTRICTED;

CREATE FUNCTION @extschema@.graph_pagerank(
    graph       text,
    damping     float8      DEFAULT 0.85,
    iterations  int         DEFAULT 20,
    edge_labels text[]      DEFAULT NULL,
    as_of       timestamptz DEFAULT NULL)
RETURNS TABLE(vertex bigint, rank float8)
AS 'MODULE_PATHNAME', 'graph_pagerank'
LANGUAGE C STABLE PARALLEL RESTRICTED;

CREATE FUNCTION @extschema@.graph_connected_components(
    graph       text,
    edge_labels text[]      DEFAULT NULL,
    as_of       timestamptz DEFAULT NULL)
RETURNS TABLE(vertex bigint, component bigint)
AS 'MODULE_PATHNAME', 'graph_connected_components'
LANGUAGE C STABLE PARALLEL RESTRICTED;

CREATE FUNCTION @extschema@.graph_degree(
    graph       text,
    edge_labels text[]      DEFAULT NULL,
    as_of       timestamptz DEFAULT NULL)
RETURNS TABLE(vertex bigint, in_degree bigint, out_degree bigint, degree bigint)
AS 'MODULE_PATHNAME', 'graph_degree'
LANGUAGE C STABLE PARALLEL RESTRICTED;

CREATE FUNCTION @extschema@.graph_sample_neighbors(
    graph       text,
    seeds       bigint[],
    fanouts     int[],
    edge_labels text[]      DEFAULT NULL,
    as_of       timestamptz DEFAULT NULL,
    seed        int         DEFAULT NULL,
    direction   text        DEFAULT 'out')
RETURNS TABLE(hop int, src bigint, dst bigint)
AS 'MODULE_PATHNAME', 'graph_sample_neighbors'
LANGUAGE C VOLATILE PARALLEL RESTRICTED;

CREATE FUNCTION @extschema@.graph_node_features(
    graph          text,
    vertices       bigint[],
    feature_column text,
    edge_labels    text[]      DEFAULT NULL,
    as_of          timestamptz DEFAULT NULL,
    direction      text        DEFAULT 'out')
RETURNS TABLE(vertex bigint, degree int, feature_count int, feature_mean float8[])
AS 'MODULE_PATHNAME', 'graph_node_features'
LANGUAGE C STABLE PARALLEL RESTRICTED;

COMMENT ON FUNCTION @extschema@.graph_neighbors(text, bigint, text[], text, timestamptz) IS
'Edges incident to a vertex (direction out/in/both), optionally as of a point in time';
COMMENT ON FUNCTION @extschema@.graph_bfs(text, bigint, int, text[], timestamptz, text) IS
'Breadth-first traversal: every reachable vertex with its depth and a shortest-hop path';
COMMENT ON FUNCTION @extschema@.graph_shortest_path(text, bigint, bigint, text[], timestamptz, text) IS
'Dijkstra shortest path on the weight column (hop count when no weight column)';
COMMENT ON FUNCTION @extschema@.graph_temporal_paths(text, bigint, timestamptz, int, text[], text) IS
'Earliest-arrival time-respecting paths (edge times = valid_from, non-decreasing along the path)';
COMMENT ON FUNCTION @extschema@.graph_pagerank(text, float8, int, text[], timestamptz) IS
'PageRank by power iteration; dangling mass is redistributed uniformly';
COMMENT ON FUNCTION @extschema@.graph_connected_components(text, text[], timestamptz) IS
'Weakly connected components (union-find); component = smallest vertex key in the component';
COMMENT ON FUNCTION @extschema@.graph_degree(text, text[], timestamptz) IS
'In/out/total degree of every vertex';
COMMENT ON FUNCTION @extschema@.graph_sample_neighbors(text, bigint[], int[], text[], timestamptz, int, text) IS
'GraphSAGE-style layer-wise uniform neighbor sampling without replacement';
COMMENT ON FUNCTION @extschema@.graph_node_features(text, bigint[], text, text[], timestamptz, text) IS
'Per-vertex degree and mean of a numeric / numeric-array / vector property over distinct neighbors';
