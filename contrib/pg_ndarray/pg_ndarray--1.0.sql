/* contrib/pg_ndarray/pg_ndarray--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_ndarray" to load this file. \quit

--
-- Catalog: one registry row per array, one row per stored chunk.
--
-- Chunks form a regular dense grid (HDF5 chunked layout / Zarr v2).  The
-- chunk index is the row-major (C order) linearisation of the chunk grid
-- coordinates.  Chunk payloads are little-endian, row-major element data
-- for the full chunk_shape (edge chunks are padded with fill_value),
-- optionally zstd-compressed.  Chunks that were never written are absent
-- and read back as fill_value.
--
CREATE SEQUENCE @extschema@.ndarray_id_seq;

CREATE TABLE @extschema@.ndarray (
    array_id    bigint PRIMARY KEY
                DEFAULT nextval('@extschema@.ndarray_id_seq'::regclass),
    name        text NOT NULL UNIQUE CHECK (name <> ''),
    dtype       text NOT NULL
                CHECK (dtype IN ('float32', 'float64', 'int32', 'int64')),
    shape       bigint[] NOT NULL,
    chunk_shape integer[] NOT NULL,
    codec       text NOT NULL DEFAULT 'zstd' CHECK (codec IN ('zstd', 'none')),
    fill_value  float8 NOT NULL DEFAULT 0,
    attrs       jsonb NOT NULL DEFAULT '{}',
    created_at  timestamptz NOT NULL DEFAULT now(),
    CHECK (array_ndims(shape) = 1 AND array_ndims(chunk_shape) = 1
           AND cardinality(shape) = cardinality(chunk_shape)
           AND cardinality(shape) BETWEEN 1 AND 8)
);

ALTER SEQUENCE @extschema@.ndarray_id_seq OWNED BY @extschema@.ndarray.array_id;

CREATE TABLE @extschema@.ndarray_chunk (
    array_id    bigint NOT NULL
                REFERENCES @extschema@.ndarray (array_id) ON DELETE CASCADE,
    chunk_idx   bigint NOT NULL CHECK (chunk_idx >= 0),
    raw_bytes   integer NOT NULL CHECK (raw_bytes > 0),
    data        bytea NOT NULL,
    PRIMARY KEY (array_id, chunk_idx)
);

-- Payloads are already compressed by the extension: keep TOAST from trying
-- to compress them again (out-of-line storage is still allowed).
ALTER TABLE @extschema@.ndarray_chunk ALTER COLUMN data SET STORAGE EXTERNAL;

SELECT pg_catalog.pg_extension_config_dump('@extschema@.ndarray', '');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.ndarray_chunk', '');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.ndarray_id_seq', '');

--
-- Array lifecycle
--
CREATE FUNCTION @extschema@.ndarray_create(
    name text,
    dtype text,
    shape bigint[],
    chunk_shape integer[],
    codec text DEFAULT 'zstd',
    fill_value float8 DEFAULT 0,
    attrs jsonb DEFAULT NULL)
RETURNS bigint
AS 'MODULE_PATHNAME', 'ndarray_create'
LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.ndarray_drop(name text, if_exists boolean DEFAULT false)
RETURNS boolean
LANGUAGE plpgsql VOLATILE STRICT
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
    DELETE FROM @extschema@.ndarray a WHERE a.name = $1;
    IF NOT FOUND THEN
        IF if_exists THEN
            RETURN false;
        END IF;
        RAISE EXCEPTION 'ndarray "%" does not exist', $1
            USING ERRCODE = 'undefined_object';
    END IF;
    RETURN true;
END;
$$;

CREATE FUNCTION @extschema@._ndarray_prod(bigint[])
RETURNS numeric
LANGUAGE plpgsql IMMUTABLE STRICT
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    r numeric := 1;
    x bigint;
BEGIN
    FOREACH x IN ARRAY $1 LOOP
        r := r * x;
    END LOOP;
    RETURN r;
END;
$$;

CREATE FUNCTION @extschema@._ndarray_grid(bigint[], integer[])
RETURNS bigint[]
LANGUAGE sql IMMUTABLE STRICT
SET search_path = pg_catalog, pg_temp
AS $$
    SELECT array_agg((s + c - 1) / c ORDER BY o)
    FROM unnest($1, $2) WITH ORDINALITY AS u(s, c, o)
$$;

CREATE FUNCTION @extschema@.ndarray_info(array_name text)
RETURNS TABLE (name text, dtype text, shape bigint[], chunk_shape integer[],
               chunk_grid bigint[], nelems numeric, codec text,
               fill_value float8, attrs jsonb, created_at timestamptz)
LANGUAGE plpgsql STABLE STRICT
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
    RETURN QUERY
    SELECT a.name, a.dtype, a.shape, a.chunk_shape,
           @extschema@._ndarray_grid(a.shape, a.chunk_shape),
           @extschema@._ndarray_prod(a.shape),
           a.codec, a.fill_value, a.attrs, a.created_at
    FROM @extschema@.ndarray a
    WHERE a.name = $1;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'ndarray "%" does not exist', $1
            USING ERRCODE = 'undefined_object';
    END IF;
END;
$$;

CREATE FUNCTION @extschema@.ndarray_list()
RETURNS TABLE (name text, dtype text, shape bigint[], chunk_shape integer[],
               codec text, fill_value float8, chunks_stored bigint)
LANGUAGE sql STABLE
SET search_path = pg_catalog, pg_temp
AS $$
    SELECT a.name, a.dtype, a.shape, a.chunk_shape, a.codec, a.fill_value,
           (SELECT count(*) FROM @extschema@.ndarray_chunk c
             WHERE c.array_id = a.array_id)
    FROM @extschema@.ndarray a
    ORDER BY a.name
$$;

CREATE FUNCTION @extschema@.ndarray_storage(name text)
RETURNS TABLE (chunks_total numeric, chunks_stored bigint, raw_bytes bigint,
               stored_bytes bigint, compression_ratio numeric)
LANGUAGE plpgsql STABLE STRICT
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_id bigint;
    v_grid bigint[];
BEGIN
    SELECT a.array_id, @extschema@._ndarray_grid(a.shape, a.chunk_shape)
      INTO v_id, v_grid
      FROM @extschema@.ndarray a WHERE a.name = $1;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'ndarray "%" does not exist', $1
            USING ERRCODE = 'undefined_object';
    END IF;
    RETURN QUERY
    SELECT @extschema@._ndarray_prod(v_grid),
           count(*),
           coalesce(sum(c.raw_bytes), 0)::bigint,
           coalesce(sum(octet_length(c.data)), 0)::bigint,
           CASE WHEN coalesce(sum(octet_length(c.data)), 0) = 0 THEN NULL
                ELSE round(sum(c.raw_bytes)::numeric
                           / sum(octet_length(c.data)), 2) END
    FROM @extschema@.ndarray_chunk c
    WHERE c.array_id = v_id;
END;
$$;

--
-- Writes (read-modify-write of the touched chunks, row-locked)
--
CREATE FUNCTION @extschema@.ndarray_write(name text, start bigint[], vals float8[])
RETURNS bigint
AS 'MODULE_PATHNAME', 'ndarray_write'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION @extschema@.ndarray_write_tensor(name text, start bigint[], t tensor)
RETURNS bigint
AS 'MODULE_PATHNAME', 'ndarray_write_tensor'
LANGUAGE C VOLATILE STRICT;

--
-- Reads / slicing (0-based start, exclusive stop, optional positive step)
--
CREATE FUNCTION @extschema@.ndarray_read(name text, start bigint[], stop bigint[],
                                         step integer[] DEFAULT NULL)
RETURNS float8[]
AS 'MODULE_PATHNAME', 'ndarray_read'
LANGUAGE C STABLE;

CREATE FUNCTION @extschema@.ndarray_read_tensor(name text, start bigint[], stop bigint[],
                                                step integer[] DEFAULT NULL)
RETURNS tensor
AS 'MODULE_PATHNAME', 'ndarray_read_tensor'
LANGUAGE C STABLE;

CREATE FUNCTION @extschema@.ndarray_get(name text, idx bigint[])
RETURNS float8
AS 'MODULE_PATHNAME', 'ndarray_get'
LANGUAGE C STABLE STRICT;

--
-- Server-side math
--
CREATE FUNCTION @extschema@.ndarray_reduce(name text, op text,
                                           start bigint[] DEFAULT NULL,
                                           stop bigint[] DEFAULT NULL)
RETURNS float8
AS 'MODULE_PATHNAME', 'ndarray_reduce'
LANGUAGE C STABLE;

CREATE FUNCTION @extschema@.ndarray_reduce_axis(name text, op text, axis integer)
RETURNS float8[]
AS 'MODULE_PATHNAME', 'ndarray_reduce_axis'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION @extschema@.ndarray_map(src text, dst text, op text,
                                        scalar float8 DEFAULT NULL,
                                        scalar2 float8 DEFAULT NULL,
                                        dtype text DEFAULT NULL)
RETURNS bigint
AS 'MODULE_PATHNAME', 'ndarray_map'
LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.ndarray_binary(a text, b text, dst text, op text,
                                           dtype text DEFAULT NULL)
RETURNS bigint
AS 'MODULE_PATHNAME', 'ndarray_binary'
LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.ndarray_matmul(a text, b text, dst text)
RETURNS bigint
AS 'MODULE_PATHNAME', 'ndarray_matmul'
LANGUAGE C VOLATILE STRICT;

--
-- Slicing helpers for the core tensor type
--
CREATE FUNCTION @extschema@.tensor_slice(t tensor, start integer[], stop integer[],
                                         step integer[] DEFAULT NULL)
RETURNS tensor
AS 'MODULE_PATHNAME', 'nd_tensor_slice'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION @extschema@.tensor_index(t tensor, idx integer[])
RETURNS float4
AS 'MODULE_PATHNAME', 'nd_tensor_index'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
