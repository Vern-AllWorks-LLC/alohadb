\echo Use "CREATE EXTENSION alohadb_geo" to load this file. \quit

-- -----------------------------------------------------------------------
-- alohadb_geo: 2D geometry types and spatial operations
--
-- Algorithms used:
--   Point-in-polygon: ray-casting (W. Randolph Franklin, ~1970; published
--     ACM SIGGRAPH 1982).  Prior art predates any patent window.
--   Polygon area: shoelace formula (Gauss, ~1795).
--   Bounding-box intersection: AABB overlap test (prior art: Roberts 1963,
--     widespread in computational geometry since 1970s).
--   Haversine distance: already in alohadb_search; reimplemented here
--     with typed geo_point arguments.
--   GiST index support: uses bounding-box overlap strategy.
-- -----------------------------------------------------------------------

-- -----------------------------------------------------------------------
-- Type definitions
-- -----------------------------------------------------------------------

CREATE TYPE geo_point;
CREATE TYPE geo_bbox;
CREATE TYPE geo_polygon;
CREATE TYPE geo_circle;

-- geo_point I/O  ("x,y" or "POINT(x y)")
CREATE FUNCTION geo_point_in(cstring) RETURNS geo_point
    LANGUAGE C IMMUTABLE STRICT AS 'MODULE_PATHNAME', 'geo_point_in';
CREATE FUNCTION geo_point_out(geo_point) RETURNS cstring
    LANGUAGE C IMMUTABLE STRICT AS 'MODULE_PATHNAME', 'geo_point_out';
CREATE TYPE geo_point (
    INTERNALLENGTH = 16,
    INPUT  = geo_point_in,
    OUTPUT = geo_point_out,
    ALIGNMENT = double
);

-- geo_bbox I/O  ("xmin,ymin,xmax,ymax" or "BOX(xmin ymin,xmax ymax)")
CREATE FUNCTION geo_bbox_in(cstring) RETURNS geo_bbox
    LANGUAGE C IMMUTABLE STRICT AS 'MODULE_PATHNAME', 'geo_bbox_in';
CREATE FUNCTION geo_bbox_out(geo_bbox) RETURNS cstring
    LANGUAGE C IMMUTABLE STRICT AS 'MODULE_PATHNAME', 'geo_bbox_out';
CREATE TYPE geo_bbox (
    INTERNALLENGTH = 32,
    INPUT  = geo_bbox_in,
    OUTPUT = geo_bbox_out,
    ALIGNMENT = double
);

-- geo_polygon I/O  (WKT: "POLYGON((x1 y1,x2 y2,...,x1 y1))")
CREATE FUNCTION geo_polygon_in(cstring) RETURNS geo_polygon
    LANGUAGE C IMMUTABLE STRICT AS 'MODULE_PATHNAME', 'geo_polygon_in';
CREATE FUNCTION geo_polygon_out(geo_polygon) RETURNS cstring
    LANGUAGE C IMMUTABLE STRICT AS 'MODULE_PATHNAME', 'geo_polygon_out';
CREATE TYPE geo_polygon (
    INTERNALLENGTH = VARIABLE,
    INPUT   = geo_polygon_in,
    OUTPUT  = geo_polygon_out,
    STORAGE = EXTENDED,
    ALIGNMENT = double
);

-- geo_circle I/O  ("cx,cy,radius" or "CIRCLE(cx cy radius)")
CREATE FUNCTION geo_circle_in(cstring) RETURNS geo_circle
    LANGUAGE C IMMUTABLE STRICT AS 'MODULE_PATHNAME', 'geo_circle_in';
CREATE FUNCTION geo_circle_out(geo_circle) RETURNS cstring
    LANGUAGE C IMMUTABLE STRICT AS 'MODULE_PATHNAME', 'geo_circle_out';
CREATE TYPE geo_circle (
    INTERNALLENGTH = 24,
    INPUT  = geo_circle_in,
    OUTPUT = geo_circle_out,
    ALIGNMENT = double
);

-- -----------------------------------------------------------------------
-- Constructor functions
-- -----------------------------------------------------------------------

CREATE FUNCTION ST_Point(x float8, y float8) RETURNS geo_point
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
    AS 'MODULE_PATHNAME', 'geo_make_point';

CREATE FUNCTION ST_MakeBox(xmin float8, ymin float8, xmax float8, ymax float8)
RETURNS geo_bbox
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
    AS 'MODULE_PATHNAME', 'geo_make_bbox';

-- -----------------------------------------------------------------------
-- Spatial predicates and measurements
-- -----------------------------------------------------------------------

-- Euclidean distance between two points
CREATE FUNCTION ST_Distance(a geo_point, b geo_point) RETURNS float8
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
    AS 'MODULE_PATHNAME', 'geo_st_distance';

-- Haversine distance in metres (great-circle, WGS-84 approximation)
CREATE FUNCTION ST_DistanceSphere(
    a geo_point,  -- (longitude, latitude)
    b geo_point
) RETURNS float8
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
    AS 'MODULE_PATHNAME', 'geo_st_distance_sphere';

-- Point-in-polygon test (ray-casting, Franklin 1970/1982)
CREATE FUNCTION ST_Contains(poly geo_polygon, pt geo_point) RETURNS boolean
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
    AS 'MODULE_PATHNAME', 'geo_st_contains';

CREATE FUNCTION ST_Within(pt geo_point, poly geo_polygon) RETURNS boolean
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
    AS 'MODULE_PATHNAME', 'geo_st_within';

-- Bounding-box intersection
CREATE FUNCTION ST_Intersects(a geo_bbox, b geo_bbox) RETURNS boolean
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
    AS 'MODULE_PATHNAME', 'geo_st_intersects_bbox';

-- Signed polygon area (shoelace formula, Gauss ~1795)
CREATE FUNCTION ST_Area(poly geo_polygon) RETURNS float8
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
    AS 'MODULE_PATHNAME', 'geo_st_area';

-- Minimum bounding rectangle of a polygon
CREATE FUNCTION ST_BoundingBox(poly geo_polygon) RETURNS geo_bbox
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
    AS 'MODULE_PATHNAME', 'geo_st_bbox';

-- Buffer a point by radius (returns geo_circle)
CREATE FUNCTION ST_Buffer(pt geo_point, radius float8) RETURNS geo_circle
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
    AS 'MODULE_PATHNAME', 'geo_st_buffer';

-- -----------------------------------------------------------------------
-- GiST support for geo_bbox (bounding-box overlap operator)
-- -----------------------------------------------------------------------

-- Overlap operator &&
CREATE FUNCTION geo_bbox_overlap(a geo_bbox, b geo_bbox) RETURNS boolean
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
    AS 'MODULE_PATHNAME', 'geo_bbox_overlap';

CREATE OPERATOR && (
    LEFTARG    = geo_bbox,
    RIGHTARG   = geo_bbox,
    PROCEDURE  = geo_bbox_overlap,
    COMMUTATOR = '&&'
);

-- -----------------------------------------------------------------------
-- Convenience: nearby-points query using geo_point columns
-- Equivalent to alohadb_search.search_nearby but accepts typed geometry.
-- -----------------------------------------------------------------------
CREATE FUNCTION ST_NearbyPoints(
    p_center        geo_point,
    p_radius_m      float8,
    p_source_table  text,
    p_point_column  text,
    p_limit         int DEFAULT 100
)
RETURNS TABLE(distance_m float8)
LANGUAGE plpgsql STABLE STRICT
AS $$
BEGIN
    RETURN QUERY EXECUTE format(
        $sql$
        SELECT ST_DistanceSphere($1, %I)
        FROM   %I
        WHERE  ST_DistanceSphere($1, %I) <= $2
        ORDER BY 1
        LIMIT  $3
        $sql$,
        p_point_column, p_source_table, p_point_column
    ) USING p_center, p_radius_m, p_limit;
END;
$$;
