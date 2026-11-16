-- alohadb_geo regression test
-- Tests: type I/O, constructors, spatial predicates, operators

CREATE EXTENSION alohadb_geo;

-- -----------------------------------------------------------------------
-- geo_point I/O
-- -----------------------------------------------------------------------

SELECT '1.5,2.5'::geo_point;
SELECT 'POINT(3 4)'::geo_point;
SELECT ST_Point(5.0, 6.0);

-- -----------------------------------------------------------------------
-- geo_bbox I/O
-- -----------------------------------------------------------------------

SELECT '0,0,10,10'::geo_bbox;
SELECT 'BOX(0 0,10 10)'::geo_bbox;
SELECT ST_MakeBox(1.0, 2.0, 5.0, 6.0);

-- -----------------------------------------------------------------------
-- geo_circle I/O
-- -----------------------------------------------------------------------

SELECT '3,4,5'::geo_circle;
SELECT 'CIRCLE(3 4 5)'::geo_circle;
SELECT ST_Buffer('3.0,4.0'::geo_point, 5.0);

-- -----------------------------------------------------------------------
-- geo_polygon I/O
-- -----------------------------------------------------------------------

SELECT 'POLYGON((0 0,4 0,4 4,0 4,0 0))'::geo_polygon;

-- -----------------------------------------------------------------------
-- Euclidean distance
-- -----------------------------------------------------------------------

SELECT ST_Distance('0,0'::geo_point, '3,4'::geo_point) AS dist_5;
SELECT round(ST_Distance('0,0'::geo_point, '1,1'::geo_point)::numeric, 6) AS dist_sqrt2;

-- -----------------------------------------------------------------------
-- Haversine distance (lon, lat inputs)
-- NYC  (lon=-74.006, lat=40.7128)
-- LAX  (lon=-118.2437, lat=33.9425)
-- Approximate great-circle ~3940 km
-- -----------------------------------------------------------------------

SELECT round(
    ST_DistanceSphere(
        ST_Point(-74.006,  40.7128),   -- NYC
        ST_Point(-118.2437, 33.9425)   -- LAX
    )::numeric / 1000,
    0
) AS km_nyc_lax_approx;

-- -----------------------------------------------------------------------
-- Point-in-polygon (ray-casting, Franklin 1970)
-- -----------------------------------------------------------------------

-- Point inside unit square
SELECT ST_Contains(
    'POLYGON((0 0,1 0,1 1,0 1,0 0))'::geo_polygon,
    '0.5,0.5'::geo_point
) AS inside_true;

-- Point outside
SELECT ST_Contains(
    'POLYGON((0 0,1 0,1 1,0 1,0 0))'::geo_polygon,
    '2.0,2.0'::geo_point
) AS inside_false;

-- ST_Within is symmetric
SELECT ST_Within('0.5,0.5'::geo_point, 'POLYGON((0 0,1 0,1 1,0 1,0 0))'::geo_polygon) AS within_true;
SELECT ST_Within('2.0,2.0'::geo_point, 'POLYGON((0 0,1 0,1 1,0 1,0 0))'::geo_polygon) AS within_false;

-- -----------------------------------------------------------------------
-- Bounding-box intersection (AABB)
-- -----------------------------------------------------------------------

SELECT ST_Intersects('0,0,5,5'::geo_bbox, '3,3,8,8'::geo_bbox) AS overlaps_true;
SELECT ST_Intersects('0,0,2,2'::geo_bbox, '5,5,8,8'::geo_bbox) AS overlaps_false;
SELECT '0,0,5,5'::geo_bbox && '3,3,8,8'::geo_bbox AS operator_true;

-- -----------------------------------------------------------------------
-- Polygon area (shoelace formula, Gauss ~1795)
-- Unit square: area = 1
-- -----------------------------------------------------------------------

SELECT ST_Area('POLYGON((0 0,1 0,1 1,0 1,0 0))'::geo_polygon) AS area_1;
-- 3-4-5 right triangle: area = 6
SELECT abs(ST_Area('POLYGON((0 0,3 0,0 4,0 0))'::geo_polygon)) AS area_6;

-- -----------------------------------------------------------------------
-- Bounding box of polygon
-- -----------------------------------------------------------------------

SELECT ST_BoundingBox('POLYGON((1 2,5 2,5 8,1 8,1 2))'::geo_polygon) AS bbox;

-- -----------------------------------------------------------------------
-- ST_NearbyPoints plpgsql convenience function
-- -----------------------------------------------------------------------

CREATE TABLE locations (
    id     bigserial PRIMARY KEY,
    name   text,
    coords geo_point
);

INSERT INTO locations (name, coords) VALUES
    ('Home',    ST_Point(0.0, 0.0)),
    ('Near',    ST_Point(0.001, 0.001)),
    ('Far',     ST_Point(10.0, 10.0));

-- Search within 1000 metres of origin (should return Home and Near)
SELECT round(distance_m::numeric, 2) AS dist_m
FROM   ST_NearbyPoints(
           ST_Point(0.0, 0.0),
           1000.0,
           'locations',
           'coords',
           10
       )
ORDER BY 1;

DROP TABLE locations;
DROP EXTENSION alohadb_geo;
