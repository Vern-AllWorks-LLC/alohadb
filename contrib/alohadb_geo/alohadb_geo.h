#ifndef ALOHADB_GEO_H
#define ALOHADB_GEO_H

#include "postgres.h"
#include "fmgr.h"

/* Fixed-size pass-by-reference types */
typedef struct GeoPoint
{
	float8		x;
	float8		y;
} GeoPoint;

typedef struct GeoBBox
{
	float8		xmin;
	float8		ymin;
	float8		xmax;
	float8		ymax;
} GeoBBox;

typedef struct GeoCircle
{
	float8		cx;
	float8		cy;
	float8		radius;
} GeoCircle;

/* Variable-length polygon */
typedef struct GeoPolygon
{
	int32		vl_len_;
	int32		npts;
	GeoPoint	pts[FLEXIBLE_ARRAY_MEMBER];
} GeoPolygon;

#define GEO_POLYGON_SIZE(n)    (offsetof(GeoPolygon, pts) + (n) * sizeof(GeoPoint))
#define DatumGetGeoPolygon(d)  ((GeoPolygon *) PG_DETOAST_DATUM(d))

/* Math helpers (geo_ops.c) */
extern bool  geo_point_in_polygon(GeoPoint *pt, GeoPolygon *poly);
extern bool  geo_bbox_intersects(GeoBBox *a, GeoBBox *b);
extern float8 geo_point_distance(GeoPoint *a, GeoPoint *b);
extern float8 geo_polygon_area(GeoPolygon *poly);
extern GeoBBox geo_polygon_bbox(GeoPolygon *poly);

/* SQL-callable functions */
extern Datum geo_point_in(PG_FUNCTION_ARGS);
extern Datum geo_point_out(PG_FUNCTION_ARGS);
extern Datum geo_bbox_in(PG_FUNCTION_ARGS);
extern Datum geo_bbox_out(PG_FUNCTION_ARGS);
extern Datum geo_polygon_in(PG_FUNCTION_ARGS);
extern Datum geo_polygon_out(PG_FUNCTION_ARGS);
extern Datum geo_circle_in(PG_FUNCTION_ARGS);
extern Datum geo_circle_out(PG_FUNCTION_ARGS);
extern Datum geo_make_point(PG_FUNCTION_ARGS);
extern Datum geo_make_bbox(PG_FUNCTION_ARGS);
extern Datum geo_st_distance(PG_FUNCTION_ARGS);
extern Datum geo_st_contains(PG_FUNCTION_ARGS);
extern Datum geo_st_within(PG_FUNCTION_ARGS);
extern Datum geo_st_intersects_bbox(PG_FUNCTION_ARGS);
extern Datum geo_st_area(PG_FUNCTION_ARGS);
extern Datum geo_st_bbox(PG_FUNCTION_ARGS);
extern Datum geo_st_buffer(PG_FUNCTION_ARGS);
extern Datum geo_bbox_overlap(PG_FUNCTION_ARGS);
extern Datum geo_nearby(PG_FUNCTION_ARGS);

/* Parquet reader (parquet_reader.c) */
extern Datum read_parquet_full(PG_FUNCTION_ARGS);

#endif							/* ALOHADB_GEO_H */
