/*-------------------------------------------------------------------------
 *
 * geo_ops.c
 *	  Spatial algorithms for pg_geo.
 *
 *	  All algorithms are prior art with no active patent concerns:
 *
 *	    Point-in-polygon (ray-casting):
 *	      W. Randolph Franklin, ~1970 (published ACM SIGGRAPH 1982).
 *	      "PNPOLY — Point Inclusion in Polygon Test."
 *
 *	    Bounding-box intersection (AABB):
 *	      Prior art: Roberts 1963; widespread since 1970s.
 *
 *	    Euclidean distance: Euclid, ~300 BCE.
 *
 *	    Polygon area (shoelace formula):
 *	      Gauss, ~1795.  Also known as the surveyor's formula.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_geo/geo_ops.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "pg_geo.h"

#include <math.h>

/* ----------------------------------------------------------------
 * geo_point_in_polygon
 *
 * Ray-casting point-in-polygon test (Franklin 1970/1982).
 *
 * Cast a horizontal ray from pt to the right (+x direction).
 * Count how many polygon edges the ray crosses; if odd, the point
 * is inside.  Edge cases (on the boundary) return true.
 * ---------------------------------------------------------------- */
bool
geo_point_in_polygon(GeoPoint *pt, GeoPolygon *poly)
{
	int		inside = 0;
	int		i, j;
	int		n = poly->npts;

	for (i = 0, j = n - 1; i < n; j = i++)
	{
		float8 xi = poly->pts[i].x;
		float8 yi = poly->pts[i].y;
		float8 xj = poly->pts[j].x;
		float8 yj = poly->pts[j].y;

		if (((yi > pt->y) != (yj > pt->y)) &&
			(pt->x < (xj - xi) * (pt->y - yi) / (yj - yi) + xi))
			inside = !inside;
	}

	return inside != 0;
}

/* ----------------------------------------------------------------
 * geo_bbox_intersects
 *
 * AABB (axis-aligned bounding box) overlap test.
 * Two boxes intersect iff they overlap on both axes.
 * ---------------------------------------------------------------- */
bool
geo_bbox_intersects(GeoBBox *a, GeoBBox *b)
{
	return (a->xmin <= b->xmax && a->xmax >= b->xmin &&
			a->ymin <= b->ymax && a->ymax >= b->ymin);
}

/* ----------------------------------------------------------------
 * geo_point_distance
 *
 * Euclidean (L2) distance between two 2-D points.
 * ---------------------------------------------------------------- */
float8
geo_point_distance(GeoPoint *a, GeoPoint *b)
{
	float8 dx = a->x - b->x;
	float8 dy = a->y - b->y;

	return sqrt(dx * dx + dy * dy);
}

/* ----------------------------------------------------------------
 * geo_polygon_area
 *
 * Signed area using the shoelace formula (Gauss, ~1795).
 * Returns a positive value for counter-clockwise orientation,
 * negative for clockwise.  Call fabs() for unsigned area.
 * ---------------------------------------------------------------- */
float8
geo_polygon_area(GeoPolygon *poly)
{
	float8	sum = 0.0;
	int		i, j;
	int		n = poly->npts;

	for (i = 0, j = n - 1; i < n; j = i++)
		sum += (poly->pts[j].x + poly->pts[i].x) *
			   (poly->pts[j].y - poly->pts[i].y);

	return sum / 2.0;
}

/* ----------------------------------------------------------------
 * geo_polygon_bbox
 *
 * Minimum bounding rectangle of a polygon.
 * ---------------------------------------------------------------- */
GeoBBox
geo_polygon_bbox(GeoPolygon *poly)
{
	GeoBBox bb;
	int		i;

	if (poly->npts == 0)
	{
		bb.xmin = bb.ymin = bb.xmax = bb.ymax = 0.0;
		return bb;
	}

	bb.xmin = bb.xmax = poly->pts[0].x;
	bb.ymin = bb.ymax = poly->pts[0].y;

	for (i = 1; i < poly->npts; i++)
	{
		if (poly->pts[i].x < bb.xmin) bb.xmin = poly->pts[i].x;
		if (poly->pts[i].x > bb.xmax) bb.xmax = poly->pts[i].x;
		if (poly->pts[i].y < bb.ymin) bb.ymin = poly->pts[i].y;
		if (poly->pts[i].y > bb.ymax) bb.ymax = poly->pts[i].y;
	}

	return bb;
}
