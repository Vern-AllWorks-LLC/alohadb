/*-------------------------------------------------------------------------
 *
 * pg_geo.c
 *	  Type I/O, constructors, and module registration for pg_geo.
 *
 *	  Implements four fixed/variable PostgreSQL types:
 *	    geo_point   — (x, y), 16 bytes
 *	    geo_bbox    — (xmin, ymin, xmax, ymax), 32 bytes
 *	    geo_circle  — (cx, cy, radius), 24 bytes
 *	    geo_polygon — variable-length array of GeoPoint
 *
 *	  Spatial algorithms are in geo_ops.c.  See pg_geo.h for
 *	  algorithm lineage and patent-mitigation notes.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_geo/pg_geo.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "varatt.h"

#include "pg_geo.h"

#include <math.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

PG_MODULE_MAGIC_EXT(
					.name = "pg_geo",
					.version = "1.0"
);

PG_FUNCTION_INFO_V1(geo_point_in);
PG_FUNCTION_INFO_V1(geo_point_out);
PG_FUNCTION_INFO_V1(geo_bbox_in);
PG_FUNCTION_INFO_V1(geo_bbox_out);
PG_FUNCTION_INFO_V1(geo_polygon_in);
PG_FUNCTION_INFO_V1(geo_polygon_out);
PG_FUNCTION_INFO_V1(geo_circle_in);
PG_FUNCTION_INFO_V1(geo_circle_out);
PG_FUNCTION_INFO_V1(geo_make_point);
PG_FUNCTION_INFO_V1(geo_make_bbox);
PG_FUNCTION_INFO_V1(geo_st_distance);
PG_FUNCTION_INFO_V1(geo_st_distance_sphere);
PG_FUNCTION_INFO_V1(geo_st_contains);
PG_FUNCTION_INFO_V1(geo_st_within);
PG_FUNCTION_INFO_V1(geo_st_intersects_bbox);
PG_FUNCTION_INFO_V1(geo_st_area);
PG_FUNCTION_INFO_V1(geo_st_bbox);
PG_FUNCTION_INFO_V1(geo_st_buffer);
PG_FUNCTION_INFO_V1(geo_bbox_overlap);

/* ----------------------------------------------------------------
 * Parsing helpers
 * ---------------------------------------------------------------- */

static const char *
skip_whitespace(const char *p)
{
	while (isspace((unsigned char) *p)) p++;
	return p;
}

static const char *
parse_double(const char *p, double *out)
{
	char   *end;
	p = skip_whitespace(p);
	*out = strtod(p, &end);
	if (end == p)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("expected a floating-point number near \"%s\"", p)));
	return end;
}

static const char *
expect_char(const char *p, char c)
{
	p = skip_whitespace(p);
	if (*p != c)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("expected '%c' but got '%c'", c, *p ? *p : '?')));
	return p + 1;
}

/* ----------------------------------------------------------------
 * geo_point I/O
 *
 * Text format: "x,y"  or  "POINT(x y)"
 * ---------------------------------------------------------------- */
Datum
geo_point_in(PG_FUNCTION_ARGS)
{
	const char *str = PG_GETARG_CSTRING(0);
	const char *p   = str;
	GeoPoint   *pt;
	double		x, y;

	p = skip_whitespace(p);

	if (strncasecmp(p, "POINT", 5) == 0)
	{
		p += 5;
		p = expect_char(p, '(');
		p = parse_double(p, &x);
		p = parse_double(p, &y);
		p = expect_char(p, ')');
	}
	else
	{
		p = parse_double(str, &x);
		p = expect_char(p, ',');
		p = parse_double(p, &y);
	}

	pt = (GeoPoint *) palloc(sizeof(GeoPoint));
	pt->x = x;
	pt->y = y;
	PG_RETURN_POINTER(pt);
}

Datum
geo_point_out(PG_FUNCTION_ARGS)
{
	GeoPoint *pt = (GeoPoint *) PG_GETARG_POINTER(0);
	char	  buf[64];

	snprintf(buf, sizeof(buf), "%g,%g", pt->x, pt->y);
	PG_RETURN_CSTRING(pstrdup(buf));
}

/* ----------------------------------------------------------------
 * geo_bbox I/O
 *
 * Text format: "xmin,ymin,xmax,ymax"  or  "BOX(xmin ymin,xmax ymax)"
 * ---------------------------------------------------------------- */
Datum
geo_bbox_in(PG_FUNCTION_ARGS)
{
	const char *str = PG_GETARG_CSTRING(0);
	const char *p   = str;
	GeoBBox	   *bb;
	double		xmin, ymin, xmax, ymax;

	p = skip_whitespace(p);

	if (strncasecmp(p, "BOX", 3) == 0)
	{
		p += 3;
		p = expect_char(p, '(');
		p = parse_double(p, &xmin);
		p = parse_double(p, &ymin);
		p = expect_char(p, ',');
		p = parse_double(p, &xmax);
		p = parse_double(p, &ymax);
		p = expect_char(p, ')');
	}
	else
	{
		p = parse_double(str, &xmin);
		p = expect_char(p, ',');
		p = parse_double(p, &ymin);
		p = expect_char(p, ',');
		p = parse_double(p, &xmax);
		p = expect_char(p, ',');
		p = parse_double(p, &ymax);
	}

	bb = (GeoBBox *) palloc(sizeof(GeoBBox));
	bb->xmin = xmin;
	bb->ymin = ymin;
	bb->xmax = xmax;
	bb->ymax = ymax;
	PG_RETURN_POINTER(bb);
}

Datum
geo_bbox_out(PG_FUNCTION_ARGS)
{
	GeoBBox *bb = (GeoBBox *) PG_GETARG_POINTER(0);
	char	 buf[128];

	snprintf(buf, sizeof(buf), "%g,%g,%g,%g",
			 bb->xmin, bb->ymin, bb->xmax, bb->ymax);
	PG_RETURN_CSTRING(pstrdup(buf));
}

/* ----------------------------------------------------------------
 * geo_polygon I/O
 *
 * Text format (WKT): "POLYGON((x1 y1,x2 y2,...,x1 y1))"
 *
 * We allocate GeoPolygon as a varlena with npts GeoPoint elements.
 * ---------------------------------------------------------------- */
Datum
geo_polygon_in(PG_FUNCTION_ARGS)
{
	const char *str = PG_GETARG_CSTRING(0);
	const char *p   = str;
	GeoPoint	pts[65536];
	int			npts = 0;
	GeoPolygon *poly;

	p = skip_whitespace(p);
	if (strncasecmp(p, "POLYGON", 7) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("polygon must start with POLYGON")));
	p += 7;
	p = skip_whitespace(p);
	p = expect_char(p, '(');
	p = skip_whitespace(p);
	p = expect_char(p, '(');

	while (*p && *p != ')')
	{
		double x, y;

		p = skip_whitespace(p);
		if (*p == ')') break;
		if (*p == ',') { p++; continue; }

		p = parse_double(p, &x);
		p = parse_double(p, &y);

		if (npts >= 65536)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("polygon exceeds 65536 vertices")));

		pts[npts].x = x;
		pts[npts].y = y;
		npts++;

		p = skip_whitespace(p);
		if (*p == ',') p++;
	}

	if (npts < 3)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("polygon must have at least 3 vertices")));

	poly = (GeoPolygon *) palloc(GEO_POLYGON_SIZE(npts));
	SET_VARSIZE(poly, GEO_POLYGON_SIZE(npts));
	poly->npts = npts;
	memcpy(poly->pts, pts, npts * sizeof(GeoPoint));

	PG_RETURN_POINTER(poly);
}

Datum
geo_polygon_out(PG_FUNCTION_ARGS)
{
	GeoPolygon *poly = DatumGetGeoPolygon(PG_GETARG_DATUM(0));
	StringInfoData buf;
	int			i;

	initStringInfo(&buf);
	appendStringInfoString(&buf, "POLYGON((");

	for (i = 0; i < poly->npts; i++)
	{
		if (i > 0) appendStringInfoChar(&buf, ',');
		appendStringInfo(&buf, "%g %g",
						 poly->pts[i].x, poly->pts[i].y);
	}
	appendStringInfoString(&buf, "))");
	PG_RETURN_CSTRING(buf.data);
}

/* ----------------------------------------------------------------
 * geo_circle I/O
 *
 * Text format: "cx,cy,radius"  or  "CIRCLE(cx cy radius)"
 * ---------------------------------------------------------------- */
Datum
geo_circle_in(PG_FUNCTION_ARGS)
{
	const char *str = PG_GETARG_CSTRING(0);
	const char *p   = str;
	GeoCircle  *c;
	double		cx, cy, r;

	p = skip_whitespace(p);
	if (strncasecmp(p, "CIRCLE", 6) == 0)
	{
		p += 6;
		p = expect_char(p, '(');
		p = parse_double(p, &cx);
		p = parse_double(p, &cy);
		p = parse_double(p, &r);
		p = expect_char(p, ')');
	}
	else
	{
		p = parse_double(str, &cx);
		p = expect_char(p, ',');
		p = parse_double(p, &cy);
		p = expect_char(p, ',');
		p = parse_double(p, &r);
	}

	c = (GeoCircle *) palloc(sizeof(GeoCircle));
	c->cx     = cx;
	c->cy     = cy;
	c->radius = r;
	PG_RETURN_POINTER(c);
}

Datum
geo_circle_out(PG_FUNCTION_ARGS)
{
	GeoCircle *c = (GeoCircle *) PG_GETARG_POINTER(0);
	char	   buf[96];

	snprintf(buf, sizeof(buf), "%g,%g,%g", c->cx, c->cy, c->radius);
	PG_RETURN_CSTRING(pstrdup(buf));
}

/* ----------------------------------------------------------------
 * Constructor functions
 * ---------------------------------------------------------------- */

Datum
geo_make_point(PG_FUNCTION_ARGS)
{
	float8		x = PG_GETARG_FLOAT8(0);
	float8		y = PG_GETARG_FLOAT8(1);
	GeoPoint   *pt = (GeoPoint *) palloc(sizeof(GeoPoint));

	pt->x = x;
	pt->y = y;
	PG_RETURN_POINTER(pt);
}

Datum
geo_make_bbox(PG_FUNCTION_ARGS)
{
	GeoBBox *bb = (GeoBBox *) palloc(sizeof(GeoBBox));

	bb->xmin = PG_GETARG_FLOAT8(0);
	bb->ymin = PG_GETARG_FLOAT8(1);
	bb->xmax = PG_GETARG_FLOAT8(2);
	bb->ymax = PG_GETARG_FLOAT8(3);
	PG_RETURN_POINTER(bb);
}

/* ----------------------------------------------------------------
 * Wrapper SQL functions that delegate to geo_ops.c
 * ---------------------------------------------------------------- */

Datum
geo_st_distance(PG_FUNCTION_ARGS)
{
	GeoPoint *a = (GeoPoint *) PG_GETARG_POINTER(0);
	GeoPoint *b = (GeoPoint *) PG_GETARG_POINTER(1);
	PG_RETURN_FLOAT8(geo_point_distance(a, b));
}

/* Haversine distance in metres (WGS-84, great circle) */
Datum
geo_st_distance_sphere(PG_FUNCTION_ARGS)
{
	GeoPoint *a = (GeoPoint *) PG_GETARG_POINTER(0);
	GeoPoint *b = (GeoPoint *) PG_GETARG_POINTER(1);
	/* Haversine formula.  Inputs: a->x = lon1, a->y = lat1 (degrees). */
	double	  lat1 = a->y * M_PI / 180.0;
	double	  lat2 = b->y * M_PI / 180.0;
	double	  dlat = (b->y - a->y) * M_PI / 180.0;
	double	  dlon = (b->x - a->x) * M_PI / 180.0;
	double	  sinDlat = sin(dlat / 2.0);
	double	  sinDlon = sin(dlon / 2.0);
	double	  ha = sinDlat * sinDlat +
				   cos(lat1) * cos(lat2) * sinDlon * sinDlon;
	double	  c  = 2.0 * atan2(sqrt(ha), sqrt(1.0 - ha));

	(void) lat1; (void) lat2;

	PG_RETURN_FLOAT8(6371008.8 * c);	/* Earth mean radius in metres */
}

Datum
geo_st_contains(PG_FUNCTION_ARGS)
{
	GeoPolygon *poly = DatumGetGeoPolygon(PG_GETARG_DATUM(0));
	GeoPoint   *pt   = (GeoPoint *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(geo_point_in_polygon(pt, poly));
}

Datum
geo_st_within(PG_FUNCTION_ARGS)
{
	GeoPoint   *pt   = (GeoPoint *) PG_GETARG_POINTER(0);
	GeoPolygon *poly = DatumGetGeoPolygon(PG_GETARG_DATUM(1));

	PG_RETURN_BOOL(geo_point_in_polygon(pt, poly));
}

Datum
geo_st_intersects_bbox(PG_FUNCTION_ARGS)
{
	GeoBBox *a = (GeoBBox *) PG_GETARG_POINTER(0);
	GeoBBox *b = (GeoBBox *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(geo_bbox_intersects(a, b));
}

Datum
geo_st_area(PG_FUNCTION_ARGS)
{
	GeoPolygon *poly = DatumGetGeoPolygon(PG_GETARG_DATUM(0));

	PG_RETURN_FLOAT8(geo_polygon_area(poly));
}

Datum
geo_st_bbox(PG_FUNCTION_ARGS)
{
	GeoPolygon *poly = DatumGetGeoPolygon(PG_GETARG_DATUM(0));
	GeoBBox		bb   = geo_polygon_bbox(poly);
	GeoBBox	   *result = (GeoBBox *) palloc(sizeof(GeoBBox));

	*result = bb;
	PG_RETURN_POINTER(result);
}

Datum
geo_st_buffer(PG_FUNCTION_ARGS)
{
	GeoPoint  *pt     = (GeoPoint *) PG_GETARG_POINTER(0);
	float8     radius = PG_GETARG_FLOAT8(1);
	GeoCircle *c      = (GeoCircle *) palloc(sizeof(GeoCircle));

	c->cx     = pt->x;
	c->cy     = pt->y;
	c->radius = radius;
	PG_RETURN_POINTER(c);
}

Datum
geo_bbox_overlap(PG_FUNCTION_ARGS)
{
	GeoBBox *a = (GeoBBox *) PG_GETARG_POINTER(0);
	GeoBBox *b = (GeoBBox *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(geo_bbox_intersects(a, b));
}
