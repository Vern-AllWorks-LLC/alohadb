/*-------------------------------------------------------------------------
 *
 * search_geo.c
 *	  Geographic search functions: Haversine distance and nearby search.
 *
 *	  search_haversine: Computes the great-circle distance in meters
 *	  between two points on Earth using the Haversine formula.
 *
 *	  search_nearby: Uses SPI to find rows within a given radius,
 *	  computing Haversine distance via SQL, ordered by distance.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_search/search_geo.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <math.h>
#include <string.h>

#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/snapmgr.h"

#include "alohadb_search.h"

PG_FUNCTION_INFO_V1(search_haversine);
PG_FUNCTION_INFO_V1(search_nearby);

/* ----------------------------------------------------------------
 * search_haversine(lat1 float8, lon1 float8, lat2 float8, lon2 float8)
 * RETURNS float8
 *
 * Haversine formula:
 *   a = sin^2((lat2-lat1)/2) + cos(lat1) * cos(lat2) * sin^2((lon2-lon1)/2)
 *   d = 2 * R * asin(sqrt(a))
 *
 * Input is in degrees; internally converted to radians.
 * Returns distance in meters.
 * ---------------------------------------------------------------- */
Datum
search_haversine(PG_FUNCTION_ARGS)
{
	float8		lat1_deg = PG_GETARG_FLOAT8(0);
	float8		lon1_deg = PG_GETARG_FLOAT8(1);
	float8		lat2_deg = PG_GETARG_FLOAT8(2);
	float8		lon2_deg = PG_GETARG_FLOAT8(3);

	float8		lat1 = lat1_deg * M_PI / 180.0;
	float8		lon1 = lon1_deg * M_PI / 180.0;
	float8		lat2 = lat2_deg * M_PI / 180.0;
	float8		lon2 = lon2_deg * M_PI / 180.0;

	float8		dlat = lat2 - lat1;
	float8		dlon = lon2 - lon1;

	float8		a;
	float8		c;
	float8		distance;

	a = sin(dlat / 2.0) * sin(dlat / 2.0) +
		cos(lat1) * cos(lat2) *
		sin(dlon / 2.0) * sin(dlon / 2.0);

	c = 2.0 * asin(sqrt(a));

	distance = EARTH_RADIUS_METERS * c;

	PG_RETURN_FLOAT8(distance);
}

/* ----------------------------------------------------------------
 * search_nearby(lat float8, lon float8, radius_meters float8,
 *               source_table text, lat_column text, lon_column text,
 *               lim int DEFAULT 100)
 * RETURNS TABLE(id text, distance float8)
 *
 * Finds rows in source_table within radius_meters of (lat, lon),
 * computing Haversine distance via SQL.  Assumes source_table has
 * an 'id' column that can be cast to text.
 * ---------------------------------------------------------------- */
Datum
search_nearby(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	float8		lat = PG_GETARG_FLOAT8(0);
	float8		lon = PG_GETARG_FLOAT8(1);
	float8		radius_meters = PG_GETARG_FLOAT8(2);
	text	   *table_text = PG_GETARG_TEXT_PP(3);
	text	   *lat_col_text = PG_GETARG_TEXT_PP(4);
	text	   *lon_col_text = PG_GETARG_TEXT_PP(5);
	int			lim = PG_GETARG_INT32(6);

	char	   *source_table = text_to_cstring(table_text);
	char	   *lat_column = text_to_cstring(lat_col_text);
	char	   *lon_column = text_to_cstring(lon_col_text);
	const char *lat_col_qi = quote_identifier(lat_column);
	const char *lon_col_qi = quote_identifier(lon_column);

	StringInfoData query;
	int			ret;
	uint64		proc;
	uint64		i;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	Datum	   *id_values = NULL;
	Datum	   *dist_values = NULL;
	bool	   *id_nulls = NULL;
	bool	   *dist_nulls = NULL;
	MemoryContext per_query_ctx;
	MemoryContext oldcxt;

	InitMaterializedSRF(fcinfo, 0);

	rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	tupdesc = rsinfo->setDesc;
	tupstore = rsinfo->setResult;

	/*
	 * Build a query that computes Haversine distance inline.
	 * We embed the user's lat/lon as literals and reference the table's
	 * lat/lon columns.  The Haversine formula uses radians.
	 */
	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT id::text, "
					 "(2.0 * %.1f * asin(sqrt("
					 "  sin((radians(%s) - radians(%f)) / 2.0) * "
					 "  sin((radians(%s) - radians(%f)) / 2.0) + "
					 "  cos(radians(%f)) * cos(radians(%s)) * "
					 "  sin((radians(%s) - radians(%f)) / 2.0) * "
					 "  sin((radians(%s) - radians(%f)) / 2.0)"
					 ")))::float8 AS distance "
					 "FROM %s "
					 "WHERE (2.0 * %.1f * asin(sqrt("
					 "  sin((radians(%s) - radians(%f)) / 2.0) * "
					 "  sin((radians(%s) - radians(%f)) / 2.0) + "
					 "  cos(radians(%f)) * cos(radians(%s)) * "
					 "  sin((radians(%s) - radians(%f)) / 2.0) * "
					 "  sin((radians(%s) - radians(%f)) / 2.0)"
					 "))) <= %f "
					 "ORDER BY distance LIMIT %d",
					 /* SELECT distance expression */
					 EARTH_RADIUS_METERS,
					 lat_col_qi, lat,
					 lat_col_qi, lat,
					 lat, lat_col_qi,
					 lon_col_qi, lon,
					 lon_col_qi, lon,
					 /* FROM */
					 source_table,
					 /* WHERE distance expression */
					 EARTH_RADIUS_METERS,
					 lat_col_qi, lat,
					 lat_col_qi, lat,
					 lat, lat_col_qi,
					 lon_col_qi, lon,
					 lon_col_qi, lon,
					 /* radius filter */
					 radius_meters,
					 lim);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(query.data, true, 0);

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "search_nearby: SPI_execute failed: error code %d", ret);

	proc = SPI_processed;

	/*
	 * Copy results out of SPI context.
	 */
	if (proc > 0)
	{
		per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
		oldcxt = MemoryContextSwitchTo(per_query_ctx);

		id_values = palloc(sizeof(Datum) * proc);
		dist_values = palloc(sizeof(Datum) * proc);
		id_nulls = palloc(sizeof(bool) * proc);
		dist_nulls = palloc(sizeof(bool) * proc);

		for (i = 0; i < proc; i++)
		{
			bool	isnull;

			/* id (column 1, text) */
			id_values[i] = SPI_getbinval(SPI_tuptable->vals[i],
										 SPI_tuptable->tupdesc,
										 1, &isnull);
			id_nulls[i] = isnull;
			if (!isnull)
				id_values[i] = datumCopy(id_values[i], false, -1);

			/* distance (column 2, float8) */
			dist_values[i] = SPI_getbinval(SPI_tuptable->vals[i],
										   SPI_tuptable->tupdesc,
										   2, &isnull);
			dist_nulls[i] = isnull;
			if (!isnull)
				dist_values[i] = datumCopy(dist_values[i], true, sizeof(float8));
		}

		MemoryContextSwitchTo(oldcxt);
	}

	PopActiveSnapshot();
	SPI_finish();

	/* Populate tuplestore from copied data */
	for (i = 0; i < proc; i++)
	{
		Datum		vals[2];
		bool		nuls[2];

		vals[0] = id_values[i];
		nuls[0] = id_nulls[i];
		vals[1] = dist_values[i];
		nuls[1] = dist_nulls[i];

		tuplestore_putvalues(tupstore, tupdesc, vals, nuls);
	}

	pfree(query.data);
	pfree(source_table);
	pfree(lat_column);
	pfree(lon_column);

	return (Datum) 0;
}
