#include "postgres.h"
#include "fmgr.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "funcapi.h"
#include "alohadb_analytics.h"

PG_MODULE_MAGIC_EXT(.name = "alohadb_analytics", .version = "1.0");

PG_FUNCTION_INFO_V1(analytics_create_cont_agg);
PG_FUNCTION_INFO_V1(analytics_drop_cont_agg);
PG_FUNCTION_INFO_V1(analytics_refresh_cont_agg);
PG_FUNCTION_INFO_V1(analytics_cont_agg_status);
PG_FUNCTION_INFO_V1(analytics_create_projection);
PG_FUNCTION_INFO_V1(analytics_drop_projection);

/*
 * analytics_create_cont_agg(name text, source_table text, agg_query text,
 *                           watermark_col text, refresh_interval interval)
 *
 * Creates a materialized view and registers it as a continuous aggregate.
 * The watermark column tracks the high-water mark for incremental refresh.
 */
Datum
analytics_create_cont_agg(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *source = text_to_cstring(PG_GETARG_TEXT_PP(1));
	char	   *query = text_to_cstring(PG_GETARG_TEXT_PP(2));
	char	   *watermark_col = text_to_cstring(PG_GETARG_TEXT_PP(3));
	char	   *refresh_interval = PG_ARGISNULL(4) ? "1 hour" :
		text_to_cstring(PG_GETARG_TEXT_PP(4));
	StringInfoData sql;
	int			ret;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* Create the materialized view */
	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "CREATE MATERIALIZED VIEW %s AS %s WITH NO DATA",
					 quote_identifier(name), query);
	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_UTILITY)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to create materialized view \"%s\"", name)));

	/* Register in metadata */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
					 "INSERT INTO %s (name, source_table, agg_query, watermark_col, "
					 "refresh_interval, last_watermark) VALUES (%s, %s, %s, %s, %s::interval, NULL)",
					 ANALYTICS_CONT_AGGS_TABLE,
					 quote_literal_cstr(name),
					 quote_literal_cstr(source),
					 quote_literal_cstr(query),
					 quote_literal_cstr(watermark_col),
					 quote_literal_cstr(refresh_interval));
	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_INSERT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to register continuous aggregate \"%s\"", name)));

	/* Initial refresh */
	resetStringInfo(&sql);
	appendStringInfo(&sql, "REFRESH MATERIALIZED VIEW %s", quote_identifier(name));
	SPI_execute(sql.data, false, 0);

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_VOID();
}

Datum
analytics_drop_cont_agg(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	StringInfoData sql;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	initStringInfo(&sql);
	appendStringInfo(&sql, "DROP MATERIALIZED VIEW IF EXISTS %s", quote_identifier(name));
	SPI_execute(sql.data, false, 0);

	resetStringInfo(&sql);
	appendStringInfo(&sql, "DELETE FROM %s WHERE name = %s",
					 ANALYTICS_CONT_AGGS_TABLE, quote_literal_cstr(name));
	SPI_execute(sql.data, false, 0);

	PopActiveSnapshot();
	SPI_finish();
	PG_RETURN_VOID();
}

Datum
analytics_refresh_cont_agg(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	StringInfoData sql;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* For incremental refresh: get watermark info, refresh with CONCURRENTLY if possible */
	initStringInfo(&sql);
	appendStringInfo(&sql, "REFRESH MATERIALIZED VIEW %s", quote_identifier(name));
	SPI_execute(sql.data, false, 0);

	/* Update last_refresh */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
					 "UPDATE %s SET last_refresh = now() WHERE name = %s",
					 ANALYTICS_CONT_AGGS_TABLE, quote_literal_cstr(name));
	SPI_execute(sql.data, false, 0);

	PopActiveSnapshot();
	SPI_finish();
	PG_RETURN_VOID();
}

/*
 * analytics_cont_agg_status() -> TABLE
 */
Datum
analytics_cont_agg_status(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	StringInfoData sql;
	int			ret;
	uint64		nrows,
				i;

	InitMaterializedSRF(fcinfo, 0);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT name, source_table, watermark_col, "
					 "refresh_interval::text, last_refresh::text, last_watermark::text "
					 "FROM %s ORDER BY name", ANALYTICS_CONT_AGGS_TABLE);

	ret = SPI_execute(sql.data, true, 0);
	nrows = SPI_processed;

	if (ret == SPI_OK_SELECT && nrows > 0)
	{
		TupleDesc	spi_tupdesc = SPI_tuptable->tupdesc;

		for (i = 0; i < nrows; i++)
		{
			Datum		values[6];
			bool		nulls[6];
			int			j;
			HeapTuple	spi_tuple = SPI_tuptable->vals[i];

			for (j = 0; j < 6; j++)
			{
				char	   *val = SPI_getvalue(spi_tuple, spi_tupdesc, j + 1);

				if (val == NULL)
				{
					nulls[j] = true;
					values[j] = (Datum) 0;
				}
				else
				{
					nulls[j] = false;
					values[j] = CStringGetTextDatum(val);
				}
			}
			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		}
	}

	PopActiveSnapshot();
	SPI_finish();
	PG_RETURN_NULL();
}

/*
 * analytics_create_projection(name, source_table, query)
 * Creates a trigger-based materialized view that auto-refreshes on INSERT.
 */
Datum
analytics_create_projection(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *source = text_to_cstring(PG_GETARG_TEXT_PP(1));
	char	   *query = text_to_cstring(PG_GETARG_TEXT_PP(2));
	StringInfoData sql;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* Create the materialized view */
	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "CREATE MATERIALIZED VIEW %s AS %s",
					 quote_identifier(name), query);
	SPI_execute(sql.data, false, 0);

	/* Create a trigger function to refresh on insert */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
					 "CREATE OR REPLACE FUNCTION %s_refresh_fn() RETURNS trigger "
					 "LANGUAGE plpgsql AS $$ BEGIN "
					 "REFRESH MATERIALIZED VIEW %s; RETURN NULL; END; $$",
					 name, quote_identifier(name));
	SPI_execute(sql.data, false, 0);

	/* Create the trigger */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
					 "CREATE TRIGGER %s_refresh_trig AFTER INSERT ON %s "
					 "FOR EACH STATEMENT EXECUTE FUNCTION %s_refresh_fn()",
					 name, quote_identifier(source), name);
	SPI_execute(sql.data, false, 0);

	PopActiveSnapshot();
	SPI_finish();
	PG_RETURN_VOID();
}

Datum
analytics_drop_projection(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	StringInfoData sql;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	initStringInfo(&sql);

	/* Drop trigger (need source table - look it up from pg_trigger) */
	appendStringInfo(&sql,
					 "DO $$ DECLARE r RECORD; BEGIN "
					 "FOR r IN SELECT tgrelid::regclass::text AS tbl FROM pg_trigger "
					 "WHERE tgname = '%s_refresh_trig' LOOP "
					 "EXECUTE format('DROP TRIGGER IF EXISTS %s_refresh_trig ON %%I', r.tbl); "
					 "END LOOP; END; $$",
					 name, name);
	SPI_execute(sql.data, false, 0);

	/* Drop function and MV */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
					 "DROP FUNCTION IF EXISTS %s_refresh_fn() CASCADE; "
					 "DROP MATERIALIZED VIEW IF EXISTS %s CASCADE",
					 name, quote_identifier(name));
	SPI_execute(sql.data, false, 0);

	PopActiveSnapshot();
	SPI_finish();
	PG_RETURN_VOID();
}
