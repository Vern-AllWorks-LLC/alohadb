/*-------------------------------------------------------------------------
 *
 * qs_index_advisor.c
 *		AlohaDB Query Store - Index advisor and autovacuum suggestions.
 *
 * Uses SPI to query system catalogs and provide recommendations.
 *
 * Copyright (c) 2025, OpenCAN / AlohaDB
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "alohadb_query_store.h"

#include "executor/spi.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/tuplestore.h"

/* PG_FUNCTION_INFO_V1 - MUST be in same file as the function implementation */
PG_FUNCTION_INFO_V1(index_advisor_recommend);
PG_FUNCTION_INFO_V1(index_advisor_unused_indexes);
PG_FUNCTION_INFO_V1(autovacuum_suggestions);

/*
 * index_advisor_recommend()
 *
 * Analyzes pg_stat_user_tables for tables with high sequential scan counts
 * relative to index scans, and pg_stats for columns with high n_distinct
 * and good correlation (suggesting a btree index would be effective).
 *
 * Returns TABLE(table_name text, column_name text, index_type text,
 *               reason text, create_statement text)
 */
Datum
index_advisor_recommend(PG_FUNCTION_ARGS)
{
#define IDX_RECOMMEND_COLS 5
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int				ret;
	uint64			proc;
	uint64			i;

	static const char *recommend_sql =
		"SELECT "
		"  s.schemaname || '.' || s.relname AS table_name, "
		"  ps.attname AS column_name, "
		"  'btree' AS index_type, "
		"  'seq_scan=' || s.seq_scan || ' idx_scan=' || COALESCE(s.idx_scan, 0) "
		"    || ' n_distinct=' || ROUND(ps.n_distinct::numeric, 2) "
		"    || ' correlation=' || ROUND(ABS(ps.correlation)::numeric, 2) AS reason, "
		"  'CREATE INDEX ON ' || s.schemaname || '.' || s.relname "
		"    || ' USING btree (' || ps.attname || ')' AS create_statement "
		"FROM pg_stat_user_tables s "
		"JOIN pg_stats ps ON ps.schemaname = s.schemaname "
		"  AND ps.tablename = s.relname "
		"WHERE s.seq_scan > 100 "
		"  AND (s.idx_scan IS NULL OR s.seq_scan > s.idx_scan * 10) "
		"  AND s.n_live_tup > 1000 "
		"  AND ps.n_distinct > 10 "
		"  AND ABS(ps.correlation) > 0.3 "
		"  AND NOT EXISTS ( "
		"    SELECT 1 FROM pg_index pi "
		"    JOIN pg_attribute pa ON pa.attrelid = pi.indrelid "
		"      AND pa.attnum = ANY(pi.indkey) "
		"    WHERE pi.indrelid = (s.schemaname || '.' || s.relname)::regclass "
		"      AND pa.attname = ps.attname "
		"  ) "
		"ORDER BY s.seq_scan DESC "
		"LIMIT 50";

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	SPI_connect();

	ret = SPI_execute(recommend_sql, true, 0);
	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("index_advisor_recommend: SPI_execute failed: %s",
						SPI_result_code_string(ret))));
	}

	proc = SPI_processed;

	for (i = 0; i < proc; i++)
	{
		Datum		values[IDX_RECOMMEND_COLS];
		bool		nulls[IDX_RECOMMEND_COLS];
		HeapTuple	spi_tuple;
		char	   *val;

		memset(nulls, 0, sizeof(nulls));

		spi_tuple = SPI_tuptable->vals[i];

		val = SPI_getvalue(spi_tuple, SPI_tuptable->tupdesc, 1);
		values[0] = val ? CStringGetTextDatum(val) : (Datum) 0;
		nulls[0] = (val == NULL);

		val = SPI_getvalue(spi_tuple, SPI_tuptable->tupdesc, 2);
		values[1] = val ? CStringGetTextDatum(val) : (Datum) 0;
		nulls[1] = (val == NULL);

		val = SPI_getvalue(spi_tuple, SPI_tuptable->tupdesc, 3);
		values[2] = val ? CStringGetTextDatum(val) : (Datum) 0;
		nulls[2] = (val == NULL);

		val = SPI_getvalue(spi_tuple, SPI_tuptable->tupdesc, 4);
		values[3] = val ? CStringGetTextDatum(val) : (Datum) 0;
		nulls[3] = (val == NULL);

		val = SPI_getvalue(spi_tuple, SPI_tuptable->tupdesc, 5);
		values[4] = val ? CStringGetTextDatum(val) : (Datum) 0;
		nulls[4] = (val == NULL);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	SPI_finish();

	return (Datum) 0;
}

/*
 * index_advisor_unused_indexes()
 *
 * Finds indexes with zero scans that are not primary key indexes.
 *
 * Returns TABLE(index_name text, table_name text, index_size text,
 *               idx_scan bigint)
 */
Datum
index_advisor_unused_indexes(PG_FUNCTION_ARGS)
{
#define IDX_UNUSED_COLS 4
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int				ret;
	uint64			proc;
	uint64			i;

	static const char *unused_sql =
		"SELECT "
		"  sui.schemaname || '.' || sui.indexrelname AS index_name, "
		"  sui.schemaname || '.' || sui.relname AS table_name, "
		"  pg_size_pretty(pg_relation_size(sui.indexrelid)) AS index_size, "
		"  sui.idx_scan "
		"FROM pg_stat_user_indexes sui "
		"JOIN pg_index pi ON pi.indexrelid = sui.indexrelid "
		"WHERE sui.idx_scan = 0 "
		"  AND NOT pi.indisprimary "
		"  AND NOT pi.indisunique "
		"ORDER BY pg_relation_size(sui.indexrelid) DESC "
		"LIMIT 50";

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	SPI_connect();

	ret = SPI_execute(unused_sql, true, 0);
	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("index_advisor_unused_indexes: SPI_execute failed: %s",
						SPI_result_code_string(ret))));
	}

	proc = SPI_processed;

	for (i = 0; i < proc; i++)
	{
		Datum		values[IDX_UNUSED_COLS];
		bool		nulls[IDX_UNUSED_COLS];
		HeapTuple	spi_tuple;
		bool		isnull;
		char	   *val;
		Datum		datum;

		memset(nulls, 0, sizeof(nulls));

		spi_tuple = SPI_tuptable->vals[i];

		/* index_name text */
		val = SPI_getvalue(spi_tuple, SPI_tuptable->tupdesc, 1);
		values[0] = val ? CStringGetTextDatum(val) : (Datum) 0;
		nulls[0] = (val == NULL);

		/* table_name text */
		val = SPI_getvalue(spi_tuple, SPI_tuptable->tupdesc, 2);
		values[1] = val ? CStringGetTextDatum(val) : (Datum) 0;
		nulls[1] = (val == NULL);

		/* index_size text */
		val = SPI_getvalue(spi_tuple, SPI_tuptable->tupdesc, 3);
		values[2] = val ? CStringGetTextDatum(val) : (Datum) 0;
		nulls[2] = (val == NULL);

		/* idx_scan bigint */
		datum = SPI_getbinval(spi_tuple, SPI_tuptable->tupdesc, 4, &isnull);
		values[3] = datum;
		nulls[3] = isnull;

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	SPI_finish();

	return (Datum) 0;
}

/*
 * autovacuum_suggestions()
 *
 * Identifies tables with high dead tuple ratios that may benefit from
 * manual VACUUM or autovacuum tuning.
 *
 * Returns TABLE(table_name text, dead_tuples bigint, live_tuples bigint,
 *               dead_ratio float8, last_autovacuum text, suggestion text)
 */
Datum
autovacuum_suggestions(PG_FUNCTION_ARGS)
{
#define AUTOVAC_COLS 6
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int				ret;
	uint64			proc;
	uint64			i;

	static const char *autovac_sql =
		"SELECT "
		"  schemaname || '.' || relname AS table_name, "
		"  n_dead_tup AS dead_tuples, "
		"  n_live_tup AS live_tuples, "
		"  CASE WHEN n_live_tup > 0 "
		"    THEN ROUND((n_dead_tup::numeric / n_live_tup::numeric) * 100, 2) "
		"    ELSE 0 END AS dead_ratio, "
		"  COALESCE(last_autovacuum::text, 'never') AS last_autovacuum, "
		"  CASE "
		"    WHEN n_live_tup > 0 AND (n_dead_tup::numeric / n_live_tup::numeric) > 0.2 "
		"      THEN 'VACUUM ANALYZE recommended - dead tuple ratio > 20%' "
		"    WHEN n_live_tup > 0 AND (n_dead_tup::numeric / n_live_tup::numeric) > 0.1 "
		"      THEN 'Consider lowering autovacuum_vacuum_scale_factor for this table' "
		"    WHEN last_autovacuum IS NULL AND n_dead_tup > 10000 "
		"      THEN 'Autovacuum has never run - check autovacuum settings' "
		"    ELSE 'Monitor - dead tuple count is elevated' "
		"  END AS suggestion "
		"FROM pg_stat_user_tables "
		"WHERE n_dead_tup > 1000 "
		"  AND (n_live_tup = 0 OR (n_dead_tup::numeric / GREATEST(n_live_tup, 1)::numeric) > 0.05) "
		"ORDER BY n_dead_tup DESC "
		"LIMIT 50";

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	SPI_connect();

	ret = SPI_execute(autovac_sql, true, 0);
	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("autovacuum_suggestions: SPI_execute failed: %s",
						SPI_result_code_string(ret))));
	}

	proc = SPI_processed;

	for (i = 0; i < proc; i++)
	{
		Datum		values[AUTOVAC_COLS];
		bool		nulls[AUTOVAC_COLS];
		HeapTuple	spi_tuple;
		bool		isnull;
		char	   *val;
		Datum		datum;

		memset(nulls, 0, sizeof(nulls));

		spi_tuple = SPI_tuptable->vals[i];

		/* table_name text */
		val = SPI_getvalue(spi_tuple, SPI_tuptable->tupdesc, 1);
		values[0] = val ? CStringGetTextDatum(val) : (Datum) 0;
		nulls[0] = (val == NULL);

		/* dead_tuples bigint */
		datum = SPI_getbinval(spi_tuple, SPI_tuptable->tupdesc, 2, &isnull);
		values[1] = datum;
		nulls[1] = isnull;

		/* live_tuples bigint */
		datum = SPI_getbinval(spi_tuple, SPI_tuptable->tupdesc, 3, &isnull);
		values[2] = datum;
		nulls[2] = isnull;

		/* dead_ratio float8 */
		datum = SPI_getbinval(spi_tuple, SPI_tuptable->tupdesc, 4, &isnull);
		if (!isnull)
		{
			/*
			 * The SQL ROUND(...numeric...) returns numeric.  Convert to float8
			 * for our output column.
			 */
			char   *numstr = SPI_getvalue(spi_tuple, SPI_tuptable->tupdesc, 4);
			values[3] = Float8GetDatum(numstr ? atof(numstr) : 0.0);
			nulls[3] = false;
		}
		else
		{
			values[3] = Float8GetDatum(0.0);
			nulls[3] = true;
		}

		/* last_autovacuum text */
		val = SPI_getvalue(spi_tuple, SPI_tuptable->tupdesc, 5);
		values[4] = val ? CStringGetTextDatum(val) : (Datum) 0;
		nulls[4] = (val == NULL);

		/* suggestion text */
		val = SPI_getvalue(spi_tuple, SPI_tuptable->tupdesc, 6);
		values[5] = val ? CStringGetTextDatum(val) : (Datum) 0;
		nulls[5] = (val == NULL);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	SPI_finish();

	return (Datum) 0;
}
