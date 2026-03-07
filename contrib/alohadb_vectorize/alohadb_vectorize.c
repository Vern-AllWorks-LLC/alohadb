/*-------------------------------------------------------------------------
 *
 * alohadb_vectorize.c
 *    AlohaDB Vectorize - analytical query acceleration layer
 *
 * Provides SQL functions for executing analytical queries with optimized
 * GUC settings (parallel workers, work_mem, JIT) and benchmarking tools.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "utils/guc.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "access/tupdesc.h"
#include "catalog/pg_type.h"
#include "utils/lsyscache.h"

#include <time.h>

PG_MODULE_MAGIC_EXT(.name = "alohadb_vectorize", .version = "1.0");

/* GUC settings we apply for vectorized execution */
typedef struct VectorizeGUC
{
	const char *name;
	const char *value;
} VectorizeGUC;

static const VectorizeGUC vectorize_gucs[] = {
	{"work_mem", "256MB"},
	{"max_parallel_workers_per_gather", "4"},
	{"parallel_tuple_cost", "0.001"},
	{"parallel_setup_cost", "100"},
	{"jit", "on"},
	{"jit_above_cost", "10000"},
	{"jit_inline_above_cost", "50000"},
	{"jit_optimize_above_cost", "50000"},
	{NULL, NULL}
};

/* Status GUCs we report */
static const char *status_guc_names[] = {
	"work_mem",
	"max_parallel_workers_per_gather",
	"max_parallel_workers",
	"parallel_tuple_cost",
	"parallel_setup_cost",
	"jit",
	"jit_above_cost",
	"jit_inline_above_cost",
	"jit_optimize_above_cost",
	"enable_parallel_append",
	"enable_parallel_hash",
	"enable_hashagg",
	"enable_hashjoin",
	NULL
};

/*
 * Apply vectorize-friendly GUC settings via SET LOCAL.
 * Must be called inside an SPI connection.
 */
static void
apply_vectorize_gucs(void)
{
	StringInfoData buf;
	int			i;

	initStringInfo(&buf);

	for (i = 0; vectorize_gucs[i].name != NULL; i++)
	{
		resetStringInfo(&buf);
		appendStringInfo(&buf, "SET LOCAL %s = '%s'",
						 vectorize_gucs[i].name,
						 vectorize_gucs[i].value);

		if (SPI_execute(buf.data, false, 0) < 0)
			elog(WARNING, "alohadb_vectorize: failed to set %s",
				 vectorize_gucs[i].name);
	}

	pfree(buf.data);
}

/* ----------------------------------------------------------------
 * vectorize_query(sql_text text) -> SETOF record
 *
 * Executes an analytical query with optimized GUC settings.
 * Caller must specify column definitions in FROM clause:
 *   SELECT * FROM vectorize_query('...') AS t(c1 type1, c2 type2);
 * ----------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(vectorize_query);
Datum
vectorize_query(PG_FUNCTION_ARGS)
{
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	text		   *sql_text;
	char		   *sql;
	uint64			proc_count;
	uint64			i;
	int				ret;
	int				natts;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("sql_text must not be NULL")));

	sql_text = PG_GETARG_TEXT_PP(0);
	sql = text_to_cstring(sql_text);

	/* Set up tuplestore before SPI so it's in the right memory context */
	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "alohadb_vectorize: SPI_connect failed");

	PushActiveSnapshot(GetTransactionSnapshot());

	/* Apply vectorize-friendly GUCs */
	apply_vectorize_gucs();

	/* Execute the user query */
	ret = SPI_execute(sql, true, 0);
	if (ret < 0)
	{
		PopActiveSnapshot();
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_vectorize: query execution failed: %s", sql)));
	}

	proc_count = SPI_processed;

	if (SPI_tuptable != NULL && proc_count > 0)
	{
		TupleDesc	spi_tupdesc = SPI_tuptable->tupdesc;
		TupleDesc	ret_tupdesc = rsinfo->setDesc;

		natts = ret_tupdesc->natts;

		/* Copy all tuples using text conversion for safety */
		for (i = 0; i < proc_count; i++)
		{
			Datum	   *values;
			bool	   *nulls;
			int			j;

			values = (Datum *) palloc(natts * sizeof(Datum));
			nulls = (bool *) palloc0(natts * sizeof(bool));

			for (j = 0; j < natts && j < spi_tupdesc->natts; j++)
			{
				char *val = SPI_getvalue(SPI_tuptable->vals[i],
										 spi_tupdesc, j + 1);
				if (val == NULL)
				{
					nulls[j] = true;
					values[j] = (Datum) 0;
				}
				else
				{
					Oid			typinput;
					Oid			typioparam;
					Form_pg_attribute attr = TupleDescAttr(ret_tupdesc, j);

					getTypeInputInfo(attr->atttypid, &typinput, &typioparam);
					values[j] = OidInputFunctionCall(typinput, val,
													 typioparam,
													 attr->atttypmod);
				}
			}

			/* Fill remaining columns with NULLs */
			for (; j < natts; j++)
			{
				nulls[j] = true;
				values[j] = (Datum) 0;
			}

			tuplestore_putvalues(rsinfo->setResult, ret_tupdesc,
								values, nulls);

			pfree(values);
			pfree(nulls);
		}
	}

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_NULL();
}

/* ----------------------------------------------------------------
 * vectorize_explain(sql_text text) -> TABLE(plan_line text)
 *
 * Returns EXPLAIN ANALYZE output with vectorization annotations.
 * ----------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(vectorize_explain);
Datum
vectorize_explain(PG_FUNCTION_ARGS)
{
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	text		   *sql_text;
	char		   *sql;
	StringInfoData	explain_sql;
	Tuplestorestate *tupstore;
	TupleDesc		tupdesc;
	uint64			proc_count;
	uint64			i;
	int				ret;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("sql_text must not be NULL")));

	sql_text = PG_GETARG_TEXT_PP(0);
	sql = text_to_cstring(sql_text);

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
	tupstore = rsinfo->setResult;
	tupdesc = rsinfo->setDesc;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "alohadb_vectorize: SPI_connect failed");

	PushActiveSnapshot(GetTransactionSnapshot());

	/* Apply vectorize GUCs so the explain reflects optimized settings */
	apply_vectorize_gucs();

	/* Build EXPLAIN query */
	initStringInfo(&explain_sql);
	appendStringInfo(&explain_sql,
					 "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) %s", sql);

	/* read_only=false because EXPLAIN ANALYZE actually executes the query */
	ret = SPI_execute(explain_sql.data, false, 0);
	pfree(explain_sql.data);

	if (ret < 0)
	{
		PopActiveSnapshot();
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_vectorize: EXPLAIN failed for query: %s", sql)));
	}

	proc_count = SPI_processed;

	/* Add a header annotation */
	{
		Datum		values[1];
		bool		nulls[1] = {false};

		values[0] = CStringGetTextDatum("-- AlohaDB Vectorize: Analytical Execution Plan --");
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* Copy EXPLAIN output lines */
	for (i = 0; i < proc_count; i++)
	{
		Datum		values[1];
		bool		nulls[1] = {false};
		char	   *line;

		line = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1);
		if (line != NULL)
			values[0] = CStringGetTextDatum(line);
		else
		{
			nulls[0] = true;
			values[0] = (Datum) 0;
		}

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* Add footer annotation */
	{
		Datum		values[1];
		bool		nulls[1] = {false};

		values[0] = CStringGetTextDatum("-- Vectorize GUCs: work_mem=256MB, parallel_workers=4, jit=on --");
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_NULL();
}

/* ----------------------------------------------------------------
 * vectorize_status() -> TABLE(setting_name text, setting_value text)
 *
 * Returns current GUC values relevant to vectorized execution.
 * ----------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(vectorize_status);
Datum
vectorize_status(PG_FUNCTION_ARGS)
{
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Tuplestorestate *tupstore;
	TupleDesc		tupdesc;
	int				i;

	InitMaterializedSRF(fcinfo, 0);
	tupstore = rsinfo->setResult;
	tupdesc = rsinfo->setDesc;

	for (i = 0; status_guc_names[i] != NULL; i++)
	{
		Datum		values[2];
		bool		nulls[2] = {false, false};
		const char *val;

		val = GetConfigOptionByName(status_guc_names[i], NULL, true);

		values[0] = CStringGetTextDatum(status_guc_names[i]);

		if (val != NULL)
			values[1] = CStringGetTextDatum(val);
		else
		{
			nulls[1] = true;
			values[1] = (Datum) 0;
		}

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	PG_RETURN_NULL();
}

/* ----------------------------------------------------------------
 * vectorize_benchmark(sql_text text, iterations int DEFAULT 10)
 *    -> TABLE(iteration int, execution_time_ms float8)
 *
 * Benchmarks a query over multiple iterations with timing.
 * NOT marked STRICT because iterations has a DEFAULT.
 * ----------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(vectorize_benchmark);
Datum
vectorize_benchmark(PG_FUNCTION_ARGS)
{
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	text		   *sql_text;
	char		   *sql;
	int				iterations;
	Tuplestorestate *tupstore;
	TupleDesc		tupdesc;
	int				iter;
	int				ret;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("sql_text must not be NULL")));

	sql_text = PG_GETARG_TEXT_PP(0);
	sql = text_to_cstring(sql_text);

	/* Handle DEFAULT for iterations */
	if (PG_ARGISNULL(1))
		iterations = 10;
	else
		iterations = PG_GETARG_INT32(1);

	if (iterations < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("iterations must be at least 1")));

	if (iterations > 10000)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("iterations must not exceed 10000")));

	InitMaterializedSRF(fcinfo, 0);
	tupstore = rsinfo->setResult;
	tupdesc = rsinfo->setDesc;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "alohadb_vectorize: SPI_connect failed");

	PushActiveSnapshot(GetTransactionSnapshot());

	/* Apply vectorize GUCs */
	apply_vectorize_gucs();

	for (iter = 1; iter <= iterations; iter++)
	{
		struct timespec	ts_start, ts_end;
		double			elapsed_ms;
		Datum			values[2];
		bool			nulls[2] = {false, false};

		CHECK_FOR_INTERRUPTS();

		clock_gettime(CLOCK_MONOTONIC, &ts_start);

		ret = SPI_execute(sql, true, 0);
		if (ret < 0)
		{
			PopActiveSnapshot();
			SPI_finish();
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("alohadb_vectorize: benchmark query failed at iteration %d: %s",
							iter, sql)));
		}

		clock_gettime(CLOCK_MONOTONIC, &ts_end);

		elapsed_ms = (ts_end.tv_sec - ts_start.tv_sec) * 1000.0 +
					 (ts_end.tv_nsec - ts_start.tv_nsec) / 1000000.0;

		values[0] = Int32GetDatum(iter);
		values[1] = Float8GetDatum(elapsed_ms);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_NULL();
}
