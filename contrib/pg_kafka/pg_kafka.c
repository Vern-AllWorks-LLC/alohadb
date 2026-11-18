/*-------------------------------------------------------------------------
 *
 * pg_kafka.c
 *	  Kafka/Kinesis consumer background worker and SQL management functions.
 *
 *	  The background worker periodically polls enabled consumers and
 *	  writes received messages to pg_kafka_messages.
 *	  Actual protocol I/O is in kafka_consumer.c.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_kafka/pg_kafka.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/latch.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"
#include "tcop/tcopprot.h"

#include "pg_kafka.h"

PG_MODULE_MAGIC_EXT(
					.name = "pg_kafka",
					.version = "1.0"
);

PG_FUNCTION_INFO_V1(kafka_consumer_start);
PG_FUNCTION_INFO_V1(kafka_consumer_stop);
PG_FUNCTION_INFO_V1(kafka_consumer_status);

/* GUC variables */
int			kafka_poll_interval_ms = 1000;
char	   *kafka_database = NULL;

static uint32 kafka_wait_event = 0;

/* ----------------------------------------------------------------
 * kafka_table_exists
 * ---------------------------------------------------------------- */
static bool
kafka_table_exists(void)
{
	int		ret;

	ret = SPI_execute(
		"SELECT 1 FROM pg_catalog.pg_class c "
		"JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
		"WHERE n.nspname = 'public' AND c.relname = 'pg_kafka_consumers'",
		true, 1);
	return (ret == SPI_OK_SELECT && SPI_processed > 0);
}

/* ----------------------------------------------------------------
 * kafka_poll_consumers
 *
 * Poll each active consumer and store any received messages.
 * This function is intentionally kept short: the heavy lifting
 * (rdkafka poll / HTTP Kinesis GetRecords) is in kafka_consumer.c.
 *
 * When USE_RDKAFKA is not defined, we skip actual polling but
 * still update the stats timestamp so the worker stays alive.
 * ---------------------------------------------------------------- */
static void
kafka_poll_consumers(void)
{
	int		ret;
	uint64	n;
	uint64	i;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "pg_kafka: polling consumers");

	if (!kafka_table_exists())
		goto done;

	ret = SPI_execute(
		"SELECT consumer_id, consumer_name, consumer_type, "
		"       brokers, topic_or_stream, consumer_group, "
		"       extra_config "
		"FROM   public.pg_kafka_consumers "
		"WHERE  status = 'running' "
		"ORDER BY consumer_id",
		true, 0);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
		goto done;

	n = SPI_processed;

	/* Copy consumer config out of SPI memory */
	{
		typedef struct ConsumerRec
		{
			int64	consumer_id;
			char	consumer_name[256];
			char	consumer_type[32];
			char	brokers[1024];
			char	topic[256];
			char	group_id[256];
		} ConsumerRec;

		ConsumerRec *consumers;
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(CurTransactionContext);
		consumers = palloc(sizeof(ConsumerRec) * n);
		MemoryContextSwitchTo(oldcxt);

		for (i = 0; i < n; i++)
		{
			bool	isnull;
			char   *val;

			consumers[i].consumer_id = DatumGetInt64(
				SPI_getbinval(SPI_tuptable->vals[i],
							  SPI_tuptable->tupdesc, 1, &isnull));

			val = SPI_getvalue(SPI_tuptable->vals[i],
							   SPI_tuptable->tupdesc, 2);
			strlcpy(consumers[i].consumer_name, val ? val : "",
					sizeof(consumers[i].consumer_name));

			val = SPI_getvalue(SPI_tuptable->vals[i],
							   SPI_tuptable->tupdesc, 3);
			strlcpy(consumers[i].consumer_type, val ? val : "kafka",
					sizeof(consumers[i].consumer_type));

			val = SPI_getvalue(SPI_tuptable->vals[i],
							   SPI_tuptable->tupdesc, 4);
			strlcpy(consumers[i].brokers, val ? val : "",
					sizeof(consumers[i].brokers));

			val = SPI_getvalue(SPI_tuptable->vals[i],
							   SPI_tuptable->tupdesc, 5);
			strlcpy(consumers[i].topic, val ? val : "",
					sizeof(consumers[i].topic));

			val = SPI_getvalue(SPI_tuptable->vals[i],
							   SPI_tuptable->tupdesc, 6);
			strlcpy(consumers[i].group_id, val ? val : "",
					sizeof(consumers[i].group_id));
		}

		PopActiveSnapshot();
		SPI_finish();
		CommitTransactionCommand();

		/*
		 * For each consumer, open a sub-transaction, poll for messages,
		 * and insert them into pg_kafka_messages.
		 *
		 * kafka_poll_once() is defined in kafka_consumer.c; it returns
		 * the number of messages stored.  When compiled without
		 * USE_RDKAFKA, it is a no-op that returns 0.
		 */
		for (i = 0; i < n; i++)
		{
			int64	msgs_received;

			SetCurrentStatementStartTimestamp();
			StartTransactionCommand();
			SPI_connect();
			PushActiveSnapshot(GetTransactionSnapshot());

			msgs_received = kafka_poll_once(consumers[i].consumer_id,
											consumers[i].consumer_name,
											consumers[i].consumer_type,
											consumers[i].brokers,
											consumers[i].topic,
											consumers[i].group_id);

			if (msgs_received > 0)
			{
				StringInfoData sql;
				initStringInfo(&sql);
				appendStringInfo(&sql,
					"UPDATE public.pg_kafka_stats "
					"SET messages_received = messages_received + " INT64_FORMAT ", "
					"    last_polled_at = now() "
					"WHERE consumer_id = " INT64_FORMAT,
					msgs_received, consumers[i].consumer_id);
				SPI_execute(sql.data, false, 0);
				pfree(sql.data);
			}

			PopActiveSnapshot();
			SPI_finish();
			CommitTransactionCommand();
		}

		pgstat_report_activity(STATE_IDLE, NULL);
		return;
	}

done:
	PopActiveSnapshot();
	SPI_finish();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
}

/* ----------------------------------------------------------------
 * Background worker entry point
 * ---------------------------------------------------------------- */
PGDLLEXPORT void
pg_kafka_worker_main(Datum main_arg)
{
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection(
		kafka_database ? kafka_database : "postgres", NULL, 0);

	elog(LOG, "pg_kafka: background worker started, "
		 "poll_interval=%dms, database=\"%s\"",
		 kafka_poll_interval_ms,
		 kafka_database ? kafka_database : "postgres");

	for (;;)
	{
		if (kafka_wait_event == 0)
			kafka_wait_event = WaitEventExtensionNew("AlohadbKafkaMain");

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 (long) kafka_poll_interval_ms,
						 kafka_wait_event);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		kafka_poll_consumers();
		pgstat_report_stat(true);
	}
}

/* ----------------------------------------------------------------
 * _PG_init
 * ---------------------------------------------------------------- */
void
_PG_init(void)
{
	BackgroundWorker worker;

	DefineCustomIntVariable("pg_kafka.poll_interval_ms",
							"Interval between Kafka consumer polls (milliseconds).",
							NULL,
							&kafka_poll_interval_ms,
							1000, 100, 60000,
							PGC_SIGHUP, 0,
							NULL, NULL, NULL);

	DefineCustomStringVariable("pg_kafka.database",
							   "Database the Kafka background worker connects to.",
							   NULL,
							   &kafka_database,
							   "postgres",
							   PGC_SIGHUP, 0,
							   NULL, NULL, NULL);

	if (!process_shared_preload_libraries_in_progress)
		return;

	MarkGUCPrefixReserved("pg_kafka");

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
	snprintf(worker.bgw_library_name, MAXPGPATH, "pg_kafka");
	snprintf(worker.bgw_function_name, BGW_MAXLEN,
			 "pg_kafka_worker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_kafka worker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "pg_kafka");
	worker.bgw_main_arg = (Datum) 0;
	worker.bgw_notify_pid = 0;

	if (!IsBinaryUpgrade)	/* no background work during pg_upgrade */
		RegisterBackgroundWorker(&worker);
}

/* ----------------------------------------------------------------
 * SQL-callable functions
 * ---------------------------------------------------------------- */

/*
 * kafka_consumer_start(consumer_name text) RETURNS void
 */
Datum
kafka_consumer_start(PG_FUNCTION_ARGS)
{
	text	   *name_text = PG_GETARG_TEXT_PP(0);
	char	   *name = text_to_cstring(name_text);
	StringInfoData sql;
	int			ret;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	initStringInfo(&sql);
	appendStringInfo(&sql,
		"UPDATE public.pg_kafka_consumers "
		"SET status = 'running' "
		"WHERE consumer_name = %s",
		quote_literal_cstr(name));

	ret = SPI_execute(sql.data, false, 0);

	PopActiveSnapshot();
	SPI_finish();
	pfree(sql.data);

	if (ret != SPI_OK_UPDATE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("kafka_consumer_start: failed to start consumer \"%s\"",
						name)));

	PG_RETURN_VOID();
}

/*
 * kafka_consumer_stop(consumer_name text) RETURNS void
 */
Datum
kafka_consumer_stop(PG_FUNCTION_ARGS)
{
	text	   *name_text = PG_GETARG_TEXT_PP(0);
	char	   *name = text_to_cstring(name_text);
	StringInfoData sql;
	int			ret;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	initStringInfo(&sql);
	appendStringInfo(&sql,
		"UPDATE public.pg_kafka_consumers "
		"SET status = 'stopped' "
		"WHERE consumer_name = %s",
		quote_literal_cstr(name));

	ret = SPI_execute(sql.data, false, 0);

	PopActiveSnapshot();
	SPI_finish();
	pfree(sql.data);

	if (ret != SPI_OK_UPDATE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("kafka_consumer_stop: failed to stop consumer \"%s\"",
						name)));

	PG_RETURN_VOID();
}

/*
 * kafka_consumer_status() RETURNS TABLE(...)
 */
Datum
kafka_consumer_status(PG_FUNCTION_ARGS)
{
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext	per_query_ctx;
	MemoryContext	oldcxt;
	int				ret;
	uint64			i;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required")));

	tupdesc = CreateTemplateTupleDesc(6);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "consumer_name", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "consumer_type", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "topic_or_stream", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "status", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 5, "messages_received", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 6, "last_polled_at", TIMESTAMPTZOID, -1, 0);

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcxt = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = BlessTupleDesc(tupdesc);

	MemoryContextSwitchTo(oldcxt);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(
		"SELECT c.consumer_name, c.consumer_type, c.topic_or_stream, "
		"       c.status, "
		"       COALESCE(s.messages_received, 0), "
		"       s.last_polled_at "
		"FROM   public.pg_kafka_consumers c "
		"LEFT   JOIN public.pg_kafka_stats s "
		"            ON s.consumer_id = c.consumer_id "
		"ORDER  BY c.consumer_name",
		true, 0);

	if (ret == SPI_OK_SELECT)
	{
		for (i = 0; i < SPI_processed; i++)
		{
			Datum	values[6];
			bool	nulls[6];
			bool	isnull;
			char   *val;

			memset(nulls, 0, sizeof(nulls));

			val = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1);
			values[0] = val ? CStringGetTextDatum(val) : (nulls[0] = true, (Datum) 0);

			val = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2);
			values[1] = val ? CStringGetTextDatum(val) : (nulls[1] = true, (Datum) 0);

			val = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 3);
			values[2] = val ? CStringGetTextDatum(val) : (nulls[2] = true, (Datum) 0);

			val = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 4);
			values[3] = val ? CStringGetTextDatum(val) : (nulls[3] = true, (Datum) 0);

			values[4] = SPI_getbinval(SPI_tuptable->vals[i],
									  SPI_tuptable->tupdesc, 5, &isnull);
			nulls[4] = isnull;

			values[5] = SPI_getbinval(SPI_tuptable->vals[i],
									  SPI_tuptable->tupdesc, 6, &isnull);
			nulls[5] = isnull;

			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}
	}

	PopActiveSnapshot();
	SPI_finish();

	return (Datum) 0;
}
