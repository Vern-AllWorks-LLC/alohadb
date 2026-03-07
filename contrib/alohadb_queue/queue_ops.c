/*-------------------------------------------------------------------------
 *
 * queue_ops.c
 *	  Queue CRUD operations and message send/receive for alohadb_queue.
 *
 *	  All SQL-callable functions are implemented here with their
 *	  PG_FUNCTION_INFO_V1 declarations co-located (required by the
 *	  PostgreSQL fmgr interface).
 *
 *	  SPI lifecycle pattern used throughout:
 *	    SPI_connect -> PushActiveSnapshot -> operations ->
 *	    PopActiveSnapshot -> SPI_finish
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_queue/queue_ops.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/array.h"
#include "catalog/pg_type.h"
#include "access/htup_details.h"

#include "alohadb_queue.h"

/* ----------------------------------------------------------------
 * PG_FUNCTION_INFO_V1 declarations -- must be in the same file
 * as the function implementations.
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(queue_create);
PG_FUNCTION_INFO_V1(queue_drop);
PG_FUNCTION_INFO_V1(queue_send);
PG_FUNCTION_INFO_V1(queue_send_batch);
PG_FUNCTION_INFO_V1(queue_receive);
PG_FUNCTION_INFO_V1(queue_ack);
PG_FUNCTION_INFO_V1(queue_nack);
PG_FUNCTION_INFO_V1(queue_purge);
PG_FUNCTION_INFO_V1(queue_stats);
PG_FUNCTION_INFO_V1(queue_subscribe);
PG_FUNCTION_INFO_V1(queue_poll);
PG_FUNCTION_INFO_V1(queue_commit_offset);


/* ----------------------------------------------------------------
 * queue_create
 *
 * Create a new named queue with the given visibility timeout and
 * retention period.
 *
 * SQL signature:
 *   queue_create(name text,
 *                visibility_timeout interval DEFAULT '30 seconds',
 *                retention_period interval DEFAULT '7 days')
 *   RETURNS void
 * ---------------------------------------------------------------- */
Datum
queue_create(PG_FUNCTION_ARGS)
{
	text	   *name = PG_GETARG_TEXT_PP(0);
	char	   *name_cstr;
	char	   *vis_cstr;
	char	   *ret_cstr;
	StringInfoData sql;
	int			ret;

	name_cstr = text_to_cstring(name);

	/* Convert interval arguments to text via interval_out */
	vis_cstr = DatumGetCString(DirectFunctionCall1(interval_out,
												   PG_GETARG_DATUM(1)));
	ret_cstr = DatumGetCString(DirectFunctionCall1(interval_out,
												   PG_GETARG_DATUM(2)));

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "INSERT INTO %s (name, visibility_timeout, retention_period) "
					 "VALUES (%s, %s::interval, %s::interval)",
					 QUEUE_TABLE_QUEUES,
					 quote_literal_cstr(name_cstr),
					 quote_literal_cstr(vis_cstr),
					 quote_literal_cstr(ret_cstr));

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_INSERT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_queue: failed to create queue \"%s\"",
						name_cstr)));

	PopActiveSnapshot();
	SPI_finish();

	/*
	 * Do not pfree after SPI_finish -- memory allocated in the SPI context
	 * is already freed.  Caller's context is cleaned up at end of statement.
	 */

	PG_RETURN_VOID();
}


/* ----------------------------------------------------------------
 * queue_drop
 *
 * Drop a named queue and all its messages.
 *
 * SQL signature:
 *   queue_drop(name text) RETURNS void
 * ---------------------------------------------------------------- */
Datum
queue_drop(PG_FUNCTION_ARGS)
{
	text	   *name = PG_GETARG_TEXT_PP(0);
	char	   *name_cstr;
	StringInfoData sql;
	int			ret;

	name_cstr = text_to_cstring(name);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* Delete messages first (FK constraint) */
	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "DELETE FROM %s WHERE queue_name = %s",
					 QUEUE_TABLE_MESSAGES,
					 quote_literal_cstr(name_cstr));

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_DELETE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_queue: failed to delete messages for queue \"%s\"",
						name_cstr)));

	/* Delete consumer subscriptions */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
					 "DELETE FROM %s WHERE queue_name = %s",
					 QUEUE_TABLE_CONSUMERS,
					 quote_literal_cstr(name_cstr));

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_DELETE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_queue: failed to delete consumers for queue \"%s\"",
						name_cstr)));

	/* Delete the queue itself */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
					 "DELETE FROM %s WHERE name = %s",
					 QUEUE_TABLE_QUEUES,
					 quote_literal_cstr(name_cstr));

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_DELETE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_queue: failed to drop queue \"%s\"",
						name_cstr)));

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_VOID();
}


/* ----------------------------------------------------------------
 * queue_send
 *
 * Send a single message to a queue.  Returns the msg_id of the
 * newly inserted message.
 *
 * SQL signature:
 *   queue_send(queue_name text, body jsonb,
 *              headers jsonb DEFAULT NULL) RETURNS bigint
 * ---------------------------------------------------------------- */
Datum
queue_send(PG_FUNCTION_ARGS)
{
	text	   *queue_name = PG_GETARG_TEXT_PP(0);
	char	   *qname_cstr;
	char	   *body_cstr;
	char	   *headers_cstr = NULL;
	StringInfoData sql;
	int			ret;
	int64		msg_id;
	bool		isnull;

	qname_cstr = text_to_cstring(queue_name);

	/* Read jsonb body as C string via jsonb_out */
	body_cstr = DatumGetCString(DirectFunctionCall1(jsonb_out,
													PG_GETARG_DATUM(1)));

	/* Headers are optional */
	if (!PG_ARGISNULL(2))
		headers_cstr = DatumGetCString(DirectFunctionCall1(jsonb_out,
														   PG_GETARG_DATUM(2)));

	initStringInfo(&sql);
	if (headers_cstr != NULL)
		appendStringInfo(&sql,
						 "INSERT INTO %s (queue_name, body, headers, created_at, updated_at) "
						 "VALUES (%s, %s::jsonb, %s::jsonb, now(), now()) "
						 "RETURNING msg_id",
						 QUEUE_TABLE_MESSAGES,
						 quote_literal_cstr(qname_cstr),
						 quote_literal_cstr(body_cstr),
						 quote_literal_cstr(headers_cstr));
	else
		appendStringInfo(&sql,
						 "INSERT INTO %s (queue_name, body, created_at, updated_at) "
						 "VALUES (%s, %s::jsonb, now(), now()) "
						 "RETURNING msg_id",
						 QUEUE_TABLE_MESSAGES,
						 quote_literal_cstr(qname_cstr),
						 quote_literal_cstr(body_cstr));

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_INSERT_RETURNING || SPI_processed != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_queue: failed to send message to queue \"%s\"",
						qname_cstr)));

	msg_id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
										 SPI_tuptable->tupdesc,
										 1, &isnull));

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_INT64(msg_id);
}


/* ----------------------------------------------------------------
 * queue_send_batch
 *
 * Send multiple messages to a queue from a jsonb array.
 * Returns the count of messages inserted.
 *
 * SQL signature:
 *   queue_send_batch(queue_name text, bodies jsonb[])
 *   RETURNS bigint
 * ---------------------------------------------------------------- */
Datum
queue_send_batch(PG_FUNCTION_ARGS)
{
	text	   *queue_name = PG_GETARG_TEXT_PP(0);
	ArrayType  *bodies_array = PG_GETARG_ARRAYTYPE_P(1);
	char	   *qname_cstr;
	Datum	   *elems;
	bool	   *elem_nulls;
	int			nelems;
	int			i;
	int64		count = 0;
	StringInfoData sql;
	int			ret;

	qname_cstr = text_to_cstring(queue_name);

	/* Deconstruct the jsonb[] array */
	deconstruct_array(bodies_array, JSONBOID, -1, false, TYPALIGN_INT,
					  &elems, &elem_nulls, &nelems);

	if (nelems == 0)
	{
		pfree(qname_cstr);
		PG_RETURN_INT64(0);
	}

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/*
	 * Build a multi-row INSERT for efficiency:
	 * INSERT INTO ... (queue_name, body, created_at, updated_at)
	 * VALUES (...), (...), ...
	 */
	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "INSERT INTO %s (queue_name, body, created_at, updated_at) VALUES ",
					 QUEUE_TABLE_MESSAGES);

	for (i = 0; i < nelems; i++)
	{
		char   *body_cstr;

		if (elem_nulls[i])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("alohadb_queue: message body must not be NULL")));

		body_cstr = DatumGetCString(DirectFunctionCall1(jsonb_out, elems[i]));

		if (i > 0)
			appendStringInfoString(&sql, ", ");

		appendStringInfo(&sql,
						 "(%s, %s::jsonb, now(), now())",
						 quote_literal_cstr(qname_cstr),
						 quote_literal_cstr(body_cstr));

		pfree(body_cstr);
	}

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_INSERT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_queue: failed to send batch to queue \"%s\"",
						qname_cstr)));

	count = SPI_processed;

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_INT64(count);
}


/* ----------------------------------------------------------------
 * queue_receive
 *
 * Receive messages from a queue using SELECT ... FOR UPDATE SKIP
 * LOCKED to allow concurrent consumers.  Messages are atomically
 * marked as 'delivered' with an updated visible_after timestamp.
 *
 * SQL signature:
 *   queue_receive(queue_name text,
 *                 batch_size int DEFAULT 1,
 *                 visibility_timeout interval DEFAULT NULL)
 *   RETURNS TABLE(msg_id bigint, body jsonb,
 *                 headers jsonb, delivery_count int)
 * ---------------------------------------------------------------- */
Datum
queue_receive(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	text	   *queue_name = PG_GETARG_TEXT_PP(0);
	int32		batch_size = PG_GETARG_INT32(1);
	char	   *qname_cstr;
	char	   *vis_timeout_cstr = NULL;
	StringInfoData sql;
	int			ret;
	uint64		nrows;
	uint64		i;

	qname_cstr = text_to_cstring(queue_name);

	/* visibility_timeout is optional; NULL means use queue default */
	/* visibility_timeout arg is interval type; convert via interval_out */
	if (!PG_ARGISNULL(2))
	{
		vis_timeout_cstr = DatumGetCString(DirectFunctionCall1(interval_out,
															   PG_GETARG_DATUM(2)));
	}

	/* Initialize the SRF using the expected tuple descriptor */
	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/*
	 * Step 1: Lock and fetch ready messages.
	 *
	 * We use a CTE to atomically SELECT + UPDATE: the CTE selects the
	 * candidate rows with FOR UPDATE SKIP LOCKED, and the outer UPDATE
	 * sets the status to 'delivered' and bumps delivery_count.
	 */
	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "WITH candidates AS ("
					 "  SELECT msg_id FROM %s"
					 "  WHERE queue_name = %s"
					 "    AND status = '%s'"
					 "    AND visible_after <= now()"
					 "  ORDER BY msg_id"
					 "  FOR UPDATE SKIP LOCKED"
					 "  LIMIT %d"
					 ") "
					 "UPDATE %s m SET"
					 "  status = '%s',"
					 "  visible_after = now() + COALESCE(%s,"
					 "    (SELECT visibility_timeout FROM %s WHERE name = %s)),"
					 "  delivery_count = m.delivery_count + 1,"
					 "  updated_at = now() "
					 "FROM candidates c "
					 "WHERE m.msg_id = c.msg_id "
					 "RETURNING m.msg_id, m.body, m.headers, m.delivery_count",
					 QUEUE_TABLE_MESSAGES,
					 quote_literal_cstr(qname_cstr),
					 QUEUE_STATUS_READY,
					 batch_size,
					 QUEUE_TABLE_MESSAGES,
					 QUEUE_STATUS_DELIVERED,
					 vis_timeout_cstr
						? quote_literal_cstr(vis_timeout_cstr)
						: "NULL::interval",
					 QUEUE_TABLE_QUEUES,
					 quote_literal_cstr(qname_cstr));

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_UPDATE_RETURNING)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_queue: failed to receive from queue \"%s\"",
						qname_cstr)));

	/* Save row count before SPI_finish */
	nrows = SPI_processed;

	/* Copy results into the tuplestore */
	for (i = 0; i < nrows; i++)
	{
		Datum		values[4];
		bool		nulls[4];
		HeapTuple	tuple;
		bool		isnull;

		memset(nulls, 0, sizeof(nulls));

		/* msg_id (bigint) */
		values[0] = SPI_getbinval(SPI_tuptable->vals[i],
								  SPI_tuptable->tupdesc, 1, &isnull);
		nulls[0] = isnull;

		/* body (jsonb) */
		values[1] = SPI_getbinval(SPI_tuptable->vals[i],
								  SPI_tuptable->tupdesc, 2, &isnull);
		nulls[1] = isnull;

		/* headers (jsonb) */
		values[2] = SPI_getbinval(SPI_tuptable->vals[i],
								  SPI_tuptable->tupdesc, 3, &isnull);
		nulls[2] = isnull;

		/* delivery_count (int) */
		values[3] = SPI_getbinval(SPI_tuptable->vals[i],
								  SPI_tuptable->tupdesc, 4, &isnull);
		nulls[3] = isnull;

		tuple = heap_form_tuple(rsinfo->setDesc, values, nulls);
		tuplestore_puttuple(rsinfo->setResult, tuple);
	}

	PopActiveSnapshot();
	SPI_finish();

	return (Datum) 0;
}


/* ----------------------------------------------------------------
 * queue_ack
 *
 * Acknowledge a message, marking it as processed.
 *
 * SQL signature:
 *   queue_ack(queue_name text, msg_id bigint) RETURNS boolean
 * ---------------------------------------------------------------- */
Datum
queue_ack(PG_FUNCTION_ARGS)
{
	text	   *queue_name = PG_GETARG_TEXT_PP(0);
	int64		msg_id = PG_GETARG_INT64(1);
	char	   *qname_cstr;
	StringInfoData sql;
	int			ret;
	uint64		nrows;

	qname_cstr = text_to_cstring(queue_name);

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "UPDATE %s SET status = '%s', updated_at = now() "
					 "WHERE msg_id = %lld AND queue_name = %s",
					 QUEUE_TABLE_MESSAGES,
					 QUEUE_STATUS_ACKNOWLEDGED,
					 (long long) msg_id,
					 quote_literal_cstr(qname_cstr));

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_UPDATE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_queue: failed to ack message %lld",
						(long long) msg_id)));

	nrows = SPI_processed;

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_BOOL(nrows > 0);
}


/* ----------------------------------------------------------------
 * queue_nack
 *
 * Negatively acknowledge a message, returning it to the ready
 * state for immediate re-delivery.
 *
 * SQL signature:
 *   queue_nack(queue_name text, msg_id bigint) RETURNS boolean
 * ---------------------------------------------------------------- */
Datum
queue_nack(PG_FUNCTION_ARGS)
{
	text	   *queue_name = PG_GETARG_TEXT_PP(0);
	int64		msg_id = PG_GETARG_INT64(1);
	char	   *qname_cstr;
	StringInfoData sql;
	int			ret;
	uint64		nrows;

	qname_cstr = text_to_cstring(queue_name);

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "UPDATE %s SET status = '%s', visible_after = now(), "
					 "updated_at = now() "
					 "WHERE msg_id = %lld AND queue_name = %s",
					 QUEUE_TABLE_MESSAGES,
					 QUEUE_STATUS_READY,
					 (long long) msg_id,
					 quote_literal_cstr(qname_cstr));

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_UPDATE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_queue: failed to nack message %lld",
						(long long) msg_id)));

	nrows = SPI_processed;

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_BOOL(nrows > 0);
}


/* ----------------------------------------------------------------
 * queue_purge
 *
 * Delete all messages from a queue.  Returns the count of deleted
 * messages.
 *
 * SQL signature:
 *   queue_purge(queue_name text) RETURNS bigint
 * ---------------------------------------------------------------- */
Datum
queue_purge(PG_FUNCTION_ARGS)
{
	text	   *queue_name = PG_GETARG_TEXT_PP(0);
	char	   *qname_cstr;
	StringInfoData sql;
	int			ret;
	uint64		deleted;

	qname_cstr = text_to_cstring(queue_name);

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "DELETE FROM %s WHERE queue_name = %s",
					 QUEUE_TABLE_MESSAGES,
					 quote_literal_cstr(qname_cstr));

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_DELETE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_queue: failed to purge queue \"%s\"",
						qname_cstr)));

	/* Save count before SPI_finish */
	deleted = SPI_processed;

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_INT64((int64) deleted);
}


/* ----------------------------------------------------------------
 * queue_stats
 *
 * Return message count statistics for a queue, broken down by
 * status.
 *
 * SQL signature:
 *   queue_stats(queue_name text)
 *   RETURNS TABLE(total bigint, ready bigint,
 *                 delivered bigint, acknowledged bigint)
 * ---------------------------------------------------------------- */
Datum
queue_stats(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	text	   *queue_name = PG_GETARG_TEXT_PP(0);
	char	   *qname_cstr;
	StringInfoData sql;
	int			ret;
	Datum		values[4];
	bool		nulls[4];
	HeapTuple	tuple;
	bool		isnull;

	qname_cstr = text_to_cstring(queue_name);

	/* Initialize the SRF using the expected tuple descriptor */
	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT "
					 "  count(*) AS total, "
					 "  count(*) FILTER (WHERE status = '%s') AS ready, "
					 "  count(*) FILTER (WHERE status = '%s') AS delivered, "
					 "  count(*) FILTER (WHERE status = '%s') AS acknowledged "
					 "FROM %s WHERE queue_name = %s",
					 QUEUE_STATUS_READY,
					 QUEUE_STATUS_DELIVERED,
					 QUEUE_STATUS_ACKNOWLEDGED,
					 QUEUE_TABLE_MESSAGES,
					 quote_literal_cstr(qname_cstr));

	ret = SPI_execute(sql.data, true, 0);
	if (ret != SPI_OK_SELECT || SPI_processed != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_queue: failed to get stats for queue \"%s\"",
						qname_cstr)));

	memset(nulls, 0, sizeof(nulls));

	values[0] = SPI_getbinval(SPI_tuptable->vals[0],
							  SPI_tuptable->tupdesc, 1, &isnull);
	nulls[0] = isnull;

	values[1] = SPI_getbinval(SPI_tuptable->vals[0],
							  SPI_tuptable->tupdesc, 2, &isnull);
	nulls[1] = isnull;

	values[2] = SPI_getbinval(SPI_tuptable->vals[0],
							  SPI_tuptable->tupdesc, 3, &isnull);
	nulls[2] = isnull;

	values[3] = SPI_getbinval(SPI_tuptable->vals[0],
							  SPI_tuptable->tupdesc, 4, &isnull);
	nulls[3] = isnull;

	tuple = heap_form_tuple(rsinfo->setDesc, values, nulls);
	tuplestore_puttuple(rsinfo->setResult, tuple);

	PopActiveSnapshot();
	SPI_finish();

	return (Datum) 0;
}


/* ----------------------------------------------------------------
 * queue_subscribe
 *
 * Register a consumer group for a queue.  Creates or updates the
 * consumer offset tracking entry.
 *
 * SQL signature:
 *   queue_subscribe(consumer_group text, queue_name text)
 *   RETURNS void
 * ---------------------------------------------------------------- */
Datum
queue_subscribe(PG_FUNCTION_ARGS)
{
	text	   *consumer_group = PG_GETARG_TEXT_PP(0);
	text	   *queue_name = PG_GETARG_TEXT_PP(1);
	char	   *cg_cstr;
	char	   *qname_cstr;
	StringInfoData sql;
	int			ret;

	cg_cstr = text_to_cstring(consumer_group);
	qname_cstr = text_to_cstring(queue_name);

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "INSERT INTO %s (consumer_group, queue_name, last_offset) "
					 "VALUES (%s, %s, 0) "
					 "ON CONFLICT (consumer_group, queue_name) DO NOTHING",
					 QUEUE_TABLE_CONSUMERS,
					 quote_literal_cstr(cg_cstr),
					 quote_literal_cstr(qname_cstr));

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_INSERT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_queue: failed to subscribe consumer group \"%s\" "
						"to queue \"%s\"",
						cg_cstr, qname_cstr)));

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_VOID();
}


/* ----------------------------------------------------------------
 * queue_poll
 *
 * Poll for messages using a consumer group's offset.  Unlike
 * queue_receive, this does NOT lock or update message status;
 * it reads messages with msg_id > last_offset for the consumer
 * group, similar to a Kafka-style consumer.
 *
 * SQL signature:
 *   queue_poll(consumer_group text, queue_name text,
 *              batch_size int DEFAULT 1)
 *   RETURNS TABLE(msg_id bigint, body jsonb,
 *                 headers jsonb, delivery_count int)
 * ---------------------------------------------------------------- */
Datum
queue_poll(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	text	   *consumer_group = PG_GETARG_TEXT_PP(0);
	text	   *queue_name = PG_GETARG_TEXT_PP(1);
	int32		batch_size = PG_GETARG_INT32(2);
	char	   *cg_cstr;
	char	   *qname_cstr;
	StringInfoData sql;
	int			ret;
	uint64		nrows;
	uint64		i;

	cg_cstr = text_to_cstring(consumer_group);
	qname_cstr = text_to_cstring(queue_name);

	/* Initialize the SRF using the expected tuple descriptor */
	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/*
	 * Select messages with msg_id greater than the consumer group's
	 * last committed offset.
	 */
	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT m.msg_id, m.body, m.headers, m.delivery_count "
					 "FROM %s m "
					 "JOIN %s c ON c.queue_name = m.queue_name "
					 "WHERE m.queue_name = %s "
					 "  AND c.consumer_group = %s "
					 "  AND m.msg_id > c.last_offset "
					 "ORDER BY m.msg_id "
					 "LIMIT %d",
					 QUEUE_TABLE_MESSAGES,
					 QUEUE_TABLE_CONSUMERS,
					 quote_literal_cstr(qname_cstr),
					 quote_literal_cstr(cg_cstr),
					 batch_size);

	ret = SPI_execute(sql.data, true, 0);
	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_queue: failed to poll queue \"%s\" for "
						"consumer group \"%s\"",
						qname_cstr, cg_cstr)));

	/* Save row count before iterating */
	nrows = SPI_processed;

	/* Copy results into the tuplestore */
	for (i = 0; i < nrows; i++)
	{
		Datum		values[4];
		bool		nulls[4];
		HeapTuple	tuple;
		bool		isnull;

		memset(nulls, 0, sizeof(nulls));

		values[0] = SPI_getbinval(SPI_tuptable->vals[i],
								  SPI_tuptable->tupdesc, 1, &isnull);
		nulls[0] = isnull;

		values[1] = SPI_getbinval(SPI_tuptable->vals[i],
								  SPI_tuptable->tupdesc, 2, &isnull);
		nulls[1] = isnull;

		values[2] = SPI_getbinval(SPI_tuptable->vals[i],
								  SPI_tuptable->tupdesc, 3, &isnull);
		nulls[2] = isnull;

		values[3] = SPI_getbinval(SPI_tuptable->vals[i],
								  SPI_tuptable->tupdesc, 4, &isnull);
		nulls[3] = isnull;

		tuple = heap_form_tuple(rsinfo->setDesc, values, nulls);
		tuplestore_puttuple(rsinfo->setResult, tuple);
	}

	PopActiveSnapshot();
	SPI_finish();

	return (Datum) 0;
}


/* ----------------------------------------------------------------
 * queue_commit_offset
 *
 * Update the consumer group's last_offset for a queue, marking
 * all messages up to and including offset_val as consumed.
 *
 * SQL signature:
 *   queue_commit_offset(consumer_group text, queue_name text,
 *                       offset_val bigint)
 *   RETURNS void
 * ---------------------------------------------------------------- */
Datum
queue_commit_offset(PG_FUNCTION_ARGS)
{
	text	   *consumer_group = PG_GETARG_TEXT_PP(0);
	text	   *queue_name = PG_GETARG_TEXT_PP(1);
	int64		offset_val = PG_GETARG_INT64(2);
	char	   *cg_cstr;
	char	   *qname_cstr;
	StringInfoData sql;
	int			ret;
	uint64		nrows;

	cg_cstr = text_to_cstring(consumer_group);
	qname_cstr = text_to_cstring(queue_name);

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "UPDATE %s SET last_offset = %lld "
					 "WHERE consumer_group = %s AND queue_name = %s",
					 QUEUE_TABLE_CONSUMERS,
					 (long long) offset_val,
					 quote_literal_cstr(cg_cstr),
					 quote_literal_cstr(qname_cstr));

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_UPDATE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_queue: failed to commit offset for "
						"consumer group \"%s\" on queue \"%s\"",
						cg_cstr, qname_cstr)));

	nrows = SPI_processed;
	if (nrows == 0)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("alohadb_queue: consumer group \"%s\" is not "
						"subscribed to queue \"%s\"",
						cg_cstr, qname_cstr)));

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_VOID();
}
