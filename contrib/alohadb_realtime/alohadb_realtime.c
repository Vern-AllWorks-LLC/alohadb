/*-------------------------------------------------------------------------
 *
 * alohadb_realtime.c
 *	  Trigger-based realtime change notifications with LISTEN/NOTIFY.
 *
 *	  Provides infrastructure to subscribe to row-level changes on any
 *	  table.  When a subscribed table is modified, the trigger captures
 *	  the change as a JSON event, stores it in alohadb_realtime_events,
 *	  and sends an asynchronous NOTIFY with a summary payload.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_realtime/alohadb_realtime.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "commands/async.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_realtime",
					.version = "1.0"
);

/* SQL-callable function declarations */
PG_FUNCTION_INFO_V1(alohadb_realtime_trigger);
PG_FUNCTION_INFO_V1(alohadb_realtime_subscribe);
PG_FUNCTION_INFO_V1(alohadb_realtime_unsubscribe);
PG_FUNCTION_INFO_V1(alohadb_realtime_poll);
PG_FUNCTION_INFO_V1(alohadb_realtime_cleanup);

/* ----------------------------------------------------------------
 * GUC variables
 * ---------------------------------------------------------------- */

static int	realtime_max_events = 100000;

/* ----------------------------------------------------------------
 * _PG_init
 *
 * Module load callback.  Registers GUC variables.
 * ---------------------------------------------------------------- */
void
_PG_init(void)
{
	DefineCustomIntVariable("alohadb.realtime_max_events",
							"Maximum number of events to retain in the events table.",
							"Events beyond this limit may be purged by cleanup.",
							&realtime_max_events,
							100000,
							1000,
							INT_MAX,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);
}

/* ----------------------------------------------------------------
 * Helper: build a JSON object string from a HeapTuple
 *
 * Iterates over all non-dropped attributes of the tuple descriptor
 * and constructs a JSON object using SPI_getvalue() for each column.
 * ---------------------------------------------------------------- */
static char *
tuple_to_json(HeapTuple tuple, TupleDesc tupdesc)
{
	StringInfoData buf;
	int			natts = tupdesc->natts;
	bool		first = true;
	int			i;

	initStringInfo(&buf);
	appendStringInfoChar(&buf, '{');

	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		char	   *colname;
		char	   *colval;

		/* Skip dropped columns */
		if (att->attisdropped)
			continue;

		if (!first)
			appendStringInfoString(&buf, ", ");
		first = false;

		colname = NameStr(att->attname);
		colval = SPI_getvalue(tuple, tupdesc, i + 1);

		/* Emit key */
		appendStringInfoChar(&buf, '"');
		appendStringInfoString(&buf, colname);
		appendStringInfoString(&buf, "\": ");

		if (colval == NULL)
		{
			appendStringInfoString(&buf, "null");
		}
		else
		{
			/*
			 * For simplicity, emit all values as JSON strings.  A more
			 * sophisticated implementation could inspect the type OID
			 * and emit numbers/booleans without quotes.
			 */
			appendStringInfoChar(&buf, '"');

			/* Escape special JSON characters in the value */
			for (const char *p = colval; *p; p++)
			{
				switch (*p)
				{
					case '"':
						appendStringInfoString(&buf, "\\\"");
						break;
					case '\\':
						appendStringInfoString(&buf, "\\\\");
						break;
					case '\n':
						appendStringInfoString(&buf, "\\n");
						break;
					case '\r':
						appendStringInfoString(&buf, "\\r");
						break;
					case '\t':
						appendStringInfoString(&buf, "\\t");
						break;
					default:
						appendStringInfoChar(&buf, *p);
						break;
				}
			}

			appendStringInfoChar(&buf, '"');
		}
	}

	appendStringInfoChar(&buf, '}');
	return buf.data;
}

/* ----------------------------------------------------------------
 * alohadb_realtime_trigger
 *
 * AFTER ROW trigger function.  Captures the changed row as JSON,
 * inserts an event record, and sends NOTIFY on the subscription
 * channel.
 *
 * The trigger is installed by alohadb_realtime_subscribe() with
 * the subscription_id passed implicitly via the trigger name
 * convention: alohadb_rt_<subscription_id>.
 * ---------------------------------------------------------------- */
Datum
alohadb_realtime_trigger(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	TupleDesc	tupdesc;
	HeapTuple	new_tuple;
	HeapTuple	old_tuple;
	char	   *table_name;
	char		op;
	char	   *new_json = NULL;
	char	   *old_json = NULL;
	char	   *trigger_name;
	int			sub_id;
	StringInfoData insert_sql;
	StringInfoData notify_payload;
	int			ret;
	int64		event_id;

	/* Must be called as a trigger */
	if (!CALLED_AS_TRIGGER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("alohadb_realtime_trigger: must be called as a trigger")));

	/* Must be an AFTER ROW trigger */
	if (!TRIGGER_FIRED_AFTER(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("alohadb_realtime_trigger: must be an AFTER trigger")));

	if (!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("alohadb_realtime_trigger: must be a FOR EACH ROW trigger")));

	tupdesc = trigdata->tg_relation->rd_att;
	table_name = SPI_getrelname(trigdata->tg_relation);

	/*
	 * Extract the subscription ID from the trigger name.
	 * Trigger names follow the pattern: alohadb_rt_<id>
	 */
	trigger_name = trigdata->tg_trigger->tgname;
	if (strncmp(trigger_name, "alohadb_rt_", 11) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_realtime_trigger: unexpected trigger name \"%s\"",
						trigger_name)));
	sub_id = atoi(trigger_name + 11);

	/* Determine operation type and get relevant tuples */
	if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
	{
		op = 'I';
		new_tuple = trigdata->tg_trigtuple;
		old_tuple = NULL;
	}
	else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
	{
		op = 'U';
		new_tuple = trigdata->tg_newtuple;
		old_tuple = trigdata->tg_trigtuple;
	}
	else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
	{
		op = 'D';
		new_tuple = NULL;
		old_tuple = trigdata->tg_trigtuple;
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("alohadb_realtime_trigger: unsupported trigger event")));
		PG_RETURN_NULL();		/* keep compiler quiet */
	}

	/* Convert tuples to JSON */
	if (new_tuple != NULL)
		new_json = tuple_to_json(new_tuple, tupdesc);
	if (old_tuple != NULL)
		old_json = tuple_to_json(old_tuple, tupdesc);

	/* Connect to SPI to insert the event */
	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_realtime_trigger: SPI_connect failed: %s",
						SPI_result_code_string(ret))));

	/*
	 * INSERT into alohadb_realtime_events and retrieve the generated
	 * event_id for the NOTIFY payload.
	 */
	initStringInfo(&insert_sql);
	appendStringInfo(&insert_sql,
					 "INSERT INTO alohadb_realtime_events "
					 "(subscription_id, table_name, operation, row_data, old_row_data) "
					 "VALUES (%d, %s, '%c', ",
					 sub_id,
					 quote_literal_cstr(table_name),
					 op);

	if (new_json != NULL)
		appendStringInfo(&insert_sql, "%s, ", quote_literal_cstr(new_json));
	else
		appendStringInfoString(&insert_sql, "NULL, ");

	if (old_json != NULL)
		appendStringInfo(&insert_sql, "%s", quote_literal_cstr(old_json));
	else
		appendStringInfoString(&insert_sql, "NULL");

	appendStringInfoString(&insert_sql, ") RETURNING event_id");

	ret = SPI_execute(insert_sql.data, false, 0);
	if (ret != SPI_OK_INSERT_RETURNING || SPI_processed != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_realtime_trigger: failed to insert event: %s",
						SPI_result_code_string(ret))));

	/* Retrieve the event_id from the RETURNING clause */
	{
		char   *event_id_str;

		event_id_str = SPI_getvalue(SPI_tuptable->vals[0],
									SPI_tuptable->tupdesc, 1);
		event_id = event_id_str ? pg_strtoint64(event_id_str) : 0;
	}

	SPI_finish();

	/*
	 * Build the NOTIFY payload and send asynchronous notification.
	 *
	 * We look up the channel from the subscriptions table, but for
	 * performance we use the default channel name.  The payload contains
	 * table name, operation, and event_id so consumers can decide whether
	 * to fetch the full event.
	 */
	initStringInfo(&notify_payload);
	appendStringInfo(&notify_payload,
					 "{\"table\":\"%s\",\"op\":\"%c\",\"event_id\":" INT64_FORMAT "}",
					 table_name, op, event_id);

	Async_Notify("alohadb_realtime", notify_payload.data);

	/* insert_sql.data was in SPI context, already freed by SPI_finish */
	pfree(notify_payload.data);
	if (new_json)
		pfree(new_json);
	if (old_json)
		pfree(old_json);
	pfree(table_name);

	/*
	 * Return the appropriate tuple.  For AFTER triggers the return value
	 * is technically ignored, but returning the tuple avoids a "returned
	 * null" error in some PG versions.
	 */
	if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
		return PointerGetDatum(old_tuple);
	else
		return PointerGetDatum(new_tuple);
}

/* ----------------------------------------------------------------
 * alohadb_realtime_subscribe
 *
 * SQL-callable function.  Creates a subscription for the given
 * table and installs an AFTER trigger.
 *
 * Arguments:
 *   target_table  regclass  - the table to watch
 *   filter        text      - optional filter expression (reserved)
 *
 * Returns the subscription ID (int).
 * ---------------------------------------------------------------- */
Datum
alohadb_realtime_subscribe(PG_FUNCTION_ARGS)
{
	Oid			table_oid = PG_GETARG_OID(0);
	char	   *filter_expr = NULL;
	char	   *table_name;
	char	   *schema_name;
	char	   *qualified_name;
	int			sub_id;
	int			ret;
	StringInfoData sql;

	/* Get optional filter expression */
	if (!PG_ARGISNULL(1))
		filter_expr = text_to_cstring(PG_GETARG_TEXT_PP(1));

	/* Validate the table exists by resolving its name */
	table_name = get_rel_name(table_oid);
	if (table_name == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("relation with OID %u does not exist", table_oid)));

	schema_name = get_namespace_name(get_rel_namespace(table_oid));
	if (schema_name == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("schema for relation with OID %u does not exist", table_oid)));

	/* Build qualified table name for use in DDL */
	{
		StringInfoData qn;

		initStringInfo(&qn);
		appendStringInfo(&qn, "%s.%s",
						 quote_identifier(schema_name),
						 quote_identifier(table_name));
		qualified_name = qn.data;
	}

	/* Connect to SPI */
	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_realtime_subscribe: SPI_connect failed: %s",
						SPI_result_code_string(ret))));

	/* Insert the subscription record */
	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "INSERT INTO alohadb_realtime_subscriptions "
					 "(table_name, filter_expr, channel) VALUES "
					 "(%s::regclass, %s, 'alohadb_realtime') RETURNING id",
					 quote_literal_cstr(qualified_name),
					 filter_expr ? quote_literal_cstr(filter_expr) : "NULL");

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_INSERT_RETURNING || SPI_processed != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_realtime_subscribe: failed to insert subscription: %s",
						SPI_result_code_string(ret))));

	/* Get the generated subscription ID */
	{
		bool	isnull;

		sub_id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
											  SPI_tuptable->tupdesc,
											  1, &isnull));
		if (isnull)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("alohadb_realtime_subscribe: subscription ID is NULL")));
	}

	/* Create the AFTER trigger on the target table */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
					 "CREATE TRIGGER alohadb_rt_%d "
					 "AFTER INSERT OR UPDATE OR DELETE ON %s "
					 "FOR EACH ROW EXECUTE FUNCTION alohadb_realtime_trigger()",
					 sub_id,
					 qualified_name);

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_UTILITY)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_realtime_subscribe: failed to create trigger: %s",
						SPI_result_code_string(ret))));

	SPI_finish();

	/* sql.data was in SPI context, already freed by SPI_finish */
	pfree(qualified_name);
	pfree(table_name);
	pfree(schema_name);

	PG_RETURN_INT32(sub_id);
}

/* ----------------------------------------------------------------
 * alohadb_realtime_unsubscribe
 *
 * SQL-callable function.  Removes a subscription and drops the
 * associated trigger.
 *
 * Arguments:
 *   sub_id  int  - the subscription ID to remove
 * ---------------------------------------------------------------- */
Datum
alohadb_realtime_unsubscribe(PG_FUNCTION_ARGS)
{
	int			sub_id = PG_GETARG_INT32(0);
	int			ret;
	StringInfoData sql;
	char	   *table_name_str;

	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_realtime_unsubscribe: SPI_connect failed: %s",
						SPI_result_code_string(ret))));

	/*
	 * Look up the subscription to find the table name so we can drop
	 * the trigger.
	 */
	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT table_name::text FROM alohadb_realtime_subscriptions "
					 "WHERE id = %d",
					 sub_id);

	ret = SPI_execute(sql.data, true, 0);
	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_realtime_unsubscribe: failed to look up subscription: %s",
						SPI_result_code_string(ret))));

	if (SPI_processed == 0)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("alohadb_realtime_unsubscribe: subscription %d does not exist",
						sub_id)));

	table_name_str = SPI_getvalue(SPI_tuptable->vals[0],
								  SPI_tuptable->tupdesc, 1);
	if (table_name_str == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_realtime_unsubscribe: table name is NULL for subscription %d",
						sub_id)));

	/* Copy the table name out of SPI context */
	table_name_str = pstrdup(table_name_str);

	/* Drop the trigger (IF EXISTS for safety) */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
					 "DROP TRIGGER IF EXISTS alohadb_rt_%d ON %s",
					 sub_id,
					 table_name_str);

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_UTILITY)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_realtime_unsubscribe: failed to drop trigger: %s",
						SPI_result_code_string(ret))));

	/* Delete the subscription record */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
					 "DELETE FROM alohadb_realtime_subscriptions WHERE id = %d",
					 sub_id);

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_DELETE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_realtime_unsubscribe: failed to delete subscription: %s",
						SPI_result_code_string(ret))));

	SPI_finish();

	/* sql.data and table_name_str were in SPI context */

	PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * alohadb_realtime_poll
 *
 * SQL-callable set-returning function.  Returns all events with
 * event_id greater than the given cursor value.
 *
 * Arguments:
 *   since_event_id  bigint  - return events after this ID (default 0)
 *
 * Returns SETOF alohadb_realtime_events.
 * ---------------------------------------------------------------- */
Datum
alohadb_realtime_poll(PG_FUNCTION_ARGS)
{
	int64		since_event_id = PG_GETARG_INT64(0);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	StringInfoData sql;
	int			ret;
	uint64		i;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC | MAT_SRF_BLESS);

	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_realtime_poll: SPI_connect failed: %s",
						SPI_result_code_string(ret))));

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * FROM alohadb_realtime_events "
					 "WHERE event_id > " INT64_FORMAT " "
					 "ORDER BY event_id",
					 since_event_id);

	ret = SPI_execute(sql.data, true, 0);
	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_realtime_poll: query failed: %s",
						SPI_result_code_string(ret))));

	/* Copy SPI result rows into the tuplestore */
	for (i = 0; i < SPI_processed; i++)
	{
		HeapTuple	spi_tuple;
		HeapTuple	out_tuple;

		spi_tuple = SPI_tuptable->vals[i];

		/*
		 * SPI tuples may be in SPI memory context, so we need to copy
		 * them into the tuplestore's context.  tuplestore_puttuple
		 * handles this internally.
		 */
		out_tuple = SPI_copytuple(spi_tuple);
		tuplestore_puttuple(rsinfo->setResult, out_tuple);
		heap_freetuple(out_tuple);
	}

	SPI_finish();

	/* sql.data was in SPI context, already freed by SPI_finish */

	return (Datum) 0;
}

/* ----------------------------------------------------------------
 * alohadb_realtime_cleanup
 *
 * SQL-callable function.  Deletes events older than the specified
 * interval and returns the number of rows deleted.
 *
 * Arguments:
 *   older_than  interval  - delete events older than this (default '1 hour')
 *
 * Returns int (count of deleted rows).
 * ---------------------------------------------------------------- */
Datum
alohadb_realtime_cleanup(PG_FUNCTION_ARGS)
{
	Interval   *older_than = PG_GETARG_INTERVAL_P(0);
	int			ret;
	int			deleted;
	StringInfoData sql;
	char	   *interval_str;

	/*
	 * Convert the interval to its text representation for embedding
	 * in the SQL query.
	 */
	interval_str = DatumGetCString(DirectFunctionCall1(interval_out,
													   IntervalPGetDatum(older_than)));

	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_realtime_cleanup: SPI_connect failed: %s",
						SPI_result_code_string(ret))));

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "DELETE FROM alohadb_realtime_events "
					 "WHERE created_at < now() - %s::interval",
					 quote_literal_cstr(interval_str));

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_DELETE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_realtime_cleanup: delete failed: %s",
						SPI_result_code_string(ret))));

	deleted = (int) SPI_processed;

	SPI_finish();

	/* sql.data was in SPI context, already freed by SPI_finish */
	pfree(interval_str);

	PG_RETURN_INT32(deleted);
}
