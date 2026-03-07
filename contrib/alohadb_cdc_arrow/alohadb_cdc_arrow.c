/*-------------------------------------------------------------------------
 *
 * alohadb_cdc_arrow.c
 *	  CDC logical decoding output plugin emitting Arrow IPC format.
 *
 * This output plugin converts PostgreSQL logical replication change
 * events into Apache Arrow IPC stream messages.  Each transaction's
 * changes are accumulated in a columnar ArrowBatchBuilder and then
 * serialized as binary Arrow IPC RecordBatch messages on commit.
 *
 * The plugin maps common PostgreSQL types to Arrow types:
 *   int4       -> Int32
 *   int8       -> Int64
 *   float4     -> Float32
 *   float8     -> Float64
 *   text       -> Utf8
 *   bool       -> Bool
 *   timestamp  -> Timestamp(us)
 *   jsonb      -> Utf8
 *   bytea      -> Binary
 *   vector     -> FixedSizeList(Float32)
 *
 * Plugin options:
 *   batch_size  - max rows per batch before flushing (default 1000)
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * IDENTIFICATION
 *	  contrib/alohadb_cdc_arrow/alohadb_cdc_arrow.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_type.h"

#include "replication/logical.h"
#include "replication/origin.h"

#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "arrow_builder.h"

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_cdc_arrow",
					.version = PG_VERSION
);

/* Default maximum rows per Arrow batch */
#define DEFAULT_BATCH_SIZE	1000

/*
 * Plugin-level private data, stored in ctx->output_plugin_private.
 */
typedef struct CdcArrowData
{
	MemoryContext context;			/* working memory context */
	int			batch_size;			/* max rows per batch */
	bool		schema_sent;		/* have we emitted the schema message? */
} CdcArrowData;

/*
 * Per-transaction private data, stored in txn->output_plugin_private.
 */
typedef struct CdcArrowTxnData
{
	ArrowBatchBuilder *builder;		/* columnar batch being accumulated */
	TupleDesc	last_tupdesc;		/* TupleDesc used for current builder */
	Oid			last_relid;			/* relation OID of current builder */
	bool		wrote_changes;		/* did this txn emit any changes? */
} CdcArrowTxnData;

/* Forward declarations of callbacks */
static void cdc_arrow_startup(LogicalDecodingContext *ctx,
							  OutputPluginOptions *opt, bool is_init);
static void cdc_arrow_shutdown(LogicalDecodingContext *ctx);
static void cdc_arrow_begin_txn(LogicalDecodingContext *ctx,
								ReorderBufferTXN *txn);
static void cdc_arrow_commit_txn(LogicalDecodingContext *ctx,
								 ReorderBufferTXN *txn,
								 XLogRecPtr commit_lsn);
static void cdc_arrow_change(LogicalDecodingContext *ctx,
							 ReorderBufferTXN *txn,
							 Relation relation,
							 ReorderBufferChange *change);

/* Internal helpers */
static void flush_batch(LogicalDecodingContext *ctx, CdcArrowTxnData *txndata,
						bool last_write);
static void maybe_send_schema(LogicalDecodingContext *ctx,
							  ArrowBatchBuilder *builder);

/* ----------------------------------------------------------------
 * Module initialization
 * ----------------------------------------------------------------
 */
void
_PG_init(void)
{
	/* nothing to do */
}

/*
 * Register output plugin callbacks.
 */
void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	cb->startup_cb = cdc_arrow_startup;
	cb->begin_cb = cdc_arrow_begin_txn;
	cb->change_cb = cdc_arrow_change;
	cb->commit_cb = cdc_arrow_commit_txn;
	cb->shutdown_cb = cdc_arrow_shutdown;
}

/* ----------------------------------------------------------------
 * startup_cb
 *
 * Called once when the replication slot is created or a streaming
 * session begins.  We parse plugin options and set binary output mode.
 * ----------------------------------------------------------------
 */
static void
cdc_arrow_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
				  bool is_init)
{
	ListCell   *option;
	CdcArrowData *data;

	data = palloc0(sizeof(CdcArrowData));
	data->context = AllocSetContextCreate(ctx->context,
										  "CDC Arrow context",
										  ALLOCSET_DEFAULT_SIZES);
	data->batch_size = DEFAULT_BATCH_SIZE;
	data->schema_sent = false;

	ctx->output_plugin_private = data;

	/* We produce binary Arrow IPC output */
	opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;
	opt->receive_rewrites = false;

	/* Parse plugin options */
	foreach(option, ctx->output_plugin_options)
	{
		DefElem    *elem = lfirst(option);

		Assert(elem->arg == NULL || IsA(elem->arg, String));

		if (strcmp(elem->defname, "batch_size") == 0 ||
			strcmp(elem->defname, "batch-size") == 0)
		{
			if (elem->arg == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("option \"%s\" requires a value",
								elem->defname)));

			data->batch_size = atoi(strVal(elem->arg));
			if (data->batch_size < 1)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid value for option \"%s\": \"%s\"",
								elem->defname, strVal(elem->arg))));
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("option \"%s\" = \"%s\" is unknown",
							elem->defname,
							elem->arg ? strVal(elem->arg) : "(null)")));
		}
	}
}

/* ----------------------------------------------------------------
 * shutdown_cb
 *
 * Clean up plugin resources.
 * ----------------------------------------------------------------
 */
static void
cdc_arrow_shutdown(LogicalDecodingContext *ctx)
{
	CdcArrowData *data = ctx->output_plugin_private;

	if (data && data->context)
		MemoryContextDelete(data->context);
}

/* ----------------------------------------------------------------
 * begin_cb
 *
 * Start of a new transaction.  We allocate per-txn data but defer
 * builder creation until we know the relation schema.
 * ----------------------------------------------------------------
 */
static void
cdc_arrow_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	CdcArrowTxnData *txndata;

	txndata = MemoryContextAllocZero(ctx->context, sizeof(CdcArrowTxnData));
	txndata->builder = NULL;
	txndata->last_tupdesc = NULL;
	txndata->last_relid = InvalidOid;
	txndata->wrote_changes = false;

	txn->output_plugin_private = txndata;
}

/* ----------------------------------------------------------------
 * commit_cb
 *
 * Flush any remaining rows in the batch and clean up.
 * ----------------------------------------------------------------
 */
static void
cdc_arrow_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
					 XLogRecPtr commit_lsn)
{
	CdcArrowTxnData *txndata = txn->output_plugin_private;

	if (txndata == NULL)
		return;

	/* Flush any pending rows */
	if (txndata->builder && txndata->builder->nrows > 0)
		flush_batch(ctx, txndata, true);

	/* Free builder */
	if (txndata->builder)
	{
		arrow_builder_free(txndata->builder);
		txndata->builder = NULL;
	}

	pfree(txndata);
	txn->output_plugin_private = NULL;
}

/* ----------------------------------------------------------------
 * change_cb
 *
 * Process a single INSERT, UPDATE, or DELETE change.  We pick the
 * appropriate tuple (new for INSERT/UPDATE, old for DELETE) and
 * append it to the columnar batch.  When the batch reaches
 * batch_size, we flush it as an Arrow IPC RecordBatch message.
 *
 * If the relation changes mid-transaction, we flush the current
 * batch and start a new one with the new schema.
 * ----------------------------------------------------------------
 */
static void
cdc_arrow_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				 Relation relation, ReorderBufferChange *change)
{
	CdcArrowData *data = ctx->output_plugin_private;
	CdcArrowTxnData *txndata = txn->output_plugin_private;
	TupleDesc	tupdesc;
	HeapTuple	tuple;
	MemoryContext old;
	Oid			relid;

	tupdesc = RelationGetDescr(relation);
	relid = RelationGetRelid(relation);

	/* Determine which tuple to process based on the change type */
	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			tuple = change->data.tp.newtuple;
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			tuple = change->data.tp.newtuple;
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			tuple = change->data.tp.oldtuple;
			break;
		default:
			return;				/* ignore other change types */
	}

	if (tuple == NULL)
		return;					/* no tuple data available */

	old = MemoryContextSwitchTo(data->context);

	/*
	 * If the relation has changed since the last row, flush the current
	 * batch and create a new builder for the new schema.
	 */
	if (txndata->builder != NULL && txndata->last_relid != relid)
	{
		if (txndata->builder->nrows > 0)
			flush_batch(ctx, txndata, false);

		arrow_builder_free(txndata->builder);
		txndata->builder = NULL;
		data->schema_sent = false;
	}

	/* Create builder on first use or after schema change */
	if (txndata->builder == NULL)
	{
		txndata->builder = arrow_builder_init(tupdesc, data->context);
		txndata->last_tupdesc = tupdesc;
		txndata->last_relid = relid;
		data->schema_sent = false;
	}

	/* Accumulate the row */
	arrow_builder_add_row(txndata->builder, tuple, tupdesc);
	txndata->wrote_changes = true;

	/* Flush if we hit the batch size limit */
	if (txndata->builder->nrows >= data->batch_size)
		flush_batch(ctx, txndata, false);

	MemoryContextSwitchTo(old);
}

/* ----------------------------------------------------------------
 * flush_batch
 *
 * Finalize the current batch and write it as an Arrow IPC message
 * via OutputPluginWrite.  If the schema has not been sent yet,
 * we prepend a schema message.
 * ----------------------------------------------------------------
 */
static void
flush_batch(LogicalDecodingContext *ctx, CdcArrowTxnData *txndata,
			bool last_write)
{
	CdcArrowData *data = ctx->output_plugin_private;
	ArrowBatchBuilder *builder = txndata->builder;

	if (builder == NULL || builder->nrows == 0)
		return;

	/* Finalize the batch (pad bitmaps etc.) */
	arrow_builder_finish(builder);

	OutputPluginPrepareWrite(ctx, last_write);

	/* Reset output buffer */
	resetStringInfo(ctx->out);

	/* Send schema if this is the first batch */
	maybe_send_schema(ctx, builder);

	/* Serialize the RecordBatch */
	arrow_ipc_write_batch(ctx->out, builder);

	OutputPluginWrite(ctx, last_write);

	/* Reset builder for next batch */
	arrow_builder_reset(builder);
}

/* ----------------------------------------------------------------
 * maybe_send_schema
 *
 * Emit the Arrow IPC schema message once per relation per session.
 * The schema is prepended to the same output buffer as the first
 * RecordBatch message.
 * ----------------------------------------------------------------
 */
static void
maybe_send_schema(LogicalDecodingContext *ctx, ArrowBatchBuilder *builder)
{
	CdcArrowData *data = ctx->output_plugin_private;

	if (!data->schema_sent)
	{
		arrow_ipc_write_schema(ctx->out, builder);
		data->schema_sent = true;
	}
}
