/*-------------------------------------------------------------------------
 *
 * arrow_ipc.c
 *	  Simplified Arrow IPC stream serialization.
 *
 * This module writes Arrow IPC stream format messages.  The format is
 * a simplified but compatible subset of the full Apache Arrow IPC
 * specification:
 *
 *   - Schema message: describes column names and types
 *   - RecordBatch message: columnar data for a batch of rows
 *
 * Each IPC message has the following layout:
 *
 *   [4 bytes]  continuation marker (0xFFFFFFFF)
 *   [4 bytes]  metadata_length (little-endian int32)
 *   [metadata_length bytes]  metadata (schema or record batch descriptor)
 *   [body bytes]  column data buffers (8-byte aligned), only for RecordBatch
 *
 * The metadata is a simplified flatbuffer-compatible binary descriptor
 * that encodes the schema (field names + types) or the record batch
 * layout (lengths and null counts per column).
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * IDENTIFICATION
 *	  contrib/alohadb_cdc_arrow/arrow_ipc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "lib/stringinfo.h"

#include "arrow_builder.h"

/*
 * Arrow IPC message type tags (used in metadata header).
 */
#define ARROW_MSG_SCHEMA		1
#define ARROW_MSG_RECORD_BATCH	2

/*
 * Arrow type ID values used in our simplified schema encoding.
 * These match the Arrow flatbuffer Type union enumeration.
 */
#define ARROW_FB_TYPE_BOOL			2
#define ARROW_FB_TYPE_INT32			3
#define ARROW_FB_TYPE_INT64			4
#define ARROW_FB_TYPE_FLOAT32		5
#define ARROW_FB_TYPE_FLOAT64		6
#define ARROW_FB_TYPE_UTF8			7
#define ARROW_FB_TYPE_BINARY		8
#define ARROW_FB_TYPE_TIMESTAMP		10
#define ARROW_FB_TYPE_FIXED_SIZE_LIST 16

/* ----------------------------------------------------------------
 * Internal helpers
 * ----------------------------------------------------------------
 */

/*
 * Write a little-endian int32 to the StringInfo.
 */
static void
write_int32(StringInfo buf, int32 val)
{
	appendBinaryStringInfo(buf, (const char *) &val, sizeof(int32));
}

/*
 * Write a little-endian int64 to the StringInfo.
 */
static void
write_int64(StringInfo buf, int64 val)
{
	appendBinaryStringInfo(buf, (const char *) &val, sizeof(int64));
}

/*
 * Write a length-prefixed string (int32 length + bytes, no NUL terminator).
 */
static void
write_string(StringInfo buf, const char *s)
{
	int32		len = (int32) strlen(s);

	write_int32(buf, len);
	appendBinaryStringInfo(buf, s, len);
}

/*
 * Map our ArrowTypeId enum to the flatbuffer type tag.
 */
static int32
arrow_type_to_fb(ArrowTypeId type_id)
{
	switch (type_id)
	{
		case ARROW_TYPE_BOOL:
			return ARROW_FB_TYPE_BOOL;
		case ARROW_TYPE_INT32:
			return ARROW_FB_TYPE_INT32;
		case ARROW_TYPE_INT64:
			return ARROW_FB_TYPE_INT64;
		case ARROW_TYPE_FLOAT32:
			return ARROW_FB_TYPE_FLOAT32;
		case ARROW_TYPE_FLOAT64:
			return ARROW_FB_TYPE_FLOAT64;
		case ARROW_TYPE_UTF8:
			return ARROW_FB_TYPE_UTF8;
		case ARROW_TYPE_BINARY:
			return ARROW_FB_TYPE_BINARY;
		case ARROW_TYPE_TIMESTAMP_USEC:
			return ARROW_FB_TYPE_TIMESTAMP;
		case ARROW_TYPE_FIXED_SIZE_LIST_FLOAT32:
			return ARROW_FB_TYPE_FIXED_SIZE_LIST;
	}
	return ARROW_FB_TYPE_UTF8;		/* fallback */
}

/*
 * Pad the StringInfo to 8-byte alignment by appending zero bytes.
 */
static void
pad_to_8(StringInfo buf)
{
	while (buf->len % 8 != 0)
		appendStringInfoChar(buf, '\0');
}

/* ----------------------------------------------------------------
 * arrow_ipc_write_schema
 *
 * Write the Arrow IPC stream schema message.  This is emitted once
 * at the start of a logical replication session to describe the
 * column layout.
 *
 * Format of our simplified schema metadata:
 *   [4 bytes]  message type = ARROW_MSG_SCHEMA
 *   [4 bytes]  number of fields (ncols)
 *   For each field:
 *     [4 bytes]  name length
 *     [N bytes]  name
 *     [4 bytes]  Arrow type tag
 *     [4 bytes]  type parameter (e.g. list_size for FixedSizeList, 0 otherwise)
 *     [1 byte]   nullable flag (always 1)
 * ----------------------------------------------------------------
 */
void
arrow_ipc_write_schema(StringInfo buf, ArrowBatchBuilder *builder)
{
	StringInfoData metadata;
	int			i;
	int32		metadata_len;

	initStringInfo(&metadata);

	/* Message type */
	write_int32(&metadata, ARROW_MSG_SCHEMA);

	/* Number of fields */
	write_int32(&metadata, builder->ncols);

	for (i = 0; i < builder->ncols; i++)
	{
		ArrowColumnBuffer *cb = &builder->columns[i];

		/* Field name */
		write_string(&metadata, builder->col_names[i]);

		/* Arrow type tag */
		write_int32(&metadata, arrow_type_to_fb(cb->type_id));

		/* Type parameter (list_size for vectors, 0 otherwise) */
		write_int32(&metadata, cb->list_size);

		/* Nullable flag */
		appendStringInfoChar(&metadata, '\1');
	}

	/* Pad metadata to 8-byte alignment */
	while (metadata.len % 8 != 0)
		appendStringInfoChar(&metadata, '\0');

	metadata_len = metadata.len;

	/* Write IPC message envelope */
	/* Continuation marker */
	write_int32(buf, (int32) 0xFFFFFFFF);
	/* Metadata length */
	write_int32(buf, metadata_len);
	/* Metadata */
	appendBinaryStringInfo(buf, metadata.data, metadata_len);
	/* No body for schema messages */

	pfree(metadata.data);
}

/* ----------------------------------------------------------------
 * arrow_ipc_write_batch
 *
 * Write an Arrow IPC RecordBatch message containing the accumulated
 * data from the builder.
 *
 * The builder must have been finalized with arrow_builder_finish()
 * before calling this function.
 *
 * Format of our simplified record batch metadata:
 *   [4 bytes]  message type = ARROW_MSG_RECORD_BATCH
 *   [8 bytes]  row count (int64)
 *   [4 bytes]  number of columns (int32)
 *   For each column:
 *     [4 bytes]  null_count
 *     [4 bytes]  Arrow type tag
 *     [8 bytes]  validity buffer length in body
 *     [8 bytes]  offsets buffer length in body (0 for fixed-width)
 *     [8 bytes]  data buffer length in body
 *
 * Body layout (appended after metadata, 8-byte aligned):
 *   For each column:
 *     [validity bitmap, padded to 8 bytes]
 *     [offsets buffer, padded to 8 bytes] (only for Utf8/Binary)
 *     [data buffer, padded to 8 bytes]
 * ----------------------------------------------------------------
 */
void
arrow_ipc_write_batch(StringInfo buf, ArrowBatchBuilder *builder)
{
	StringInfoData metadata;
	StringInfoData body;
	int			i;
	int32		metadata_len;

	if (builder->nrows == 0)
		return;

	initStringInfo(&metadata);
	initStringInfo(&body);

	/* --- Build metadata --- */

	write_int32(&metadata, ARROW_MSG_RECORD_BATCH);
	write_int64(&metadata, (int64) builder->nrows);
	write_int32(&metadata, builder->ncols);

	/*
	 * First pass: compute buffer lengths per column.
	 * We need to know the padded sizes before writing to body so that
	 * metadata can record the correct lengths.  We build the body in a
	 * second pass.  For simplicity we compute and record the unpadded
	 * lengths in metadata, then write padded buffers to the body.
	 */
	for (i = 0; i < builder->ncols; i++)
	{
		ArrowColumnBuffer *cb = &builder->columns[i];
		int64		validity_len = cb->validity_buf.len;
		int64		offsets_len = 0;
		int64		data_len = cb->data_buf.len;

		if (cb->type_id == ARROW_TYPE_UTF8 || cb->type_id == ARROW_TYPE_BINARY)
			offsets_len = cb->offsets_buf.len;

		write_int32(&metadata, cb->null_count);
		write_int32(&metadata, arrow_type_to_fb(cb->type_id));
		write_int64(&metadata, validity_len);
		write_int64(&metadata, offsets_len);
		write_int64(&metadata, data_len);
	}

	/* Pad metadata to 8-byte alignment */
	while (metadata.len % 8 != 0)
		appendStringInfoChar(&metadata, '\0');

	metadata_len = metadata.len;

	/* --- Build body --- */

	for (i = 0; i < builder->ncols; i++)
	{
		ArrowColumnBuffer *cb = &builder->columns[i];

		/* Validity bitmap */
		appendBinaryStringInfo(&body, cb->validity_buf.data,
							   cb->validity_buf.len);
		pad_to_8(&body);

		/* Offsets buffer (variable-length types only) */
		if (cb->type_id == ARROW_TYPE_UTF8 || cb->type_id == ARROW_TYPE_BINARY)
		{
			appendBinaryStringInfo(&body, cb->offsets_buf.data,
								   cb->offsets_buf.len);
			pad_to_8(&body);
		}

		/* Data buffer */
		appendBinaryStringInfo(&body, cb->data_buf.data, cb->data_buf.len);
		pad_to_8(&body);
	}

	/* --- Write IPC message envelope --- */

	/* Continuation marker */
	write_int32(buf, (int32) 0xFFFFFFFF);
	/* Metadata length */
	write_int32(buf, metadata_len);
	/* Metadata bytes */
	appendBinaryStringInfo(buf, metadata.data, metadata_len);
	/* Body bytes */
	appendBinaryStringInfo(buf, body.data, body.len);

	pfree(metadata.data);
	pfree(body.data);
}
