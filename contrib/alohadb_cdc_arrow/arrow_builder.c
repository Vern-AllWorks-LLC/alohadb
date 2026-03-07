/*-------------------------------------------------------------------------
 *
 * arrow_builder.c
 *	  Arrow record batch builder - accumulates rows in columnar format.
 *
 * This module provides column-oriented buffering of PostgreSQL tuples
 * for subsequent serialization into Arrow IPC format.  Each column
 * maintains a validity bitmap and a data buffer (plus offsets for
 * variable-length types).
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * IDENTIFICATION
 *	  contrib/alohadb_cdc_arrow/arrow_builder.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
#include "utils/vector.h"

#include "arrow_builder.h"

/*
 * Well-known OIDs.  These come from pg_type_d.h (generated at build time)
 * but we reference them via the catalog/pg_type.h include which transitively
 * pulls in pg_type_d.h.  The vector type OID is defined in pg_type.dat as
 * 6000; if the generated header does not yet provide a macro we define a
 * fallback here.
 */
#ifndef VECTOROID
#define VECTOROID 6000
#endif

/* ----------------------------------------------------------------
 * Helper: map a PG type OID to our Arrow type enum.
 * ----------------------------------------------------------------
 */
static ArrowTypeId
pg_type_to_arrow(Oid typid)
{
	switch (typid)
	{
		case BOOLOID:
			return ARROW_TYPE_BOOL;
		case INT4OID:
			return ARROW_TYPE_INT32;
		case INT8OID:
			return ARROW_TYPE_INT64;
		case FLOAT4OID:
			return ARROW_TYPE_FLOAT32;
		case FLOAT8OID:
			return ARROW_TYPE_FLOAT64;
		case TEXTOID:
		case JSONBOID:
			return ARROW_TYPE_UTF8;
		case BYTEAOID:
			return ARROW_TYPE_BINARY;
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			return ARROW_TYPE_TIMESTAMP_USEC;
		case VECTOROID:
			return ARROW_TYPE_FIXED_SIZE_LIST_FLOAT32;
		default:
			/* Fall back to UTF8 (text representation) for unknown types */
			return ARROW_TYPE_UTF8;
	}
}

/* ----------------------------------------------------------------
 * Validity bitmap helpers
 * ----------------------------------------------------------------
 */

/*
 * Ensure the validity bitmap has room for at least `nrows` bits.
 * Bits default to 0 (null).
 */
static void
ensure_validity_capacity(StringInfo buf, int nrows)
{
	int			needed_bytes = (nrows + 7) / 8;

	while (buf->len < needed_bytes)
		appendStringInfoChar(buf, '\0');
}

/*
 * Set bit `row` in the validity bitmap (mark as non-null).
 */
static void
set_validity_bit(StringInfo buf, int row)
{
	int			byte_idx = row / 8;
	int			bit_idx = row % 8;

	ensure_validity_capacity(buf, row + 1);
	buf->data[byte_idx] |= (1 << bit_idx);
}

/* ----------------------------------------------------------------
 * arrow_builder_init
 *
 * Create a new ArrowBatchBuilder from a PostgreSQL TupleDesc.
 * Skips dropped columns.  The builder is allocated in the given
 * memory context.
 * ----------------------------------------------------------------
 */
ArrowBatchBuilder *
arrow_builder_init(TupleDesc tupdesc, MemoryContext mcxt)
{
	MemoryContext oldcxt;
	ArrowBatchBuilder *builder;
	int			natts = tupdesc->natts;
	int			ncols;
	int			i;
	int			col;

	oldcxt = MemoryContextSwitchTo(mcxt);

	builder = palloc0(sizeof(ArrowBatchBuilder));
	builder->mcxt = mcxt;
	builder->nrows = 0;

	/* Count non-dropped columns */
	ncols = 0;
	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (attr->attisdropped)
			continue;
		if (attr->attnum < 0)
			continue;
		ncols++;
	}

	builder->ncols = ncols;
	builder->columns = palloc0(sizeof(ArrowColumnBuffer) * ncols);
	builder->col_names = palloc0(sizeof(char *) * ncols);
	builder->col_type_oids = palloc0(sizeof(Oid) * ncols);

	col = 0;
	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		ArrowColumnBuffer *cb;

		if (attr->attisdropped)
			continue;
		if (attr->attnum < 0)
			continue;

		cb = &builder->columns[col];
		cb->type_id = pg_type_to_arrow(attr->atttypid);
		cb->null_count = 0;
		cb->list_size = 0;

		initStringInfo(&cb->validity_buf);
		initStringInfo(&cb->data_buf);
		initStringInfo(&cb->offsets_buf);

		/* For variable-length types, write initial offset 0 */
		if (cb->type_id == ARROW_TYPE_UTF8 || cb->type_id == ARROW_TYPE_BINARY)
		{
			int32		zero = 0;

			appendBinaryStringInfo(&cb->offsets_buf, (const char *) &zero,
								   sizeof(int32));
		}

		builder->col_names[col] = pstrdup(NameStr(attr->attname));
		builder->col_type_oids[col] = attr->atttypid;

		col++;
	}

	MemoryContextSwitchTo(oldcxt);

	return builder;
}

/* ----------------------------------------------------------------
 * arrow_builder_add_row
 *
 * Append a single HeapTuple to the columnar buffers.
 * ----------------------------------------------------------------
 */
void
arrow_builder_add_row(ArrowBatchBuilder *builder, HeapTuple tuple,
					  TupleDesc tupdesc)
{
	MemoryContext oldcxt;
	int			natts = tupdesc->natts;
	int			col = 0;
	int			row = builder->nrows;
	int			i;

	oldcxt = MemoryContextSwitchTo(builder->mcxt);

	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		ArrowColumnBuffer *cb;
		Datum		val;
		bool		isnull;

		if (attr->attisdropped)
			continue;
		if (attr->attnum < 0)
			continue;

		cb = &builder->columns[col];

		val = heap_getattr(tuple, i + 1, tupdesc, &isnull);

		if (isnull)
		{
			cb->null_count++;

			/* Ensure validity bitmap has room; bit stays 0 (null) */
			ensure_validity_capacity(&cb->validity_buf, row + 1);

			/* Write placeholder data so column offsets stay aligned */
			switch (cb->type_id)
			{
				case ARROW_TYPE_BOOL:
					/* Bools are packed into bits; no extra byte needed */
					ensure_validity_capacity(&cb->data_buf, row + 1);
					break;
				case ARROW_TYPE_INT32:
				case ARROW_TYPE_FLOAT32:
					{
						int32		zero = 0;

						appendBinaryStringInfo(&cb->data_buf,
											   (const char *) &zero,
											   sizeof(int32));
					}
					break;
				case ARROW_TYPE_INT64:
				case ARROW_TYPE_FLOAT64:
				case ARROW_TYPE_TIMESTAMP_USEC:
					{
						int64		zero = 0;

						appendBinaryStringInfo(&cb->data_buf,
											   (const char *) &zero,
											   sizeof(int64));
					}
					break;
				case ARROW_TYPE_UTF8:
				case ARROW_TYPE_BINARY:
					{
						/* Repeat the last offset (zero-length entry) */
						int32		last_off;

						memcpy(&last_off,
							   cb->offsets_buf.data + cb->offsets_buf.len - sizeof(int32),
							   sizeof(int32));
						appendBinaryStringInfo(&cb->offsets_buf,
											   (const char *) &last_off,
											   sizeof(int32));
					}
					break;
				case ARROW_TYPE_FIXED_SIZE_LIST_FLOAT32:
					{
						/* Write dim zero floats */
						int			d;

						for (d = 0; d < cb->list_size; d++)
						{
							float		zero = 0.0f;

							appendBinaryStringInfo(&cb->data_buf,
												   (const char *) &zero,
												   sizeof(float));
						}
					}
					break;
			}

			col++;
			continue;
		}

		/* Non-null value: set validity bit */
		set_validity_bit(&cb->validity_buf, row);

		switch (cb->type_id)
		{
			case ARROW_TYPE_BOOL:
				{
					bool		bval = DatumGetBool(val);
					int			byte_idx = row / 8;
					int			bit_idx = row % 8;

					/* Ensure data bitmap has room */
					while (cb->data_buf.len <= byte_idx)
						appendStringInfoChar(&cb->data_buf, '\0');

					if (bval)
						cb->data_buf.data[byte_idx] |= (1 << bit_idx);
				}
				break;

			case ARROW_TYPE_INT32:
				{
					int32		ival = DatumGetInt32(val);

					appendBinaryStringInfo(&cb->data_buf,
										   (const char *) &ival,
										   sizeof(int32));
				}
				break;

			case ARROW_TYPE_INT64:
				{
					int64		ival = DatumGetInt64(val);

					appendBinaryStringInfo(&cb->data_buf,
										   (const char *) &ival,
										   sizeof(int64));
				}
				break;

			case ARROW_TYPE_FLOAT32:
				{
					float4		fval = DatumGetFloat4(val);

					appendBinaryStringInfo(&cb->data_buf,
										   (const char *) &fval,
										   sizeof(float4));
				}
				break;

			case ARROW_TYPE_FLOAT64:
				{
					float8		dval = DatumGetFloat8(val);

					appendBinaryStringInfo(&cb->data_buf,
										   (const char *) &dval,
										   sizeof(float8));
				}
				break;

			case ARROW_TYPE_UTF8:
				{
					Oid			typoutput;
					bool		typisvarlena;
					char	   *text_val;
					int32		len;
					int32		new_off;

					getTypeOutputInfo(attr->atttypid, &typoutput,
									  &typisvarlena);

					if (typisvarlena)
					{
						Datum		detoasted = PointerGetDatum(
														PG_DETOAST_DATUM(val));

						text_val = OidOutputFunctionCall(typoutput, detoasted);
					}
					else
						text_val = OidOutputFunctionCall(typoutput, val);

					len = strlen(text_val);
					appendBinaryStringInfo(&cb->data_buf, text_val, len);

					/* Write the new offset */
					new_off = cb->data_buf.len;
					appendBinaryStringInfo(&cb->offsets_buf,
										   (const char *) &new_off,
										   sizeof(int32));
				}
				break;

			case ARROW_TYPE_BINARY:
				{
					bytea	   *bval_raw = DatumGetByteaP(val);
					int32		len = VARSIZE_ANY_EXHDR(bval_raw);
					int32		new_off;

					appendBinaryStringInfo(&cb->data_buf,
										   VARDATA_ANY(bval_raw), len);

					new_off = cb->data_buf.len;
					appendBinaryStringInfo(&cb->offsets_buf,
										   (const char *) &new_off,
										   sizeof(int32));
				}
				break;

			case ARROW_TYPE_TIMESTAMP_USEC:
				{
					/*
					 * PostgreSQL stores timestamps as int64 microseconds
					 * since 2000-01-01.  Arrow Timestamp(us) uses
					 * microseconds since 1970-01-01.  We need to convert
					 * by adding the epoch difference.
					 */
					Timestamp	ts = DatumGetTimestamp(val);
					int64		arrow_us;

					/* POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE = 10957 days */
#define PG_EPOCH_OFFSET_USEC INT64CONST(946684800000000)
					arrow_us = ts + PG_EPOCH_OFFSET_USEC;

					appendBinaryStringInfo(&cb->data_buf,
										   (const char *) &arrow_us,
										   sizeof(int64));
				}
				break;

			case ARROW_TYPE_FIXED_SIZE_LIST_FLOAT32:
				{
					Vector	   *vec = DatumGetVector(val);
					int			dim = vec->dim;
					int			d;

					/*
					 * On first non-null vector, record the dimension.
					 * Subsequent vectors must match.
					 */
					if (cb->list_size == 0)
						cb->list_size = dim;
					else if (cb->list_size != dim)
						ereport(ERROR,
								(errcode(ERRCODE_DATA_EXCEPTION),
								 errmsg("vector dimension mismatch: expected %d, got %d",
										cb->list_size, dim)));

					for (d = 0; d < dim; d++)
					{
						float		fv = vec->x[d];

						appendBinaryStringInfo(&cb->data_buf,
											   (const char *) &fv,
											   sizeof(float));
					}
				}
				break;
		}

		col++;
	}

	builder->nrows++;

	MemoryContextSwitchTo(oldcxt);
}

/* ----------------------------------------------------------------
 * arrow_builder_finish
 *
 * Finalize the current batch.  Pad validity bitmaps to the correct
 * number of bytes for the row count.  After this call the builder
 * is ready for serialization by arrow_ipc_write_batch().
 * ----------------------------------------------------------------
 */
void
arrow_builder_finish(ArrowBatchBuilder *builder)
{
	int			i;

	for (i = 0; i < builder->ncols; i++)
	{
		ArrowColumnBuffer *cb = &builder->columns[i];

		/* Ensure validity bitmaps are the right length */
		ensure_validity_capacity(&cb->validity_buf, builder->nrows);

		/* For bool data buffer, ensure correct bit-packed length */
		if (cb->type_id == ARROW_TYPE_BOOL)
		{
			int			needed = (builder->nrows + 7) / 8;

			while (cb->data_buf.len < needed)
				appendStringInfoChar(&cb->data_buf, '\0');
		}
	}
}

/* ----------------------------------------------------------------
 * arrow_builder_reset
 *
 * Clear the builder for a new batch, keeping the column structure.
 * ----------------------------------------------------------------
 */
void
arrow_builder_reset(ArrowBatchBuilder *builder)
{
	int			i;

	builder->nrows = 0;

	for (i = 0; i < builder->ncols; i++)
	{
		ArrowColumnBuffer *cb = &builder->columns[i];

		resetStringInfo(&cb->validity_buf);
		resetStringInfo(&cb->data_buf);
		resetStringInfo(&cb->offsets_buf);
		cb->null_count = 0;

		/* Re-initialize initial offset for variable-length types */
		if (cb->type_id == ARROW_TYPE_UTF8 || cb->type_id == ARROW_TYPE_BINARY)
		{
			int32		zero = 0;

			appendBinaryStringInfo(&cb->offsets_buf, (const char *) &zero,
								   sizeof(int32));
		}

		/* Reset list_size so the next batch picks it up from the first row */
		if (cb->type_id == ARROW_TYPE_FIXED_SIZE_LIST_FLOAT32)
			cb->list_size = 0;
	}
}

/* ----------------------------------------------------------------
 * arrow_builder_free
 *
 * Release all memory owned by the builder.
 * ----------------------------------------------------------------
 */
void
arrow_builder_free(ArrowBatchBuilder *builder)
{
	int			i;

	for (i = 0; i < builder->ncols; i++)
	{
		pfree(builder->columns[i].validity_buf.data);
		pfree(builder->columns[i].data_buf.data);
		pfree(builder->columns[i].offsets_buf.data);
		pfree(builder->col_names[i]);
	}

	pfree(builder->columns);
	pfree(builder->col_names);
	pfree(builder->col_type_oids);
	pfree(builder);
}
