/*-------------------------------------------------------------------------
 *
 * arrow_builder.h
 *	  Arrow record batch builder for CDC output plugin.
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * contrib/alohadb_cdc_arrow/arrow_builder.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ARROW_BUILDER_H
#define ARROW_BUILDER_H

#include "postgres.h"
#include "access/htup_details.h"
#include "access/tupdesc.h"
#include "lib/stringinfo.h"

/*
 * Arrow type enumeration - subset of Arrow types we support for CDC.
 */
typedef enum ArrowTypeId
{
	ARROW_TYPE_BOOL = 0,
	ARROW_TYPE_INT32,
	ARROW_TYPE_INT64,
	ARROW_TYPE_FLOAT32,
	ARROW_TYPE_FLOAT64,
	ARROW_TYPE_UTF8,
	ARROW_TYPE_BINARY,
	ARROW_TYPE_TIMESTAMP_USEC,
	ARROW_TYPE_FIXED_SIZE_LIST_FLOAT32	/* for vector type */
} ArrowTypeId;

/*
 * Per-column buffer used during batch accumulation.
 *
 * For fixed-width types (Bool, Int32, Int64, Float32, Float64, Timestamp):
 *   - data_buf holds the raw values (packed bools for BOOL)
 *   - offsets_buf is unused
 *
 * For variable-width types (Utf8, Binary):
 *   - offsets_buf holds int32 offsets (length = row_count + 1)
 *   - data_buf holds the concatenated variable-length data
 *
 * For FixedSizeList(Float32) (vector):
 *   - data_buf holds float32 values (dim floats per row)
 *   - list_size stores the fixed dimension count
 *
 * validity_buf is a bitmask: bit N is set if row N is non-null.
 */
typedef struct ArrowColumnBuffer
{
	ArrowTypeId type_id;
	StringInfoData validity_buf;	/* null bitmap (1 bit per row) */
	StringInfoData data_buf;		/* value data */
	StringInfoData offsets_buf;		/* offsets for variable-length types */
	int			list_size;			/* dimension for FixedSizeList */
	int			null_count;			/* number of nulls in this column */
} ArrowColumnBuffer;

/*
 * Arrow record batch builder.
 *
 * Accumulates rows in column-oriented buffers, then serializes to
 * Arrow IPC record batch format.
 */
typedef struct ArrowBatchBuilder
{
	MemoryContext mcxt;				/* memory context for allocations */
	int			ncols;				/* number of columns */
	int			nrows;				/* current row count */
	ArrowColumnBuffer *columns;		/* per-column buffers */
	char	  **col_names;			/* column names (copied from TupleDesc) */
	Oid		   *col_type_oids;		/* PG type OIDs for each column */
} ArrowBatchBuilder;

/* arrow_builder.c functions */
extern ArrowBatchBuilder *arrow_builder_init(TupleDesc tupdesc,
											 MemoryContext mcxt);
extern void arrow_builder_add_row(ArrowBatchBuilder *builder,
								  HeapTuple tuple,
								  TupleDesc tupdesc);
extern void arrow_builder_finish(ArrowBatchBuilder *builder);
extern void arrow_builder_reset(ArrowBatchBuilder *builder);
extern void arrow_builder_free(ArrowBatchBuilder *builder);

/* arrow_ipc.c functions */
extern void arrow_ipc_write_schema(StringInfo buf,
								   ArrowBatchBuilder *builder);
extern void arrow_ipc_write_batch(StringInfo buf,
								  ArrowBatchBuilder *builder);

#endif							/* ARROW_BUILDER_H */
