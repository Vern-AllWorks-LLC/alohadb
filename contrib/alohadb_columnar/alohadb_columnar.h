/*-------------------------------------------------------------------------
 *
 * alohadb_columnar.h
 *	  Columnar storage table access method with zstd compression.
 *
 * Provides an append-only columnar storage engine for analytical workloads.
 * Data is organized in stripes of rows, where each stripe stores column
 * values contiguously and applies zstd compression.
 *
 * Patent note: Based on C-Store (BSD 2005), Hydra columnar (Apache 2.0).
 * Column-oriented storage is a well-established academic concept from the
 * 1970s.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_columnar/alohadb_columnar.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ALOHADB_COLUMNAR_H
#define ALOHADB_COLUMNAR_H

#include "postgres.h"
#include "access/tableam.h"
#include "storage/smgr.h"
#include "utils/relcache.h"
#include "utils/memutils.h"

/*
 * Use smgrnblocks() directly to get the number of blocks.
 * We MUST NOT use RelationGetNumberOfBlocks() or
 * RelationGetNumberOfBlocksInFork() because those call
 * table_relation_size() for table AMs, which calls our
 * columnar_relation_size callback, causing infinite recursion.
 */
#define ColumnarGetNblocks(rel) \
	smgrnblocks(RelationGetSmgr(rel), MAIN_FORKNUM)

/* Magic number to identify columnar metadata pages */
#define COLUMNAR_MAGIC			0x434F4C4D	/* "COLM" */
#define COLUMNAR_VERSION		1
#define COLUMNAR_DEFAULT_STRIPE_ROWS	150000

/* Block 0 is the metadata page */
#define COLUMNAR_META_BLOCKNO	0

/*
 * Maximum number of stripes we can track in the metadata area.
 * Metadata starts at block 0; stripe directory entries are stored
 * sequentially within metadata blocks following the meta header.
 */
#define COLUMNAR_MAX_STRIPES_PER_METABLOCK \
	((BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - \
	  MAXALIGN(sizeof(ColumnarMetaPageData))) / sizeof(ColumnarStripeInfo))

/*
 * Metadata stored in the special area of block 0.
 * This header describes the overall state of the columnar relation.
 */
typedef struct ColumnarMetaPageData
{
	uint32		magic;				/* COLUMNAR_MAGIC */
	uint32		version;			/* COLUMNAR_VERSION */
	int64		total_rows;			/* total number of rows across all stripes */
	int32		num_stripes;		/* number of stripes written */
	int32		natts;				/* number of columns */
	BlockNumber data_start_block;	/* first data block after metadata */
} ColumnarMetaPageData;

/*
 * Per-stripe descriptor.  Stored sequentially in metadata blocks after
 * the ColumnarMetaPageData header on block 0.
 */
typedef struct ColumnarStripeInfo
{
	int64		first_row;			/* logical row number of first row */
	int32		row_count;			/* number of rows in this stripe */
	int64		compressed_size;	/* total compressed bytes for all columns */
	int64		uncompressed_size;	/* total uncompressed bytes */
	BlockNumber start_block;		/* first data block for this stripe */
	int32		num_blocks;			/* total 8K blocks used by this stripe */
} ColumnarStripeInfo;

/*
 * Write state: buffers inserts until a full stripe is accumulated, then
 * flushes (serializes + compresses + writes blocks).
 */
typedef struct ColumnarWriteState
{
	MemoryContext mcxt;				/* per-write-state memory context */
	TupleDesc	tupdesc;			/* tuple descriptor (borrowed reference) */
	int			natts;				/* number of attributes */
	int			buffered_rows;		/* rows currently buffered */
	int			max_rows;			/* stripe size limit */
	Datum	  **col_values;			/* [natts][max_rows] column value arrays */
	bool	  **col_nulls;			/* [natts][max_rows] null bitmap arrays */
} ColumnarWriteState;

/*
 * Scan descriptor: extends the base TableScanDescData with state for
 * reading stripes and decompressing column data.
 */
typedef struct ColumnarScanDescData
{
	TableScanDescData base;			/* must be first */

	int			current_stripe;		/* index of current stripe being read */
	int			current_row;		/* row position within current stripe */
	int			stripe_rows;		/* number of rows in current stripe */

	/* Current row values for returning to executor */
	Datum	   *row_values;
	bool	   *row_nulls;

	/* Full decompressed stripe data */
	Datum	  **stripe_values;		/* [natts][stripe_rows] */
	bool	  **stripe_nulls;		/* [natts][stripe_rows] */
	MemoryContext stripe_mcxt;		/* memory context for stripe data */
} ColumnarScanDescData;

typedef ColumnarScanDescData *ColumnarScanDesc;

/* columnar_storage.c: metadata I/O */
extern void columnar_write_metadata(Relation rel, ColumnarMetaPageData *meta);
extern void columnar_read_metadata(Relation rel, ColumnarMetaPageData *meta);

/* columnar_storage.c: stripe I/O */
extern void columnar_flush_stripe(Relation rel, ColumnarWriteState *wstate);
extern void columnar_read_stripe(Relation rel, ColumnarScanDesc scan, int stripe_idx);

/* columnar_storage.c: stripe info */
extern void columnar_read_stripe_info(Relation rel, int stripe_idx,
									  ColumnarStripeInfo *sinfo);

/* GUC: stripe row count */
extern int	columnar_stripe_rows;

#endif							/* ALOHADB_COLUMNAR_H */
