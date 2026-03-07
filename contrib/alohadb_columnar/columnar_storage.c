/*-------------------------------------------------------------------------
 *
 * columnar_storage.c
 *	  Stripe I/O for the columnar storage table access method.
 *
 *	  Handles serialization/deserialization of column data into stripes,
 *	  zstd compression/decompression, and block-level I/O using the
 *	  PostgreSQL buffer manager and GenericXLog.
 *
 *	  Storage layout:
 *	    Block 0:    ColumnarMetaPageData + ColumnarStripeInfo array
 *	    Block 1..N: Compressed stripe data (sequential blocks per stripe)
 *
 *	  Serialization format per stripe (before compression):
 *	    For each column:
 *	      - Null bitmap: ceil(row_count / 8) bytes, 1 = null
 *	      - Values: for non-null rows:
 *	          Fixed-length types: raw Datum bytes (attlen bytes each)
 *	          Variable-length types: [int32 len][len bytes data] per value
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_columnar/columnar_storage.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <zstd.h>

#include "access/generic_xlog.h"
#include "access/xloginsert.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/smgr.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "alohadb_columnar.h"

/*
 * Offset into page free space where the ColumnarMetaPageData lives.
 * We use PageGetContents() which points right after the page header.
 */
#define META_OFFSET		0

/*
 * The stripe info array starts immediately after the ColumnarMetaPageData
 * in the same page.
 */
#define STRIPE_INFO_OFFSET	(MAXALIGN(sizeof(ColumnarMetaPageData)))

/*
 * Maximum number of stripe descriptors that fit on the metadata page.
 * The usable space on a page (after page header) is BLCKSZ - SizeOfPageHeaderData.
 */
#define MAX_STRIPES_ON_META_PAGE \
	((int) ((BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - STRIPE_INFO_OFFSET) / \
			sizeof(ColumnarStripeInfo)))


/* ----------------------------------------------------------------
 * Metadata read/write
 * ----------------------------------------------------------------
 */

/*
 * columnar_write_metadata
 *		Write the ColumnarMetaPageData to block 0.
 *
 * Caller must ensure the relation has at least 1 block.
 * Uses GenericXLog for WAL safety.
 */
void
columnar_write_metadata(Relation rel, ColumnarMetaPageData *meta)
{
	Buffer		buf;
	Page		page;
	ColumnarMetaPageData *page_meta;

	buf = ReadBuffer(rel, COLUMNAR_META_BLOCKNO);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	page = BufferGetPage(buf);
	page_meta = (ColumnarMetaPageData *) PageGetContents(page);
	memcpy(page_meta, meta, sizeof(ColumnarMetaPageData));

	START_CRIT_SECTION();
	MarkBufferDirty(buf);
	log_newpage_buffer(buf, true);
	END_CRIT_SECTION();

	UnlockReleaseBuffer(buf);
}

/*
 * columnar_read_metadata
 *		Read the ColumnarMetaPageData from block 0.
 *
 * Caller must ensure the relation has at least 1 block.
 */
void
columnar_read_metadata(Relation rel, ColumnarMetaPageData *meta)
{
	Buffer		buf;
	Page		page;
	ColumnarMetaPageData *page_meta;

	buf = ReadBuffer(rel, COLUMNAR_META_BLOCKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);

	page = BufferGetPage(buf);
	page_meta = (ColumnarMetaPageData *) PageGetContents(page);

	memcpy(meta, page_meta, sizeof(ColumnarMetaPageData));

	UnlockReleaseBuffer(buf);

	/* Sanity check */
	if (meta->magic != COLUMNAR_MAGIC)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("columnar metadata magic mismatch: expected 0x%08X, got 0x%08X",
						COLUMNAR_MAGIC, meta->magic)));
}

/*
 * columnar_read_stripe_info
 *		Read a single ColumnarStripeInfo from the metadata page.
 */
void
columnar_read_stripe_info(Relation rel, int stripe_idx,
						  ColumnarStripeInfo *sinfo)
{
	Buffer		buf;
	Page		page;
	ColumnarStripeInfo *stripe_array;

	if (stripe_idx < 0 || stripe_idx >= MAX_STRIPES_ON_META_PAGE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("columnar stripe index %d out of range", stripe_idx)));

	buf = ReadBuffer(rel, COLUMNAR_META_BLOCKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);

	page = BufferGetPage(buf);
	stripe_array = (ColumnarStripeInfo *)
		((char *) PageGetContents(page) + STRIPE_INFO_OFFSET);

	memcpy(sinfo, &stripe_array[stripe_idx], sizeof(ColumnarStripeInfo));

	UnlockReleaseBuffer(buf);
}

/*
 * write_stripe_info
 *		Write a ColumnarStripeInfo entry to the metadata page.
 *
 * This also updates the ColumnarMetaPageData (total_rows, num_stripes).
 * Called under exclusive lock on the metadata buffer.
 */
static void
write_stripe_info(Relation rel, ColumnarMetaPageData *meta,
				  ColumnarStripeInfo *sinfo, int stripe_idx)
{
	Buffer		buf;
	Page		bufpage;
	ColumnarMetaPageData *page_meta;
	ColumnarStripeInfo *stripe_array;

	if (stripe_idx >= MAX_STRIPES_ON_META_PAGE)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("columnar table has too many stripes (%d), maximum is %d",
						stripe_idx + 1, MAX_STRIPES_ON_META_PAGE)));

	buf = ReadBuffer(rel, COLUMNAR_META_BLOCKNO);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	bufpage = BufferGetPage(buf);

	/* Update metadata header */
	page_meta = (ColumnarMetaPageData *) PageGetContents(bufpage);
	memcpy(page_meta, meta, sizeof(ColumnarMetaPageData));

	/* Write stripe info entry */
	stripe_array = (ColumnarStripeInfo *)
		((char *) PageGetContents(bufpage) + STRIPE_INFO_OFFSET);
	memcpy(&stripe_array[stripe_idx], sinfo, sizeof(ColumnarStripeInfo));

	START_CRIT_SECTION();
	MarkBufferDirty(buf);
	log_newpage_buffer(buf, true);
	END_CRIT_SECTION();

	UnlockReleaseBuffer(buf);
}


/* ----------------------------------------------------------------
 * Serialization helpers
 * ----------------------------------------------------------------
 */

/*
 * Estimate the maximum uncompressed size for a stripe.
 * This is a rough upper bound to pre-allocate the serialization buffer.
 */
static Size
estimate_stripe_size(ColumnarWriteState *wstate)
{
	Size		size = 0;
	int			i;

	for (i = 0; i < wstate->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(wstate->tupdesc, i);

		/* Null bitmap */
		size += (wstate->buffered_rows + 7) / 8;

		if (attr->attisdropped)
			continue;

		if (attr->attlen > 0)
		{
			/* Fixed-length type */
			size += (Size) attr->attlen * wstate->buffered_rows;
		}
		else
		{
			/*
			 * Variable-length: estimate 128 bytes per value average as upper
			 * bound for initial allocation.  We'll grow if needed.
			 */
			size += (Size) 128 * wstate->buffered_rows;
		}
	}

	/* Add some headroom */
	return size + 1024;
}

/*
 * serialize_stripe
 *		Convert buffered column data into a flat binary buffer.
 *
 * Returns the serialized buffer and sets *out_size.
 * The buffer is allocated in the current memory context.
 *
 * Format per column:
 *   [null_bitmap: ceil(nrows/8) bytes]
 *   [values: see below]
 *
 * For fixed-length types: attlen bytes per non-null value.
 * For variable-length types: [int32 len][data...] per non-null value.
 */
static char *
serialize_stripe(ColumnarWriteState *wstate, Size *out_size)
{
	Size		bufsize;
	Size		used;
	char	   *buf;
	int			nrows = wstate->buffered_rows;
	int			i,
				r;

	bufsize = estimate_stripe_size(wstate);
	buf = palloc(bufsize);
	used = 0;

	for (i = 0; i < wstate->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(wstate->tupdesc, i);
		int			bitmap_bytes = (nrows + 7) / 8;

		/* Ensure we have space for the null bitmap */
		while (used + bitmap_bytes > bufsize)
		{
			bufsize *= 2;
			buf = repalloc(buf, bufsize);
		}

		/* Write null bitmap: bit set = null */
		memset(buf + used, 0, bitmap_bytes);
		for (r = 0; r < nrows; r++)
		{
			if (wstate->col_nulls[i][r])
				buf[used + r / 8] |= (1 << (r % 8));
		}
		used += bitmap_bytes;

		if (attr->attisdropped)
			continue;

		/* Write values */
		if (attr->attlen > 0)
		{
			/* Fixed-length type */
			int			attlen = attr->attlen;
			Size		needed = (Size) attlen * nrows;

			while (used + needed > bufsize)
			{
				bufsize *= 2;
				buf = repalloc(buf, bufsize);
			}

			for (r = 0; r < nrows; r++)
			{
				if (!wstate->col_nulls[i][r])
				{
					if (attr->attbyval)
					{
						/* Store Datum directly for pass-by-value types */
						memcpy(buf + used, &wstate->col_values[i][r], attlen);
					}
					else
					{
						/* Pass-by-reference fixed-length */
						memcpy(buf + used,
							   DatumGetPointer(wstate->col_values[i][r]),
							   attlen);
					}
				}
				else
				{
					/* Write zeros for null values (placeholder) */
					memset(buf + used, 0, attlen);
				}
				used += attlen;
			}
		}
		else
		{
			/* Variable-length type */
			for (r = 0; r < nrows; r++)
			{
				if (!wstate->col_nulls[i][r])
				{
					Datum		val = wstate->col_values[i][r];
					struct varlena *vl = (struct varlena *) DatumGetPointer(val);
					int32		len = VARSIZE(vl);

					while (used + sizeof(int32) + len > bufsize)
					{
						bufsize *= 2;
						buf = repalloc(buf, bufsize);
					}

					memcpy(buf + used, &len, sizeof(int32));
					used += sizeof(int32);
					memcpy(buf + used, vl, len);
					used += len;
				}
				else
				{
					/* Write a zero-length marker for null */
					int32		zero = 0;

					while (used + sizeof(int32) > bufsize)
					{
						bufsize *= 2;
						buf = repalloc(buf, bufsize);
					}
					memcpy(buf + used, &zero, sizeof(int32));
					used += sizeof(int32);
				}
			}
		}
	}

	*out_size = used;
	return buf;
}

/*
 * deserialize_stripe
 *		Reconstruct column arrays from a flat binary buffer.
 *
 * Allocates stripe_values and stripe_nulls arrays in the given memory context.
 */
static void
deserialize_stripe(char *data, Size data_size, TupleDesc tupdesc,
				   int nrows, MemoryContext mcxt,
				   Datum ***out_values, bool ***out_nulls)
{
	MemoryContext oldcxt;
	int			natts = tupdesc->natts;
	Datum	  **values;
	bool	  **nulls;
	Size		pos = 0;
	int			i,
				r;

	oldcxt = MemoryContextSwitchTo(mcxt);

	values = palloc(sizeof(Datum *) * natts);
	nulls = palloc(sizeof(bool *) * natts);

	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		int			bitmap_bytes = (nrows + 7) / 8;

		values[i] = palloc(sizeof(Datum) * nrows);
		nulls[i] = palloc(sizeof(bool) * nrows);

		/* Read null bitmap */
		if (pos + bitmap_bytes > data_size)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("columnar stripe data truncated reading null bitmap for column %d",
							i)));

		for (r = 0; r < nrows; r++)
		{
			nulls[i][r] = (data[pos + r / 8] & (1 << (r % 8))) != 0;
		}
		pos += bitmap_bytes;

		if (attr->attisdropped)
		{
			for (r = 0; r < nrows; r++)
			{
				values[i][r] = (Datum) 0;
				nulls[i][r] = true;
			}
			continue;
		}

		/* Read values */
		if (attr->attlen > 0)
		{
			int			attlen = attr->attlen;

			for (r = 0; r < nrows; r++)
			{
				if (pos + attlen > data_size)
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("columnar stripe data truncated reading column %d row %d",
									i, r)));

				if (!nulls[i][r])
				{
					if (attr->attbyval)
					{
						/* Reconstruct Datum from stored bytes */
						Datum		d = 0;

						memcpy(&d, data + pos, attlen);
						values[i][r] = d;
					}
					else
					{
						/* Pass-by-reference: copy to palloc'd memory */
						char	   *p = palloc(attlen);

						memcpy(p, data + pos, attlen);
						values[i][r] = PointerGetDatum(p);
					}
				}
				else
				{
					values[i][r] = (Datum) 0;
				}
				pos += attlen;
			}
		}
		else
		{
			/* Variable-length type */
			for (r = 0; r < nrows; r++)
			{
				int32		len;

				if (pos + sizeof(int32) > data_size)
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("columnar stripe data truncated reading varlena length for column %d row %d",
									i, r)));

				memcpy(&len, data + pos, sizeof(int32));
				pos += sizeof(int32);

				if (!nulls[i][r] && len > 0)
				{
					struct varlena *vl;

					if (pos + len > data_size)
						ereport(ERROR,
								(errcode(ERRCODE_DATA_CORRUPTED),
								 errmsg("columnar stripe data truncated reading varlena data for column %d row %d",
										i, r)));

					vl = palloc(len);
					memcpy(vl, data + pos, len);
					values[i][r] = PointerGetDatum(vl);
					pos += len;
				}
				else
				{
					values[i][r] = (Datum) 0;
				}
			}
		}
	}

	MemoryContextSwitchTo(oldcxt);

	*out_values = values;
	*out_nulls = nulls;
}


/* ----------------------------------------------------------------
 * Stripe flush (write)
 * ----------------------------------------------------------------
 */

/*
 * columnar_flush_stripe
 *		Serialize, compress, and write a stripe of buffered data to the
 *		relation's storage.
 *
 * After writing, the write state's buffer is reset (buffered_rows = 0).
 */
void
columnar_flush_stripe(Relation rel, ColumnarWriteState *wstate)
{
	MemoryContext oldcxt;
	ColumnarMetaPageData meta;
	ColumnarStripeInfo sinfo;
	char	   *serialized;
	Size		serialized_size;
	char	   *compressed;
	Size		compressed_size;
	Size		compress_bound;
	int			nblocks;
	BlockNumber start_block;
	int			blk;
	Size		offset;
	int			nrows;

	if (wstate->buffered_rows == 0)
		return;

	nrows = wstate->buffered_rows;

	/* Serialize column data */
	oldcxt = MemoryContextSwitchTo(wstate->mcxt);

	serialized = serialize_stripe(wstate, &serialized_size);

	/* Compress with zstd */
	compress_bound = ZSTD_compressBound(serialized_size);
	compressed = palloc(compress_bound);
	compressed_size = ZSTD_compress(compressed, compress_bound,
									serialized, serialized_size,
									3);		/* compression level 3 */

	if (ZSTD_isError(compressed_size))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("zstd compression failed: %s",
						ZSTD_getErrorName(compressed_size))));

	pfree(serialized);

	MemoryContextSwitchTo(oldcxt);

	/*
	 * Initialize the metadata page if this is the first write.
	 * The metadata page is not created during relation_set_new_filelocator
	 * because the relation's locator may not be stable at that point.
	 */
	/*
	 * Initialize the metadata page if this is the first write.
	 * The metadata page is not created during relation_set_new_filelocator
	 * because the relation's locator may not be stable at that point.
	 */
	if (ColumnarGetNblocks(rel) == 0)
	{
		Buffer		metabuf;
		Page		metapage;
		ColumnarMetaPageData *page_meta;

		metabuf = ExtendBufferedRel(BMR_REL(rel), MAIN_FORKNUM, NULL,
									EB_LOCK_FIRST);
		metapage = BufferGetPage(metabuf);

		PageInit(metapage, BLCKSZ, 0);

		page_meta = (ColumnarMetaPageData *) PageGetContents(metapage);
		page_meta->magic = COLUMNAR_MAGIC;
		page_meta->version = COLUMNAR_VERSION;
		page_meta->total_rows = 0;
		page_meta->num_stripes = 0;
		page_meta->natts = wstate->natts;
		page_meta->data_start_block = 1;

		START_CRIT_SECTION();
		MarkBufferDirty(metabuf);
		log_newpage_buffer(metabuf, true);
		END_CRIT_SECTION();

		UnlockReleaseBuffer(metabuf);
	}

	/* Read current metadata */
	columnar_read_metadata(rel, &meta);

	/* Calculate storage blocks needed */
	nblocks = (compressed_size + BLCKSZ - 1) / BLCKSZ;

	/*
	 * Determine where to place this stripe.  New stripes go at the end of
	 * the relation.
	 */
	start_block = ColumnarGetNblocks(rel);

	/*
	 * Extend the relation and write compressed data into new blocks.
	 * We use GenericXLog for WAL safety on each block.
	 */
	offset = 0;
	for (blk = 0; blk < nblocks; blk++)
	{
		Buffer		buf;
		Page		page;
		Size		chunk;

		buf = ExtendBufferedRel(BMR_REL(rel), MAIN_FORKNUM, NULL,
								EB_LOCK_FIRST);
		page = BufferGetPage(buf);

		PageInit(page, BLCKSZ, 0);

		/*
		 * Write compressed data into the page's free space area.
		 * We use the full page body (BLCKSZ - page header) for data.
		 */
		chunk = Min(compressed_size - offset,
					BLCKSZ - MAXALIGN(SizeOfPageHeaderData));

		memcpy(PageGetContents(page), compressed + offset, chunk);
		offset += chunk;

		START_CRIT_SECTION();
		MarkBufferDirty(buf);
		log_newpage_buffer(buf, true);
		END_CRIT_SECTION();

		UnlockReleaseBuffer(buf);
	}

	pfree(compressed);

	/* Prepare stripe info */
	sinfo.first_row = meta.total_rows;
	sinfo.row_count = nrows;
	sinfo.compressed_size = compressed_size;
	sinfo.uncompressed_size = serialized_size;
	sinfo.start_block = start_block;
	sinfo.num_blocks = nblocks;

	/* Update metadata */
	meta.total_rows += nrows;
	meta.num_stripes++;

	/* Write updated metadata and stripe info to block 0 */
	write_stripe_info(rel, &meta, &sinfo, meta.num_stripes - 1);

	/* Reset write state buffer */
	wstate->buffered_rows = 0;
}


/* ----------------------------------------------------------------
 * Stripe read (decompress)
 * ----------------------------------------------------------------
 */

/*
 * columnar_read_stripe
 *		Read and decompress a stripe, populating the scan descriptor's
 *		stripe data arrays.
 */
void
columnar_read_stripe(Relation rel, ColumnarScanDesc scan, int stripe_idx)
{
	ColumnarStripeInfo sinfo;
	char	   *compressed;
	Size		compressed_size;
	char	   *decompressed;
	Size		decompressed_size;
	Size		offset;
	int			blk;
	TupleDesc	tupdesc = RelationGetDescr(rel);

	/* Free previous stripe data if any */
	if (scan->stripe_mcxt != NULL)
	{
		MemoryContextReset(scan->stripe_mcxt);
	}
	else
	{
		scan->stripe_mcxt = AllocSetContextCreate(CurrentMemoryContext,
												  "columnar stripe",
												  ALLOCSET_DEFAULT_SIZES);
	}

	/* Read stripe info from metadata page */
	columnar_read_stripe_info(rel, stripe_idx, &sinfo);

	scan->stripe_rows = sinfo.row_count;
	if (sinfo.row_count == 0)
		return;

	compressed_size = sinfo.compressed_size;
	compressed = MemoryContextAlloc(scan->stripe_mcxt, compressed_size);

	/* Read compressed data from sequential blocks */
	offset = 0;
	for (blk = 0; blk < sinfo.num_blocks; blk++)
	{
		Buffer		buf;
		Page		page;
		Size		chunk;

		buf = ReadBuffer(rel, sinfo.start_block + blk);
		LockBuffer(buf, BUFFER_LOCK_SHARE);

		page = BufferGetPage(buf);

		chunk = Min(compressed_size - offset,
					BLCKSZ - MAXALIGN(SizeOfPageHeaderData));

		memcpy(compressed + offset, PageGetContents(page), chunk);
		offset += chunk;

		UnlockReleaseBuffer(buf);
	}

	/* Decompress */
	decompressed_size = ZSTD_getFrameContentSize(compressed, compressed_size);
	if (decompressed_size == ZSTD_CONTENTSIZE_ERROR ||
		decompressed_size == ZSTD_CONTENTSIZE_UNKNOWN)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cannot determine decompressed size for columnar stripe %d",
						stripe_idx)));

	decompressed = MemoryContextAlloc(scan->stripe_mcxt, decompressed_size);

	{
		Size		result;

		result = ZSTD_decompress(decompressed, decompressed_size,
								 compressed, compressed_size);

		if (ZSTD_isError(result))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("zstd decompression failed for columnar stripe %d: %s",
							stripe_idx, ZSTD_getErrorName(result))));
	}

	pfree(compressed);

	/* Deserialize into column arrays */
	deserialize_stripe(decompressed, decompressed_size, tupdesc,
					   sinfo.row_count, scan->stripe_mcxt,
					   &scan->stripe_values, &scan->stripe_nulls);
}
