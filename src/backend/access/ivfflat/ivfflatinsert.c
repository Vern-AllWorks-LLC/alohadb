/*-------------------------------------------------------------------------
 *
 * ivfflatinsert.c
 *	  Insert routines for the IVFFlat index access method.
 *
 * A new vector is inserted by finding the nearest centroid and appending
 * the entry to that centroid's posting list.  If the tail posting list
 * page is full, a new page is allocated and linked to the chain.
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * IDENTIFICATION
 *	  src/backend/access/ivfflat/ivfflatinsert.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/generic_xlog.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"
#include "utils/vector.h"

#include "ivfflat.h"

/*
 * Walk centroid pages to update the list descriptor for the given list_idx.
 *
 * Sets the first/last posting block numbers and increments the entry count.
 */
static void
ivfflat_update_list_descriptor(Relation index, int dim,
							   BlockNumber first_centroid_blkno,
							   int list_idx,
							   BlockNumber new_first_blkno,
							   BlockNumber new_last_blkno,
							   bool set_first)
{
	Size		centry_size = MAXALIGN(IVFFLAT_LIST_SIZE(dim));
	BlockNumber cblkno;
	int			cidx = 0;

	for (cblkno = first_centroid_blkno; cidx <= list_idx; cblkno++)
	{
		Buffer		cbuf;
		Page		cpage;
		GenericXLogState *cstate;
		char	   *ptr;
		char	   *end;

		cbuf = ReadBuffer(index, cblkno);
		LockBuffer(cbuf, BUFFER_LOCK_EXCLUSIVE);
		cstate = GenericXLogStart(index);
		cpage = GenericXLogRegisterBuffer(cstate, cbuf, 0);

		ptr = (char *) PageGetContents(cpage);
		end = (char *) cpage + ((PageHeader) cpage)->pd_lower;

		while (ptr + centry_size <= end && cidx <= list_idx)
		{
			if (cidx == list_idx)
			{
				IvfflatListData *list = (IvfflatListData *) ptr;

				if (set_first)
					list->first_posting_blkno = new_first_blkno;
				list->last_posting_blkno = new_last_blkno;
				list->num_entries++;

				GenericXLogFinish(cstate);
				UnlockReleaseBuffer(cbuf);
				return;
			}

			ptr += centry_size;
			cidx++;
		}

		GenericXLogFinish(cstate);
		UnlockReleaseBuffer(cbuf);
	}
}

/*
 * Update the metapage's num_vectors count (increment by 1).
 */
static void
ivfflat_increment_num_vectors(Relation index)
{
	Buffer		mbuf;
	Page		mpage;
	GenericXLogState *mstate;
	IvfflatMetaPageData *mdata;

	mbuf = ReadBuffer(index, IVFFLAT_METAPAGE_BLKNO);
	LockBuffer(mbuf, BUFFER_LOCK_EXCLUSIVE);
	mstate = GenericXLogStart(index);
	mpage = GenericXLogRegisterBuffer(mstate, mbuf, 0);
	mdata = (IvfflatMetaPageData *) PageGetContents(mpage);
	mdata->num_vectors++;
	GenericXLogFinish(mstate);
	UnlockReleaseBuffer(mbuf);
}

/*
 * Write a single vector entry into a posting page buffer.
 *
 * The caller must ensure there is enough free space on the page.
 */
static void
ivfflat_write_entry(Page page, ItemPointer ht_ctid, Vector *vec,
					int dim, Size entry_size)
{
	IvfflatEntryData *entry;
	Vector	   *entry_vec;

	entry = (IvfflatEntryData *) ((char *) page +
								  ((PageHeader) page)->pd_lower);
	entry->heap_tid = *ht_ctid;

	entry_vec = IvfflatEntryGetVector(entry);
	SET_VARSIZE(entry_vec, VECTOR_SIZE(dim));
	entry_vec->dim = dim;
	entry_vec->unused = 0;
	memcpy(entry_vec->x, vec->x, sizeof(float) * dim);

	((PageHeader) page)->pd_lower += entry_size;
}

/*
 * Find the nearest centroid to the given vector by scanning centroid pages.
 *
 * Returns the list index (0-based) and sets *last_blkno_out to the
 * last posting page of that list (or InvalidBlockNumber if empty).
 */
static int
ivfflat_find_nearest_list(Relation index, const Vector *vec, int dim,
						  BlockNumber first_centroid_blkno, int num_lists,
						  BlockNumber *last_blkno_out)
{
	Size		centry_size = MAXALIGN(IVFFLAT_LIST_SIZE(dim));
	BlockNumber blkno;
	int			list_idx = 0;
	int			best_list = 0;
	double		best_dist = 0;
	bool		first = true;
	IvfflatDistFunc distfunc;
	BlockNumber best_last = InvalidBlockNumber;

	distfunc = ivfflat_get_distfunc(IVFFLAT_L2_DISTANCE_STRATEGY);

	for (blkno = first_centroid_blkno; list_idx < num_lists; blkno++)
	{
		Buffer		cbuf;
		Page		cpage;
		char	   *ptr;
		char	   *end;

		cbuf = ReadBuffer(index, blkno);
		LockBuffer(cbuf, BUFFER_LOCK_SHARE);
		cpage = BufferGetPage(cbuf);

		ptr = (char *) PageGetContents(cpage);
		end = (char *) cpage + ((PageHeader) cpage)->pd_lower;

		while (ptr + centry_size <= end && list_idx < num_lists)
		{
			IvfflatListData *list = (IvfflatListData *) ptr;
			Vector	   *centroid = IvfflatListGetCentroid(list);
			double		dist;

			dist = distfunc(vec, centroid);

			if (first || dist < best_dist)
			{
				best_dist = dist;
				best_list = list_idx;
				best_last = list->last_posting_blkno;
				first = false;
			}

			ptr += centry_size;
			list_idx++;
		}

		UnlockReleaseBuffer(cbuf);
	}

	if (last_blkno_out)
		*last_blkno_out = best_last;

	return best_list;
}

/*
 * ivfflatinsert - insert a new vector into the IVFFlat index.
 *
 * Finds the nearest centroid and appends the entry to that centroid's
 * posting list.
 */
bool
ivfflatinsert(Relation index, Datum *values, bool *isnull,
			  ItemPointer ht_ctid, Relation heapRel,
			  IndexUniqueCheck checkUnique,
			  bool indexUnchanged,
			  struct IndexInfo *indexInfo)
{
	Vector	   *vec;
	Buffer		meta_buf;
	Page		meta_page;
	IvfflatMetaPageData *metadata;
	int			dim;
	int			num_lists;
	BlockNumber first_centroid_blkno;
	Size		entry_size;
	BlockNumber list_last_blkno;
	int			list_idx;

	/* Skip NULLs */
	if (isnull[0])
		return false;

	vec = DatumGetVector(values[0]);

	/* Read metapage to get index parameters */
	meta_buf = ReadBuffer(index, IVFFLAT_METAPAGE_BLKNO);
	LockBuffer(meta_buf, BUFFER_LOCK_SHARE);
	meta_page = BufferGetPage(meta_buf);
	metadata = (IvfflatMetaPageData *) PageGetContents(meta_page);

	dim = metadata->dimensions;
	num_lists = metadata->num_lists;
	first_centroid_blkno = metadata->first_centroid_blkno;

	UnlockReleaseBuffer(meta_buf);

	if (num_lists == 0)
		return false;

	/* Check dimensions match */
	if (vec->dim != dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("ivfflat index expects vectors with %d dimensions, "
						"got %d", dim, vec->dim)));

	entry_size = IVFFLAT_ENTRY_SIZE(dim);

	/* Find nearest centroid */
	list_idx = ivfflat_find_nearest_list(index, vec, dim,
										 first_centroid_blkno, num_lists,
										 &list_last_blkno);

	if (list_last_blkno == InvalidBlockNumber)
	{
		/*
		 * The list is empty -- allocate a new posting page.
		 */
		Buffer		new_buf;
		Page		new_page;
		GenericXLogState *state;
		BlockNumber new_blkno;

		new_buf = ReadBuffer(index, P_NEW);
		new_blkno = BufferGetBlockNumber(new_buf);
		LockBuffer(new_buf, BUFFER_LOCK_EXCLUSIVE);

		state = GenericXLogStart(index);
		new_page = GenericXLogRegisterBuffer(state, new_buf,
											 GENERIC_XLOG_FULL_IMAGE);
		ivfflat_page_init(new_page, IVFFLAT_PAGE_POSTING);
		ivfflat_write_entry(new_page, ht_ctid, vec, dim, entry_size);

		GenericXLogFinish(state);
		UnlockReleaseBuffer(new_buf);

		/* Update centroid list: set first and last to new_blkno */
		ivfflat_update_list_descriptor(index, dim, first_centroid_blkno,
									   list_idx, new_blkno, new_blkno,
									   true);
	}
	else
	{
		Buffer		tail_buf;
		Page		tail_page;

		tail_buf = ReadBuffer(index, list_last_blkno);
		LockBuffer(tail_buf, BUFFER_LOCK_EXCLUSIVE);
		tail_page = BufferGetPage(tail_buf);

		if (PageGetFreeSpace(tail_page) >= entry_size)
		{
			/*
			 * Enough room on the existing tail page.
			 */
			GenericXLogState *state;

			state = GenericXLogStart(index);
			tail_page = GenericXLogRegisterBuffer(state, tail_buf, 0);
			ivfflat_write_entry(tail_page, ht_ctid, vec, dim, entry_size);
			GenericXLogFinish(state);
			UnlockReleaseBuffer(tail_buf);

			/* Update centroid list: only increment count, last stays same */
			ivfflat_update_list_descriptor(index, dim, first_centroid_blkno,
										   list_idx,
										   InvalidBlockNumber,
										   list_last_blkno, false);
		}
		else
		{
			/*
			 * No room -- allocate a new page and link it.
			 */
			Buffer		new_buf;
			Page		new_page;
			GenericXLogState *state;
			GenericXLogState *new_state;
			BlockNumber new_blkno;

			/* Link old tail to new page */
			state = GenericXLogStart(index);
			tail_page = GenericXLogRegisterBuffer(state, tail_buf, 0);

			new_buf = ReadBuffer(index, P_NEW);
			new_blkno = BufferGetBlockNumber(new_buf);
			LockBuffer(new_buf, BUFFER_LOCK_EXCLUSIVE);

			IvfflatPageGetOpaque(tail_page)->nextblkno = new_blkno;
			GenericXLogFinish(state);
			UnlockReleaseBuffer(tail_buf);

			/* Write entry to new page */
			new_state = GenericXLogStart(index);
			new_page = GenericXLogRegisterBuffer(new_state, new_buf,
												 GENERIC_XLOG_FULL_IMAGE);
			ivfflat_page_init(new_page, IVFFLAT_PAGE_POSTING);
			ivfflat_write_entry(new_page, ht_ctid, vec, dim, entry_size);

			GenericXLogFinish(new_state);
			UnlockReleaseBuffer(new_buf);

			/* Update centroid list: set new last_blkno */
			ivfflat_update_list_descriptor(index, dim, first_centroid_blkno,
										   list_idx,
										   InvalidBlockNumber,
										   new_blkno, false);
		}
	}

	/* Update metapage vector count */
	ivfflat_increment_num_vectors(index);

	return false;
}
