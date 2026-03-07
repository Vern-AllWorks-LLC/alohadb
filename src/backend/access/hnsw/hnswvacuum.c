/*-------------------------------------------------------------------------
 *
 * hnswvacuum.c
 *	  VACUUM support for HNSW indexes.
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * IDENTIFICATION
 *	  src/backend/access/hnsw/hnswvacuum.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/generic_xlog.h"
#include "commands/vacuum.h"
#include "hnsw.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/indexfsm.h"

/*
 * hnswbulkdelete - bulk delete index entries.
 *
 * Walk through element pages, check each element against the callback,
 * and remove dead entries.  We also need to repair neighbor connections
 * that reference deleted elements.
 */
IndexBulkDeleteResult *
hnswbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
			   IndexBulkDeleteCallback callback, void *callback_state)
{
	Relation	index = info->index;
	BlockNumber nblocks;
	BlockNumber blkno;

	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	nblocks = RelationGetNumberOfBlocks(index);

	/* Phase 1: Scan element pages and delete dead entries */
	for (blkno = HNSW_HEAD_BLKNO; blkno < nblocks; blkno++)
	{
		Buffer		buf;
		Page		page;
		OffsetNumber maxoff;
		OffsetNumber off;
		GenericXLogState *state;
		bool		modified = false;

		CHECK_FOR_INTERRUPTS();

		buf = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
								 RBM_NORMAL, info->strategy);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		page = BufferGetPage(buf);

		/* Only process element pages */
		if (!(HnswPageGetOpaque(page)->flags & HNSW_PAGE_ELEMENT))
		{
			UnlockReleaseBuffer(buf);
			continue;
		}

		state = GenericXLogStart(index);
		page = GenericXLogRegisterBuffer(state, buf, 0);

		maxoff = PageGetMaxOffsetNumber(page);
		for (off = FirstOffsetNumber; off <= maxoff; off++)
		{
			ItemId		itemId = PageGetItemId(page, off);
			HnswElementTuple etup;

			if (!ItemIdIsUsed(itemId))
				continue;

			etup = (HnswElementTuple) PageGetItem(page, itemId);

			stats->num_index_tuples++;

			/* Check if this tuple should be deleted */
			if (callback(&etup->heaptid, callback_state))
			{
				/* Mark as dead by clearing the item */
				ItemIdSetUnused(itemId);
				modified = true;
				stats->tuples_removed++;
				stats->num_index_tuples--;
			}
		}

		if (modified)
			GenericXLogFinish(state);
		else
			GenericXLogAbort(state);

		UnlockReleaseBuffer(buf);
	}

	/* Phase 2: Scan neighbor pages and remove references to deleted elements */
	for (blkno = HNSW_HEAD_BLKNO; blkno < nblocks; blkno++)
	{
		Buffer		buf;
		Page		page;
		OffsetNumber maxoff;
		OffsetNumber off;
		GenericXLogState *state;
		bool		modified = false;

		CHECK_FOR_INTERRUPTS();

		buf = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
								 RBM_NORMAL, info->strategy);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		page = BufferGetPage(buf);

		if (!(HnswPageGetOpaque(page)->flags & HNSW_PAGE_NEIGHBOR))
		{
			UnlockReleaseBuffer(buf);
			continue;
		}

		state = GenericXLogStart(index);
		page = GenericXLogRegisterBuffer(state, buf, 0);

		maxoff = PageGetMaxOffsetNumber(page);
		for (off = FirstOffsetNumber; off <= maxoff; off++)
		{
			ItemId		itemId = PageGetItemId(page, off);
			HnswNeighborTuple ntup;

			if (!ItemIdIsUsed(itemId))
				continue;

			ntup = (HnswNeighborTuple) PageGetItem(page, itemId);

			/*
			 * Check if the neighbor's element has been deleted.  We do this
			 * by trying to read the element page and checking if the offset
			 * is still valid.
			 */
			if (ntup->elementBlkno != InvalidBlockNumber &&
				ntup->elementBlkno < nblocks)
			{
				Buffer		elemBuf;
				Page		elemPage;
				bool		dead = false;

				elemBuf = ReadBufferExtended(index, MAIN_FORKNUM,
											 ntup->elementBlkno,
											 RBM_NORMAL, info->strategy);
				LockBuffer(elemBuf, BUFFER_LOCK_SHARE);
				elemPage = BufferGetPage(elemBuf);

				if (ntup->elementOffset <= PageGetMaxOffsetNumber(elemPage))
				{
					ItemId		elemItemId;

					elemItemId = PageGetItemId(elemPage, ntup->elementOffset);
					if (!ItemIdIsUsed(elemItemId))
						dead = true;
				}
				else
				{
					dead = true;
				}

				UnlockReleaseBuffer(elemBuf);

				if (dead)
				{
					ItemIdSetUnused(itemId);
					modified = true;
				}
			}
		}

		if (modified)
			GenericXLogFinish(state);
		else
			GenericXLogAbort(state);

		UnlockReleaseBuffer(buf);
	}

	/* Record the number of pages in the index */
	stats->num_pages = nblocks;
	stats->pages_free = 0;

	return stats;
}

/*
 * hnswvacuumcleanup - post-VACUUM cleanup.
 *
 * Update metapage statistics and reclaim any free space.
 */
IndexBulkDeleteResult *
hnswvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	Relation	index = info->index;
	Buffer		metaBuf;
	Page		metaPage;
	GenericXLogState *state;
	HnswMetaPageData *metadata;
	BlockNumber nblocks;

	/* If bulkdelete wasn't called, allocate stats */
	if (stats == NULL)
	{
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

		/* Count tuples if this is just a cleanup (no delete phase) */
		nblocks = RelationGetNumberOfBlocks(index);
		for (BlockNumber blkno = HNSW_HEAD_BLKNO; blkno < nblocks; blkno++)
		{
			Buffer		buf;
			Page		page;
			OffsetNumber maxoff;
			OffsetNumber off;

			buf = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
									 RBM_NORMAL, info->strategy);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buf);

			if (HnswPageGetOpaque(page)->flags & HNSW_PAGE_ELEMENT)
			{
				maxoff = PageGetMaxOffsetNumber(page);
				for (off = FirstOffsetNumber; off <= maxoff; off++)
				{
					if (ItemIdIsUsed(PageGetItemId(page, off)))
						stats->num_index_tuples++;
				}
			}

			UnlockReleaseBuffer(buf);
		}
	}

	/* Update metapage with current element count */
	metaBuf = ReadBuffer(index, HNSW_METAPAGE_BLKNO);
	LockBuffer(metaBuf, BUFFER_LOCK_EXCLUSIVE);

	state = GenericXLogStart(index);
	metaPage = GenericXLogRegisterBuffer(state, metaBuf, 0);
	metadata = (HnswMetaPageData *) PageGetContents(metaPage);

	metadata->elementCount = (int64) stats->num_index_tuples;

	GenericXLogFinish(state);
	UnlockReleaseBuffer(metaBuf);

	/* Update stats */
	stats->num_pages = RelationGetNumberOfBlocks(index);
	stats->pages_free = 0;

	return stats;
}
