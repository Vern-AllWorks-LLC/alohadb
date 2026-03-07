/*-------------------------------------------------------------------------
 *
 * ivfflatvacuum.c
 *	  VACUUM routines for the IVFFlat index access method.
 *
 * Currently IVFFlat does not reclaim space from deleted tuples.
 * ambulkdelete marks dead entries by invalidating their heap TID.
 * amvacuumcleanup reports statistics.
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * IDENTIFICATION
 *	  src/backend/access/ivfflat/ivfflatvacuum.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/generic_xlog.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/vector.h"

#include "ivfflat.h"

/*
 * ivfflatbulkdelete - delete index entries for dead heap tuples.
 *
 * Scans all posting list pages and marks entries whose heap TIDs are
 * identified as dead by the callback.  "Marking" is done by invalidating
 * the heap TID (the space is not reclaimed until a full REINDEX).
 *
 * For each posting page, we check all entries first (using the original page
 * contents), then if any need deletion we create a single GenericXLog record
 * that modifies the page to invalidate all dead TIDs at once.
 */
IndexBulkDeleteResult *
ivfflatbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
				  IndexBulkDeleteCallback callback, void *callback_state)
{
	Relation	index = info->index;
	Buffer		meta_buf;
	Page		meta_page;
	IvfflatMetaPageData *metadata;
	int			num_lists;
	int			dim;
	BlockNumber first_centroid_blkno;
	Size		entry_size;
	Size		centry_size;
	int			list_idx;

	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	/* Read metapage */
	meta_buf = ReadBuffer(index, IVFFLAT_METAPAGE_BLKNO);
	LockBuffer(meta_buf, BUFFER_LOCK_SHARE);
	meta_page = BufferGetPage(meta_buf);
	metadata = (IvfflatMetaPageData *) PageGetContents(meta_page);

	num_lists = metadata->num_lists;
	dim = metadata->dimensions;
	first_centroid_blkno = metadata->first_centroid_blkno;

	UnlockReleaseBuffer(meta_buf);

	if (num_lists == 0)
		return stats;

	entry_size = IVFFLAT_ENTRY_SIZE(dim);
	centry_size = MAXALIGN(IVFFLAT_LIST_SIZE(dim));

	/*
	 * Walk each centroid's posting list, checking entries against the
	 * callback.
	 */
	{
		BlockNumber cblkno = first_centroid_blkno;

		list_idx = 0;

		while (list_idx < num_lists)
		{
			Buffer		cbuf;
			Page		cpage;
			char	   *cptr;
			char	   *cend;

			cbuf = ReadBuffer(index, cblkno);
			LockBuffer(cbuf, BUFFER_LOCK_SHARE);
			cpage = BufferGetPage(cbuf);

			cptr = (char *) PageGetContents(cpage);
			cend = (char *) cpage + ((PageHeader) cpage)->pd_lower;

			while (cptr + centry_size <= cend && list_idx < num_lists)
			{
				IvfflatListData *list = (IvfflatListData *) cptr;
				BlockNumber posting_blkno = list->first_posting_blkno;

				/* Walk this list's posting pages */
				while (BlockNumberIsValid(posting_blkno))
				{
					Buffer		pbuf;
					Page		ppage;
					Page		modified_page;
					char	   *ptr;
					char	   *end;
					bool		page_has_deletions = false;
					BlockNumber next_blkno;

					CHECK_FOR_INTERRUPTS();

					pbuf = ReadBuffer(index, posting_blkno);
					LockBuffer(pbuf, BUFFER_LOCK_EXCLUSIVE);
					ppage = BufferGetPage(pbuf);

					/*
					 * First pass: check if any entries on this page need
					 * deletion.  Also count tuples.
					 */
					ptr = (char *) PageGetContents(ppage);
					end = (char *) ppage + ((PageHeader) ppage)->pd_lower;

					while (ptr + entry_size <= end)
					{
						IvfflatEntryData *entry = (IvfflatEntryData *) ptr;

						if (ItemPointerIsValid(&entry->heap_tid))
						{
							stats->num_index_tuples++;

							if (callback(&entry->heap_tid, callback_state))
								page_has_deletions = true;
						}

						ptr += entry_size;
					}

					/*
					 * Second pass: if deletions needed, register a
					 * GenericXLog and invalidate all dead TIDs.
					 */
					if (page_has_deletions)
					{
						GenericXLogState *state;

						state = GenericXLogStart(index);
						modified_page = GenericXLogRegisterBuffer(state, pbuf, 0);

						ptr = (char *) PageGetContents(modified_page);
						end = (char *) modified_page + ((PageHeader) modified_page)->pd_lower;

						while (ptr + entry_size <= end)
						{
							IvfflatEntryData *entry = (IvfflatEntryData *) ptr;

							if (ItemPointerIsValid(&entry->heap_tid) &&
								callback(&entry->heap_tid, callback_state))
							{
								ItemPointerSetInvalid(&entry->heap_tid);
								stats->tuples_removed++;
								stats->num_index_tuples--;
							}

							ptr += entry_size;
						}

						GenericXLogFinish(state);

						/*
						 * Read nextblkno from the modified page (the buffer
						 * content has been updated by GenericXLogFinish).
						 */
						ppage = BufferGetPage(pbuf);
					}

					next_blkno = IvfflatPageGetOpaque(ppage)->nextblkno;
					UnlockReleaseBuffer(pbuf);
					posting_blkno = next_blkno;
				}

				cptr += centry_size;
				list_idx++;
			}

			UnlockReleaseBuffer(cbuf);
			cblkno++;
		}
	}

	return stats;
}

/*
 * ivfflatvacuumcleanup - post-VACUUM cleanup for IVFFlat.
 *
 * Returns statistics about the index.
 */
IndexBulkDeleteResult *
ivfflatvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	Relation	index = info->index;

	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	stats->num_pages = RelationGetNumberOfBlocks(index);
	stats->estimated_count = info->estimated_count;

	/* If we weren't called during VACUUM, count the tuples ourselves */
	if (stats->num_index_tuples == 0)
	{
		Buffer		meta_buf;
		Page		meta_page;
		IvfflatMetaPageData *metadata;

		meta_buf = ReadBuffer(index, IVFFLAT_METAPAGE_BLKNO);
		LockBuffer(meta_buf, BUFFER_LOCK_SHARE);
		meta_page = BufferGetPage(meta_buf);
		metadata = (IvfflatMetaPageData *) PageGetContents(meta_page);

		stats->num_index_tuples = metadata->num_vectors;
		stats->estimated_count = true;

		UnlockReleaseBuffer(meta_buf);
	}

	return stats;
}
