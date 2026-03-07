/*-------------------------------------------------------------------------
 *
 * hnswinsert.c
 *	  Insert tuples into an HNSW index.
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * IDENTIFICATION
 *	  src/backend/access/hnsw/hnswinsert.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/generic_xlog.h"
#include "hnsw.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"

/*
 * Read the metapage and return its data (caller must release buffer).
 */
static HnswMetaPageData *
hnsw_read_metapage(Relation index, Buffer *buf)
{
	Page		page;
	HnswMetaPageData *metadata;

	*buf = ReadBuffer(index, HNSW_METAPAGE_BLKNO);
	LockBuffer(*buf, BUFFER_LOCK_SHARE);

	page = BufferGetPage(*buf);
	metadata = (HnswMetaPageData *) PageGetContents(page);

	if (metadata->magicNumber != HNSW_MAGIC_NUMBER)
		elog(ERROR, "invalid HNSW index metapage (magic %08x)",
			 metadata->magicNumber);

	return metadata;
}

/*
 * Read an element from disk into an in-memory HnswElement.
 */
static HnswElement *
hnsw_read_element(Relation index, BlockNumber blkno, OffsetNumber offset,
				  MemoryContext ctx)
{
	Buffer		buf;
	Page		page;
	HnswElementTuple etup;
	HnswElement *element;
	MemoryContext oldCtx;
	ItemId		itemId;

	buf = ReadBuffer(index, blkno);
	LockBuffer(buf, BUFFER_LOCK_SHARE);

	page = BufferGetPage(buf);
	itemId = PageGetItemId(page, offset);
	etup = (HnswElementTuple) PageGetItem(page, itemId);

	oldCtx = MemoryContextSwitchTo(ctx);

	element = (HnswElement *) palloc0(sizeof(HnswElement));
	ItemPointerCopy(&etup->heaptid, &element->heaptid);
	element->blkno = blkno;
	element->offset = offset;
	element->level = etup->level;
	element->dim = etup->dim;
	element->vec = (float *) palloc(sizeof(float) * etup->dim);
	memcpy(element->vec, etup->vec, sizeof(float) * etup->dim);

	/* Allocate neighbor lists (will be populated by reading neighbor pages) */
	element->neighbors = (List **) palloc0(sizeof(List *) * (element->level + 1));

	MemoryContextSwitchTo(oldCtx);

	UnlockReleaseBuffer(buf);

	return element;
}

/*
 * Load a subset of elements from the index into memory for neighbor search
 * during insertion.  This is a simplified version that loads elements
 * reachable from the entry point.
 */
static List *
hnsw_load_graph_elements(Relation index, HnswMetaPageData *meta,
						 MemoryContext ctx)
{
	List	   *elements = NIL;
	BlockNumber nblocks;
	BlockNumber blkno;

	nblocks = RelationGetNumberOfBlocks(index);

	/* Load all elements from element pages */
	for (blkno = HNSW_HEAD_BLKNO; blkno < nblocks; blkno++)
	{
		Buffer		buf;
		Page		page;
		OffsetNumber maxoff;
		OffsetNumber off;

		buf = ReadBuffer(index, blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);

		if (!(HnswPageGetOpaque(page)->flags & HNSW_PAGE_ELEMENT))
		{
			UnlockReleaseBuffer(buf);
			continue;
		}

		maxoff = PageGetMaxOffsetNumber(page);
		for (off = FirstOffsetNumber; off <= maxoff; off++)
		{
			ItemId		itemId = PageGetItemId(page, off);
			HnswElement *element;

			if (!ItemIdIsUsed(itemId))
				continue;

			element = hnsw_read_element(index, blkno, off, ctx);
			elements = lappend(elements, element);
		}

		UnlockReleaseBuffer(buf);
	}

	/* Allocate neighbor arrays for all elements */
	{
		ListCell   *lc;
		MemoryContext oldNCtx;

		oldNCtx = MemoryContextSwitchTo(ctx);
		foreach(lc, elements)
		{
			HnswElement *e = (HnswElement *) lfirst(lc);
			int			i;

			if (e->neighbors == NULL)
			{
				e->neighbors = (List **) palloc0(sizeof(List *) * (e->level + 1));
				for (i = 0; i <= e->level; i++)
					e->neighbors[i] = NIL;
			}
		}
		MemoryContextSwitchTo(oldNCtx);
	}

	/* Now load neighbor connections from neighbor pages */
	{
		ListCell   *lc;
		BlockNumber nblk;

		for (nblk = HNSW_HEAD_BLKNO; nblk < nblocks; nblk++)
		{
			Buffer		nbuf;
			Page		npage;
			OffsetNumber nmaxoff;
			OffsetNumber noff;

			nbuf = ReadBuffer(index, nblk);
			LockBuffer(nbuf, BUFFER_LOCK_SHARE);
			npage = BufferGetPage(nbuf);

			if (!(HnswPageGetOpaque(npage)->flags & HNSW_PAGE_NEIGHBOR))
			{
				UnlockReleaseBuffer(nbuf);
				continue;
			}

			nmaxoff = PageGetMaxOffsetNumber(npage);
			for (noff = FirstOffsetNumber; noff <= nmaxoff; noff++)
			{
				ItemId		nitemId = PageGetItemId(npage, noff);
				HnswNeighborTuple ntup;
				HnswElement *srcElem = NULL;
				HnswElement *neighborElem = NULL;

				if (!ItemIdIsUsed(nitemId))
					continue;

				ntup = (HnswNeighborTuple) PageGetItem(npage, nitemId);

				/* Find the source element */
				foreach(lc, elements)
				{
					HnswElement *e = (HnswElement *) lfirst(lc);

					if (e->blkno == ntup->srcBlkno &&
						e->offset == ntup->srcOffset)
					{
						srcElem = e;
						break;
					}
				}

				/* Find the target neighbor element */
				foreach(lc, elements)
				{
					HnswElement *e = (HnswElement *) lfirst(lc);

					if (e->blkno == ntup->elementBlkno &&
						e->offset == ntup->elementOffset)
					{
						neighborElem = e;
						break;
					}
				}

				/* Add neighbor connection to source element */
				if (srcElem != NULL && neighborElem != NULL &&
					ntup->layer <= srcElem->level)
				{
					MemoryContext oldNCtx = MemoryContextSwitchTo(ctx);
					HnswCandidate *cand;

					cand = (HnswCandidate *) palloc(sizeof(HnswCandidate));
					cand->element = neighborElem;
					cand->distance = ntup->distance;
					memset(&cand->ph_node, 0, sizeof(pairingheap_node));
					srcElem->neighbors[ntup->layer] =
						lappend(srcElem->neighbors[ntup->layer], cand);

					MemoryContextSwitchTo(oldNCtx);
				}
			}

			UnlockReleaseBuffer(nbuf);
		}
	}

	return elements;
}

/*
 * Write an element to the index and return its position.
 */
static void
hnsw_insert_element_to_disk(Relation index, HnswElement *element)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	Size		etupSize;
	char	   *tupleData;
	OffsetNumber off;
	BlockNumber blkno;
	HnswElementTupleData etup;

	etupSize = HNSW_ELEMENT_TUPLE_SIZE(element->dim);

	/* Initialize tuple */
	memset(&etup, 0, offsetof(HnswElementTupleData, vec));
	ItemPointerCopy(&element->heaptid, &etup.heaptid);
	etup.level = element->level;
	etup.dim = element->dim;
	etup.neighborPage = InvalidBlockNumber;
	etup.neighborOffset = InvalidOffsetNumber;
	etup.neighborCount = 0;

	/* Find or create an element page with space */
	blkno = HNSW_HEAD_BLKNO;
	for (;;)
	{
		if (blkno >= RelationGetNumberOfBlocks(index))
		{
			buf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW,
									 RBM_NORMAL, NULL);
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
			state = GenericXLogStart(index);
			page = GenericXLogRegisterBuffer(state, buf,
											 GENERIC_XLOG_FULL_IMAGE);
			PageInit(page, BufferGetPageSize(buf),
					 sizeof(HnswPageOpaqueData));
			HnswPageGetOpaque(page)->nextblkno = InvalidBlockNumber;
			HnswPageGetOpaque(page)->flags = HNSW_PAGE_ELEMENT;
			HnswPageGetOpaque(page)->unused = 0;
			break;
		}

		buf = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
								 RBM_NORMAL, NULL);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		state = GenericXLogStart(index);
		page = GenericXLogRegisterBuffer(state, buf, 0);

		if ((HnswPageGetOpaque(page)->flags & HNSW_PAGE_ELEMENT) &&
			PageGetFreeSpace(page) >= etupSize + sizeof(ItemIdData))
			break;

		GenericXLogAbort(state);
		UnlockReleaseBuffer(buf);
		blkno++;
	}

	/* Write the tuple */
	tupleData = (char *) palloc(etupSize);
	memcpy(tupleData, &etup, offsetof(HnswElementTupleData, vec));
	memcpy(tupleData + offsetof(HnswElementTupleData, vec),
		   element->vec, sizeof(float) * element->dim);

	off = PageAddItem(page, (Item) tupleData, etupSize,
					  InvalidOffsetNumber, false, false);
	if (off == InvalidOffsetNumber)
		elog(ERROR, "failed to add element to HNSW index page");

	pfree(tupleData);

	element->blkno = BufferGetBlockNumber(buf);
	element->offset = off;

	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
}

/*
 * Write neighbor entries for the newly inserted element.
 */
static void
hnsw_insert_neighbors_to_disk(Relation index, HnswElement *element)
{
	int			layer;

	for (layer = 0; layer <= element->level; layer++)
	{
		ListCell   *lc;

		foreach(lc, element->neighbors[layer])
		{
			HnswCandidate *cand = (HnswCandidate *) lfirst(lc);
			HnswNeighborTupleData ntup;
			Buffer		buf;
			Page		page;
			GenericXLogState *state;
			OffsetNumber off;
			BlockNumber curBlkno;
			bool		found = false;

			memset(&ntup, 0, sizeof(HnswNeighborTupleData));
			ItemPointerCopy(&cand->element->heaptid, &ntup.heaptid);
			ntup.srcBlkno = element->blkno;
			ntup.srcOffset = element->offset;
			ntup.elementBlkno = cand->element->blkno;
			ntup.elementOffset = cand->element->offset;
			ntup.layer = layer;
			ntup.distance = cand->distance;

			/* Find a neighbor page with space */
			curBlkno = HNSW_HEAD_BLKNO;
			while (curBlkno < RelationGetNumberOfBlocks(index))
			{
				buf = ReadBufferExtended(index, MAIN_FORKNUM, curBlkno,
										 RBM_NORMAL, NULL);
				LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
				state = GenericXLogStart(index);
				page = GenericXLogRegisterBuffer(state, buf, 0);

				if ((HnswPageGetOpaque(page)->flags & HNSW_PAGE_NEIGHBOR) &&
					PageGetFreeSpace(page) >=
					sizeof(HnswNeighborTupleData) + sizeof(ItemIdData))
				{
					found = true;
					break;
				}

				GenericXLogAbort(state);
				UnlockReleaseBuffer(buf);
				curBlkno++;
			}

			if (!found)
			{
				buf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW,
										 RBM_NORMAL, NULL);
				LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
				state = GenericXLogStart(index);
				page = GenericXLogRegisterBuffer(state, buf,
												 GENERIC_XLOG_FULL_IMAGE);
				PageInit(page, BufferGetPageSize(buf),
						 sizeof(HnswPageOpaqueData));
				HnswPageGetOpaque(page)->nextblkno = InvalidBlockNumber;
				HnswPageGetOpaque(page)->flags = HNSW_PAGE_NEIGHBOR;
				HnswPageGetOpaque(page)->unused = 0;
			}

			off = PageAddItem(page, (Item) &ntup,
							  sizeof(HnswNeighborTupleData),
							  InvalidOffsetNumber, false, false);
			if (off == InvalidOffsetNumber)
				elog(ERROR, "failed to add neighbor to HNSW index page");

			GenericXLogFinish(state);
			UnlockReleaseBuffer(buf);
		}
	}
}

/*
 * Update the metapage after an insertion.
 */
static void
hnsw_update_metapage(Relation index, HnswElement *newEntryPoint,
					 int maxLevel, int64 elementCount)
{
	Buffer		metaBuf;
	Page		metaPage;
	GenericXLogState *state;
	HnswMetaPageData *metadata;

	metaBuf = ReadBuffer(index, HNSW_METAPAGE_BLKNO);
	LockBuffer(metaBuf, BUFFER_LOCK_EXCLUSIVE);

	state = GenericXLogStart(index);
	metaPage = GenericXLogRegisterBuffer(state, metaBuf, 0);
	metadata = (HnswMetaPageData *) PageGetContents(metaPage);

	if (newEntryPoint != NULL)
	{
		metadata->entryBlkno = newEntryPoint->blkno;
		metadata->entryOffset = newEntryPoint->offset;
		metadata->entryLevel = newEntryPoint->level;
		metadata->maxLevel = maxLevel;
	}
	metadata->elementCount = elementCount;

	GenericXLogFinish(state);
	UnlockReleaseBuffer(metaBuf);
}

/*
 * hnswinsert - insert a tuple into an existing HNSW index.
 */
bool
hnswinsert(Relation index, Datum *values, bool *isnull,
		   ItemPointer ht_ctid, Relation heapRel,
		   IndexUniqueCheck checkUnique, bool indexUnchanged,
		   struct IndexInfo *indexInfo)
{
	MemoryContext insertCtx;
	MemoryContext oldCtx;
	Vector	   *vec;
	HnswElement *element;
	HnswMetaPageData *meta;
	HnswMetaPageData metaCopy;
	Buffer		metaBuf;
	int			level;
	int			m;
	double		ml;
	int			i;
	bool		updateEntry = false;

	/* Skip NULL vectors */
	if (isnull[0])
		return false;

	vec = DatumGetVector(values[0]);

	if (vec->dim < 1 || vec->dim > HNSW_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("vector dimension %d is out of range (1-%d)",
						vec->dim, HNSW_MAX_DIM)));

	/* Create a memory context for this insertion */
	insertCtx = AllocSetContextCreate(CurrentMemoryContext,
									  "HNSW insert",
									  ALLOCSET_DEFAULT_SIZES);
	oldCtx = MemoryContextSwitchTo(insertCtx);

	/* Read metapage */
	meta = hnsw_read_metapage(index, &metaBuf);
	memcpy(&metaCopy, meta, sizeof(HnswMetaPageData));
	UnlockReleaseBuffer(metaBuf);

	m = metaCopy.m;
	ml = 1.0 / log((double) m);

	/* Create new element */
	level = hnsw_random_level(ml);
	element = (HnswElement *) palloc0(sizeof(HnswElement));
	element->dim = vec->dim;
	element->vec = (float *) palloc(sizeof(float) * vec->dim);
	memcpy(element->vec, vec->x, sizeof(float) * vec->dim);
	ItemPointerCopy(ht_ctid, &element->heaptid);
	element->level = level;
	element->neighbors = (List **) palloc0(sizeof(List *) * (level + 1));
	for (i = 0; i <= level; i++)
		element->neighbors[i] = NIL;

	/* If index is empty, this is the first element */
	if (metaCopy.entryBlkno == InvalidBlockNumber)
	{
		hnsw_insert_element_to_disk(index, element);
		hnsw_update_metapage(index, element, level, 1);
		MemoryContextSwitchTo(oldCtx);
		MemoryContextDelete(insertCtx);
		return false;
	}

	/*
	 * Load the graph into memory for search.  This is the straightforward
	 * approach; a production implementation would use buffer-based search.
	 */
	{
		List	   *elements;
		HnswElement *entryPoint = NULL;
		HnswElement *ep;
		int			topLevel;
		int			lc;
		ListCell   *cell;

		elements = hnsw_load_graph_elements(index, &metaCopy, insertCtx);

		/* Find the entry point element */
		foreach(cell, elements)
		{
			HnswElement *e = (HnswElement *) lfirst(cell);

			if (e->blkno == metaCopy.entryBlkno &&
				e->offset == metaCopy.entryOffset)
			{
				entryPoint = e;
				break;
			}
		}

		if (entryPoint == NULL && elements != NIL)
			entryPoint = (HnswElement *) linitial(elements);

		if (entryPoint == NULL)
		{
			/* No elements found; insert as first */
			hnsw_insert_element_to_disk(index, element);
			hnsw_update_metapage(index, element, level, 1);
			MemoryContextSwitchTo(oldCtx);
			MemoryContextDelete(insertCtx);
			return false;
		}

		ep = entryPoint;
		topLevel = metaCopy.maxLevel;

		/* Phase 1: Greedy descent to level+1 */
		for (lc = topLevel; lc > level; lc--)
		{
			List	   *nearest;

			nearest = hnsw_search_layer(ep, vec, 1, lc,
										HNSW_L2_STRATEGY, elements);
			if (nearest != NIL)
			{
				HnswCandidate *best = (HnswCandidate *) linitial(nearest);

				ep = best->element;
			}
			list_free(nearest);
		}

		/* Phase 2: Insert at layers min(level, topLevel) down to 0 */
		for (lc = Min(level, topLevel); lc >= 0; lc--)
		{
			List	   *candidates;
			int			maxNeighbors;
			int			count;

			candidates = hnsw_search_layer(ep, vec,
										   metaCopy.efConstruction,
										   lc, HNSW_L2_STRATEGY,
										   elements);

			maxNeighbors = hnsw_get_max_neighbors(m, lc);

			count = 0;
			foreach(cell, candidates)
			{
				HnswCandidate *cand = (HnswCandidate *) lfirst(cell);
				HnswCandidate *fwdLink;

				if (count >= maxNeighbors)
					break;

				fwdLink = (HnswCandidate *) palloc(sizeof(HnswCandidate));
				fwdLink->element = cand->element;
				fwdLink->distance = cand->distance;
				memset(&fwdLink->ph_node, 0, sizeof(pairingheap_node));
				element->neighbors[lc] = lappend(element->neighbors[lc],
												 fwdLink);
				count++;
			}

			if (candidates != NIL)
			{
				HnswCandidate *best = (HnswCandidate *) linitial(candidates);

				ep = best->element;
			}
			list_free(candidates);
		}

		/* Write element and its neighbors to disk */
		hnsw_insert_element_to_disk(index, element);
		hnsw_insert_neighbors_to_disk(index, element);

		/* Update metapage if needed */
		if (level > topLevel)
			updateEntry = true;

		hnsw_update_metapage(index,
							updateEntry ? element : NULL,
							updateEntry ? level : topLevel,
							metaCopy.elementCount + 1);

		/*
		 * No need to explicitly free elements; the insertCtx memory context
		 * will be destroyed below, freeing all allocations.
		 */
	}

	MemoryContextSwitchTo(oldCtx);
	MemoryContextDelete(insertCtx);

	return false;
}
