/*-------------------------------------------------------------------------
 *
 * hnsw_scan.c
 *	  Scan support for HNSW index.
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * IDENTIFICATION
 *	  src/backend/access/hnsw/hnsw_scan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/relscan.h"
#include "hnsw.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/*
 * Read an element from disk into a HnswElement allocated in the given context.
 */
static HnswElement *
hnsw_scan_read_element(Relation index, BlockNumber blkno,
					   OffsetNumber offset, MemoryContext ctx)
{
	Buffer		buf;
	Page		page;
	ItemId		itemId;
	HnswElementTuple etup;
	HnswElement *element;
	MemoryContext oldCtx;

	buf = ReadBuffer(index, blkno);
	LockBuffer(buf, BUFFER_LOCK_SHARE);

	page = BufferGetPage(buf);

	if (offset > PageGetMaxOffsetNumber(page) || offset < FirstOffsetNumber)
	{
		UnlockReleaseBuffer(buf);
		return NULL;
	}

	itemId = PageGetItemId(page, offset);
	if (!ItemIdIsUsed(itemId))
	{
		UnlockReleaseBuffer(buf);
		return NULL;
	}

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
	element->neighbors = NULL;	/* loaded on demand */

	MemoryContextSwitchTo(oldCtx);

	UnlockReleaseBuffer(buf);

	return element;
}

/*
 * Load all elements from the index into memory for scanning.
 * Returns a list of HnswElement* allocated in the given context.
 */
static List *
hnsw_scan_load_elements(Relation index, MemoryContext ctx)
{
	List	   *elements = NIL;
	BlockNumber nblocks;
	BlockNumber blkno;

	nblocks = RelationGetNumberOfBlocks(index);

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

			element = hnsw_scan_read_element(index, blkno, off, ctx);
			if (element != NULL)
				elements = lappend(elements, element);
		}

		UnlockReleaseBuffer(buf);
	}

	/* Load neighbor connections */
	{
		ListCell   *lc;

		/* Allocate neighbor arrays for all elements */
		foreach(lc, elements)
		{
			HnswElement *e = (HnswElement *) lfirst(lc);
			MemoryContext oldCtx = MemoryContextSwitchTo(ctx);
			int			i;

			e->neighbors = (List **) palloc0(sizeof(List *) * (e->level + 1));
			for (i = 0; i <= e->level; i++)
				e->neighbors[i] = NIL;

			MemoryContextSwitchTo(oldCtx);
		}

		/* Scan neighbor pages and populate neighbor lists */
		for (blkno = HNSW_HEAD_BLKNO; blkno < nblocks; blkno++)
		{
			Buffer		nbuf;
			Page		npage;
			OffsetNumber nmaxoff;
			OffsetNumber noff;

			nbuf = ReadBuffer(index, blkno);
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

				/* Find the source element (who owns this neighbor edge) */
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

				/*
				 * If we found both source and target, add the neighbor
				 * connection to the source element's neighbor list at the
				 * appropriate layer.
				 */
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
 * Perform the HNSW search and populate the result list.
 */
static void
hnsw_scan_search(IndexScanDesc scan)
{
	HnswScanOpaque so = (HnswScanOpaque) scan->opaque;
	Relation	index = scan->indexRelation;
	Buffer		metaBuf;
	Page		metaPage;
	HnswMetaPageData *metadata;
	HnswMetaPageData metaCopy;
	List	   *elements;
	HnswElement *entryPoint = NULL;
	List	   *candidates;
	ListCell   *lc;
	HnswElement *ep;
	int			topLevel;
	int			layer;

	/* Read metapage */
	metaBuf = ReadBuffer(index, HNSW_METAPAGE_BLKNO);
	LockBuffer(metaBuf, BUFFER_LOCK_SHARE);
	metaPage = BufferGetPage(metaBuf);
	metadata = (HnswMetaPageData *) PageGetContents(metaPage);

	if (metadata->magicNumber != HNSW_MAGIC_NUMBER)
		elog(ERROR, "invalid HNSW index metapage");

	memcpy(&metaCopy, metadata, sizeof(HnswMetaPageData));
	UnlockReleaseBuffer(metaBuf);

	/* Empty index */
	if (metaCopy.entryBlkno == InvalidBlockNumber ||
		metaCopy.elementCount == 0)
	{
		so->resultList = NIL;
		return;
	}

	/* Load graph into memory */
	elements = hnsw_scan_load_elements(index, so->scanCtx);
	if (elements == NIL)
	{
		so->resultList = NIL;
		return;
	}

	/* Find entry point */
	foreach(lc, elements)
	{
		HnswElement *e = (HnswElement *) lfirst(lc);

		if (e->blkno == metaCopy.entryBlkno &&
			e->offset == metaCopy.entryOffset)
		{
			entryPoint = e;
			break;
		}
	}

	if (entryPoint == NULL)
		entryPoint = (HnswElement *) linitial(elements);

	ep = entryPoint;
	topLevel = metaCopy.maxLevel;

	/*
	 * Phase 1: Greedy descent from top layer to layer 1, finding the
	 * nearest element with ef=1 at each layer.
	 */
	for (layer = topLevel; layer >= 1; layer--)
	{
		List	   *nearest;

		nearest = hnsw_search_layer(ep, so->queryVec, 1, layer,
									so->strategy, elements);
		if (nearest != NIL)
		{
			HnswCandidate *best = (HnswCandidate *) linitial(nearest);

			ep = best->element;
		}
		list_free(nearest);
	}

	/*
	 * Phase 2: Search at layer 0 with ef = ef_search to find the candidates.
	 */
	candidates = hnsw_search_layer(ep, so->queryVec, so->efSearch, 0,
								   so->strategy, elements);

	/*
	 * If the graph has no neighbor connections (common for small indexes or
	 * when neighbor data wasn't fully reconstructed), fall back to a brute
	 * force scan of all elements.
	 */
	if (candidates == NIL && elements != NIL)
	{
		MemoryContext oldCtx = MemoryContextSwitchTo(so->scanCtx);

		foreach(lc, elements)
		{
			HnswElement *e = (HnswElement *) lfirst(lc);
			HnswCandidate *cand;
			Vector	   *evec;
			float		dist;

			evec = InitVector(e->dim);
			memcpy(evec->x, e->vec, sizeof(float) * e->dim);
			dist = hnsw_calc_distance(so->queryVec, evec, so->strategy);
			pfree(evec);

			cand = (HnswCandidate *) palloc(sizeof(HnswCandidate));
			cand->element = e;
			cand->distance = dist;
			memset(&cand->ph_node, 0, sizeof(pairingheap_node));
			candidates = lappend(candidates, cand);
		}

		/* Sort by distance using a simple insertion sort */
		{
			List	   *sorted = NIL;
			ListCell   *cc;

			foreach(cc, candidates)
			{
				HnswCandidate *c = (HnswCandidate *) lfirst(cc);
				ListCell   *cur;
				bool		inserted = false;

				foreach(cur, sorted)
				{
					HnswCandidate *s = (HnswCandidate *) lfirst(cur);

					if (c->distance < s->distance)
					{
						sorted = list_insert_nth(sorted,
												 foreach_current_index(cur),
												 c);
						inserted = true;
						break;
					}
				}
				if (!inserted)
					sorted = lappend(sorted, c);
			}

			list_free(candidates);
			candidates = sorted;
		}

		MemoryContextSwitchTo(oldCtx);
	}

	/* Store results */
	so->resultList = candidates;
	so->resultIdx = 0;
}

/*
 * hnswbeginscan - prepare for an index scan.
 */
IndexScanDesc
hnswbeginscan(Relation index, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	HnswScanOpaque so;

	scan = RelationGetIndexScan(index, nkeys, norderbys);

	so = (HnswScanOpaque) palloc0(sizeof(HnswScanOpaqueData));
	so->queryVec = NULL;
	so->strategy = 0;
	so->efSearch = hnsw_ef_search;
	so->resultQueue = NULL;
	so->firstCall = true;
	so->resultList = NIL;
	so->resultIdx = 0;
	so->scanCtx = AllocSetContextCreate(CurrentMemoryContext,
										"HNSW scan",
										ALLOCSET_DEFAULT_SIZES);

	scan->opaque = so;

	return scan;
}

/*
 * hnswrescan - restart an index scan.
 */
void
hnswrescan(IndexScanDesc scan, ScanKey keys, int nkeys,
		   ScanKey orderbys, int norderbys)
{
	HnswScanOpaque so = (HnswScanOpaque) scan->opaque;

	/* Reset scan state */
	so->firstCall = true;
	so->resultList = NIL;
	so->resultIdx = 0;
	so->efSearch = hnsw_ef_search;

	/* Reset the memory context */
	MemoryContextReset(so->scanCtx);

	if (orderbys != NULL && scan->numberOfOrderBys > 0)
	{
		MemoryContext oldCtx;

		/* Copy the orderby scankeys */
		memmove(scan->orderByData, orderbys,
				scan->numberOfOrderBys * sizeof(ScanKeyData));

		/*
		 * Extract the query vector from the first ORDER BY key.
		 * The sk_argument is the query vector datum.
		 */
		if (scan->numberOfOrderBys > 0 &&
			!scan->orderByData[0].sk_flags)
		{
			Datum		queryDatum = scan->orderByData[0].sk_argument;
			Vector	   *queryVec = DatumGetVector(queryDatum);
			int			strategy = scan->orderByData[0].sk_strategy;

			oldCtx = MemoryContextSwitchTo(so->scanCtx);

			so->queryVec = InitVector(queryVec->dim);
			memcpy(so->queryVec->x, queryVec->x,
				   sizeof(float) * queryVec->dim);
			so->strategy = strategy;

			MemoryContextSwitchTo(oldCtx);
		}
	}
}

/*
 * hnswgettuple - return the next tuple from the index scan.
 */
bool
hnswgettuple(IndexScanDesc scan, ScanDirection dir)
{
	HnswScanOpaque so = (HnswScanOpaque) scan->opaque;

	/*
	 * On the first call, perform the actual HNSW search and populate
	 * the result list.
	 */
	if (so->firstCall)
	{
		/* Must have a query vector */
		if (so->queryVec == NULL)
			return false;

		pgstat_count_index_scan(scan->indexRelation);

		hnsw_scan_search(scan);
		so->firstCall = false;
	}

	/* Return results one at a time */
	if (so->resultIdx < list_length(so->resultList))
	{
		HnswCandidate *cand;

		cand = (HnswCandidate *) list_nth(so->resultList, so->resultIdx);
		so->resultIdx++;

		/* Set the heap TID for the executor to fetch */
		scan->xs_heaptid = cand->element->heaptid;
		scan->xs_recheckorderby = false;

		/* Return the distance as the ORDER BY value */
		if (scan->xs_orderbyvals != NULL)
		{
			scan->xs_orderbyvals[0] = Float8GetDatum((double) cand->distance);
			scan->xs_orderbynulls[0] = false;
		}

		scan->xs_recheck = false;

		return true;
	}

	return false;
}

/*
 * hnswendscan - end an index scan.
 */
void
hnswendscan(IndexScanDesc scan)
{
	HnswScanOpaque so = (HnswScanOpaque) scan->opaque;

	if (so->scanCtx != NULL)
		MemoryContextDelete(so->scanCtx);

	pfree(so);
	scan->opaque = NULL;
}
