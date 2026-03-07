/*-------------------------------------------------------------------------
 *
 * hnswbuild.c
 *	  Build an HNSW index.
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * IDENTIFICATION
 *	  src/backend/access/hnsw/hnswbuild.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/generic_xlog.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "hnsw.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"

/*
 * Initialize the metapage for an HNSW index.
 */
static void
hnsw_init_metapage(Relation index, int m, int efConstruction, ForkNumber forkNum)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	HnswMetaPageData *metadata;

	buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);

	PageInit(page, BufferGetPageSize(buf), sizeof(HnswPageOpaqueData));

	metadata = (HnswMetaPageData *) PageGetContents(page);
	metadata->magicNumber = HNSW_MAGIC_NUMBER;
	metadata->entryBlkno = InvalidBlockNumber;
	metadata->entryOffset = InvalidOffsetNumber;
	metadata->entryLevel = 0;
	metadata->maxLevel = -1;
	metadata->elementCount = 0;
	metadata->m = m;
	metadata->efConstruction = efConstruction;
	metadata->dimensions = 0;

	/* Set pd_lower past the metadata */
	((PageHeader) page)->pd_lower =
		((char *) metadata + sizeof(HnswMetaPageData)) - (char *) page;

	/* Mark page as metapage */
	HnswPageGetOpaque(page)->nextblkno = InvalidBlockNumber;
	HnswPageGetOpaque(page)->flags = HNSW_PAGE_META;
	HnswPageGetOpaque(page)->unused = 0;

	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
}

/*
 * Add a new element to the in-memory HNSW graph during build.
 */
static void
hnsw_build_insert_element(HnswBuildState *buildstate, Vector *vec,
						  ItemPointer heaptid)
{
	HnswElement *element;
	int			level;
	int			i;
	MemoryContext oldCtx;

	oldCtx = MemoryContextSwitchTo(buildstate->buildCtx);

	/* Allocate element */
	element = (HnswElement *) palloc0(sizeof(HnswElement));
	element->dim = vec->dim;
	element->vec = (float *) palloc(sizeof(float) * vec->dim);
	memcpy(element->vec, vec->x, sizeof(float) * vec->dim);
	ItemPointerCopy(heaptid, &element->heaptid);

	/* Assign random level */
	level = hnsw_random_level(buildstate->ml);
	element->level = level;

	/* Allocate neighbor lists for each layer */
	element->neighbors = (List **) palloc0(sizeof(List *) * (level + 1));
	for (i = 0; i <= level; i++)
		element->neighbors[i] = NIL;

	/* First element becomes the entry point */
	if (buildstate->entryPoint == NULL)
	{
		buildstate->entryPoint = element;
		buildstate->maxLevel = level;
		buildstate->elements = lappend(buildstate->elements, element);
		buildstate->elementCount++;
		MemoryContextSwitchTo(oldCtx);
		return;
	}

	/*
	 * Insert into the graph using the HNSW algorithm.
	 *
	 * Phase 1: Traverse from top layer down to level+1, finding the closest
	 * element at each layer (ef=1 greedy search).
	 */
	{
		HnswElement *ep = buildstate->entryPoint;
		int			topLevel = buildstate->maxLevel;
		int			lc;
		Vector	   *queryVec = vec;

		/* Greedy descent from topLevel down to level+1 */
		for (lc = topLevel; lc > level; lc--)
		{
			List	   *nearest;

			nearest = hnsw_search_layer(ep, queryVec, 1, lc,
										HNSW_L2_STRATEGY,
										buildstate->elements);
			if (nearest != NIL)
			{
				HnswCandidate *best = (HnswCandidate *) linitial(nearest);

				ep = best->element;
			}
			list_free(nearest);
		}

		/*
		 * Phase 2: From min(level, topLevel) down to layer 0, do beam search
		 * with ef=efConstruction and connect to the M nearest neighbors.
		 */
		for (lc = Min(level, topLevel); lc >= 0; lc--)
		{
			List	   *candidates;
			int			maxNeighbors;
			ListCell   *cell;
			int			count;

			candidates = hnsw_search_layer(ep, queryVec,
										   buildstate->efConstruction,
										   lc, HNSW_L2_STRATEGY,
										   buildstate->elements);

			maxNeighbors = hnsw_get_max_neighbors(buildstate->m, lc);

			/* Connect element to its nearest neighbors at this layer */
			count = 0;
			foreach(cell, candidates)
			{
				HnswCandidate *cand = (HnswCandidate *) lfirst(cell);
				HnswCandidate *fwdLink;
				HnswCandidate *revLink;

				if (count >= maxNeighbors)
					break;

				/* Forward link: element -> neighbor */
				fwdLink = (HnswCandidate *) palloc(sizeof(HnswCandidate));
				fwdLink->element = cand->element;
				fwdLink->distance = cand->distance;
				memset(&fwdLink->ph_node, 0, sizeof(pairingheap_node));
				element->neighbors[lc] = lappend(element->neighbors[lc], fwdLink);

				/* Reverse link: neighbor -> element */
				if (cand->element->neighbors != NULL && lc <= cand->element->level)
				{
					revLink = (HnswCandidate *) palloc(sizeof(HnswCandidate));
					revLink->element = element;
					revLink->distance = cand->distance;
					memset(&revLink->ph_node, 0, sizeof(pairingheap_node));
					cand->element->neighbors[lc] =
						lappend(cand->element->neighbors[lc], revLink);

					/*
					 * If the neighbor now has too many connections, prune the
					 * farthest one.
					 */
					if (list_length(cand->element->neighbors[lc]) > maxNeighbors)
					{
						/* Find and remove the farthest neighbor */
						float		maxDist = -1.0f;
						HnswCandidate *worst = NULL;
						ListCell   *nc;

						foreach(nc, cand->element->neighbors[lc])
						{
							HnswCandidate *n = (HnswCandidate *) lfirst(nc);

							if (n->distance > maxDist)
							{
								maxDist = n->distance;
								worst = n;
							}
						}
						if (worst != NULL)
						{
							cand->element->neighbors[lc] =
								list_delete_ptr(cand->element->neighbors[lc], worst);
							pfree(worst);
						}
					}
				}

				count++;
			}

			/* Update entry point for next layer */
			if (candidates != NIL)
			{
				HnswCandidate *best = (HnswCandidate *) linitial(candidates);

				ep = best->element;
			}
			list_free(candidates);
		}

		/* Update entry point if new element has higher level */
		if (level > buildstate->maxLevel)
		{
			buildstate->entryPoint = element;
			buildstate->maxLevel = level;
		}
	}

	buildstate->elements = lappend(buildstate->elements, element);
	buildstate->elementCount++;

	MemoryContextSwitchTo(oldCtx);
}

/*
 * Write an element tuple to the index.
 *
 * Returns the block number and offset where it was stored.
 */
static void
hnsw_write_element(Relation index, HnswElement *element,
				   BlockNumber *outBlkno, OffsetNumber *outOffset)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	HnswElementTupleData etup;
	Size		etupSize;
	OffsetNumber off;
	BlockNumber blkno;

	etupSize = HNSW_ELEMENT_TUPLE_SIZE(element->dim);

	/* Initialize the tuple */
	memset(&etup, 0, offsetof(HnswElementTupleData, vec));
	ItemPointerCopy(&element->heaptid, &etup.heaptid);
	etup.level = element->level;
	etup.dim = element->dim;
	etup.neighborPage = InvalidBlockNumber;
	etup.neighborOffset = InvalidOffsetNumber;
	etup.neighborCount = 0;

	/*
	 * Try to find a page with enough space. We scan from HNSW_HEAD_BLKNO
	 * forward looking for element pages.
	 */
	blkno = HNSW_HEAD_BLKNO;

	for (;;)
	{
		/*
		 * Try to extend or read an existing page
		 */
		if (blkno >= RelationGetNumberOfBlocks(index))
		{
			/* Need a new page */
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
		else
		{
			buf = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
									 RBM_NORMAL, NULL);
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

			state = GenericXLogStart(index);
			page = GenericXLogRegisterBuffer(state, buf, 0);

			if ((HnswPageGetOpaque(page)->flags & HNSW_PAGE_ELEMENT) &&
				PageGetFreeSpace(page) >= etupSize + sizeof(ItemIdData))
			{
				break;
			}

			GenericXLogAbort(state);
			UnlockReleaseBuffer(buf);
			blkno++;
		}
	}

	/* We have a page with enough space; insert the element */
	{
		char	   *tupleData;

		tupleData = (char *) palloc(etupSize);
		memcpy(tupleData, &etup, offsetof(HnswElementTupleData, vec));
		memcpy(tupleData + offsetof(HnswElementTupleData, vec),
			   element->vec, sizeof(float) * element->dim);

		off = PageAddItem(page, (Item) tupleData, etupSize,
						  InvalidOffsetNumber, false, false);
		if (off == InvalidOffsetNumber)
			elog(ERROR, "failed to add element to HNSW index page");

		pfree(tupleData);
	}

	*outBlkno = BufferGetBlockNumber(buf);
	*outOffset = off;

	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);

	/* Store position back in the element */
	element->blkno = *outBlkno;
	element->offset = *outOffset;
}

/*
 * Write neighbor data for an element to the index.
 */
static void
hnsw_write_neighbors(Relation index, HnswElement *element)
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
				/* Allocate a new neighbor page */
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

			/*
			 * The neighbor page/offset information is stored in the
			 * on-disk element tuple, not the in-memory representation.
			 * We could update the element tuple on disk here, but for
			 * simplicity during build, we skip this and rely on scanning
			 * neighbor pages during search.
			 */
		}
	}
}

/*
 * Flush the in-memory HNSW graph to disk.
 */
static void
hnsw_flush_graph(Relation index, HnswBuildState *buildstate)
{
	ListCell   *lc;
	Buffer		metaBuf;
	Page		metaPage;
	GenericXLogState *state;
	HnswMetaPageData *metadata;

	/* Write all elements to disk */
	foreach(lc, buildstate->elements)
	{
		HnswElement *element = (HnswElement *) lfirst(lc);
		BlockNumber blkno;
		OffsetNumber offset;

		hnsw_write_element(index, element, &blkno, &offset);
	}

	/* Write all neighbor lists */
	foreach(lc, buildstate->elements)
	{
		HnswElement *element = (HnswElement *) lfirst(lc);

		hnsw_write_neighbors(index, element);
	}

	/* Update metapage */
	metaBuf = ReadBufferExtended(index, MAIN_FORKNUM, HNSW_METAPAGE_BLKNO,
								 RBM_NORMAL, NULL);
	LockBuffer(metaBuf, BUFFER_LOCK_EXCLUSIVE);

	state = GenericXLogStart(index);
	metaPage = GenericXLogRegisterBuffer(state, metaBuf, 0);
	metadata = (HnswMetaPageData *) PageGetContents(metaPage);

	if (buildstate->entryPoint != NULL)
	{
		metadata->entryBlkno = buildstate->entryPoint->blkno;
		metadata->entryOffset = buildstate->entryPoint->offset;
		metadata->entryLevel = buildstate->maxLevel;
	}
	metadata->maxLevel = buildstate->maxLevel;
	metadata->elementCount = buildstate->elementCount;
	metadata->dimensions = buildstate->dimensions;

	GenericXLogFinish(state);
	UnlockReleaseBuffer(metaBuf);
}

/*
 * Callback for table_index_build_scan.
 */
static void
hnsw_build_callback(Relation index, ItemPointer tid, Datum *values,
					bool *isnull, bool tupleIsAlive, void *state)
{
	HnswBuildState *buildstate = (HnswBuildState *) state;
	Vector	   *vec;
	MemoryContext oldCtx;

	/* Skip NULL vectors */
	if (isnull[0])
		return;

	vec = DatumGetVector(values[0]);

	/* Validate dimensions */
	if (vec->dim < 1 || vec->dim > HNSW_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("vector dimension %d is out of range (1-%d)",
						vec->dim, HNSW_MAX_DIM)));

	/* Set dimensions from first tuple */
	if (buildstate->dimensions == 0)
		buildstate->dimensions = vec->dim;
	else if (vec->dim != buildstate->dimensions)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("all vectors must have the same dimension, expected %d got %d",
						buildstate->dimensions, vec->dim)));

	/* Allow interrupts */
	CHECK_FOR_INTERRUPTS();

	/* Insert into the in-memory graph */
	oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);
	hnsw_build_insert_element(buildstate, vec, tid);
	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(buildstate->tmpCtx);
}

/*
 * hnswbuild - build an HNSW index.
 */
IndexBuildResult *
hnswbuild(Relation heap, Relation index, struct IndexInfo *indexInfo)
{
	IndexBuildResult *result;
	HnswBuildState buildstate;
	double		reltuples;
	int			m;
	int			efConstruction;

	/* Get build parameters from reloptions */
	m = HnswGetM(index);
	efConstruction = HnswGetEfConstruction(index);

	/* Initialize the metapage */
	hnsw_init_metapage(index, m, efConstruction, MAIN_FORKNUM);

	/* Set up build state */
	memset(&buildstate, 0, sizeof(HnswBuildState));
	buildstate.heap = heap;
	buildstate.index = index;
	buildstate.indexInfo = indexInfo;
	buildstate.m = m;
	buildstate.efConstruction = efConstruction;
	buildstate.dimensions = 0;
	buildstate.ml = 1.0 / log((double) m);
	buildstate.entryPoint = NULL;
	buildstate.maxLevel = -1;
	buildstate.elements = NIL;
	buildstate.elementCount = 0;
	buildstate.flushedCount = 0;

	/* Create memory contexts */
	buildstate.buildCtx = AllocSetContextCreate(CurrentMemoryContext,
												"HNSW build",
												ALLOCSET_DEFAULT_SIZES);
	buildstate.tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
											  "HNSW build temp",
											  ALLOCSET_DEFAULT_SIZES);

	/* Scan the heap and build the graph in memory */
	reltuples = table_index_build_scan(heap, index, indexInfo, true, true,
									   hnsw_build_callback,
									   (void *) &buildstate, NULL);

	/* Flush the in-memory graph to disk */
	hnsw_flush_graph(index, &buildstate);

	/* Clean up */
	MemoryContextDelete(buildstate.buildCtx);
	MemoryContextDelete(buildstate.tmpCtx);

	/* Return build results */
	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples = reltuples;
	result->index_tuples = buildstate.elementCount;

	return result;
}

/*
 * hnswbuildempty - build an empty HNSW index for an unlogged table.
 */
void
hnswbuildempty(Relation index)
{
	int		m = HnswGetM(index);
	int		efConstruction = HnswGetEfConstruction(index);

	hnsw_init_metapage(index, m, efConstruction, INIT_FORKNUM);
}
