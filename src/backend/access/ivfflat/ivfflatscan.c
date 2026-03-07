/*-------------------------------------------------------------------------
 *
 * ivfflatscan.c
 *	  Scan routines for the IVFFlat index access method.
 *
 * The scan follows the classic IVFFlat approach:
 *
 *   1. Identify the nprobes nearest centroids to the query vector.
 *   2. Exhaustively scan the posting lists of those centroids.
 *   3. Compute exact distances and return results in order using a
 *      min-heap (pairing heap).
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * IDENTIFICATION
 *	  src/backend/access/ivfflat/ivfflatscan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/relscan.h"
#include "lib/pairingheap.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"
#include "utils/vector.h"

#include "ivfflat.h"

/* ----------
 * Helper structs for centroid probing
 * ----------
 */
typedef struct IvfflatCentroidInfo
{
	int			list_idx;		/* centroid list index */
	double		distance;		/* distance from query to centroid */
	BlockNumber first_posting_blkno;	/* first posting list block */
} IvfflatCentroidInfo;

/*
 * Pairing heap comparator for scan results (min-heap by distance).
 *
 * Returns < 0 if a should come before b (i.e., a has smaller distance).
 */
static int
ivfflat_result_comparator(const pairingheap_node *a,
						  const pairingheap_node *b,
						  void *arg)
{
	const IvfflatScanEntry *ea = pairingheap_const_container(IvfflatScanEntry, ph_node, a);
	const IvfflatScanEntry *eb = pairingheap_const_container(IvfflatScanEntry, ph_node, b);

	/* Min-heap: smaller distance should come first, so return > 0 if a < b */
	if (ea->distance < eb->distance)
		return 1;
	else if (ea->distance > eb->distance)
		return -1;
	else
		return 0;
}

/*
 * Comparison function for sorting centroid info by distance (ascending).
 */
static int
ivfflat_centroid_cmp(const void *a, const void *b)
{
	const IvfflatCentroidInfo *ca = (const IvfflatCentroidInfo *) a;
	const IvfflatCentroidInfo *cb = (const IvfflatCentroidInfo *) b;

	if (ca->distance < cb->distance)
		return -1;
	else if (ca->distance > cb->distance)
		return 1;
	else
		return 0;
}

/*
 * Perform the actual scan: find nprobes nearest centroids, scan their
 * posting lists, and add all entries to the results heap.
 */
static void
ivfflat_do_scan(IndexScanDesc scan)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;
	Relation	index = scan->indexRelation;
	Buffer		meta_buf;
	Page		meta_page;
	IvfflatMetaPageData *metadata;
	int			num_lists;
	int			dim;
	BlockNumber first_centroid_blkno;
	IvfflatCentroidInfo *centroid_info;
	int			nprobes;
	Size		entry_size;
	Size		centry_size;
	int			list_idx;
	int			i;
	MemoryContext oldctx;

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
	{
		so->scan_done = true;
		return;
	}

	/* Verify dimensions */
	if (so->query->dim != dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("ivfflat index expects vectors with %d dimensions, query has %d",
						dim, so->query->dim)));

	so->dimensions = dim;
	entry_size = IVFFLAT_ENTRY_SIZE(dim);
	centry_size = MAXALIGN(IVFFLAT_LIST_SIZE(dim));

	/* Determine how many probes to do */
	nprobes = so->nprobes;
	if (nprobes > num_lists)
		nprobes = num_lists;

	/*
	 * Phase 1: Compute distance from query to every centroid and find the
	 * nprobes nearest ones.
	 */
	centroid_info = (IvfflatCentroidInfo *) palloc(sizeof(IvfflatCentroidInfo) * num_lists);
	list_idx = 0;

	{
		BlockNumber blkno = first_centroid_blkno;

		while (list_idx < num_lists)
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

				centroid_info[list_idx].list_idx = list_idx;
				centroid_info[list_idx].distance = so->distfunc(so->query, centroid);
				centroid_info[list_idx].first_posting_blkno = list->first_posting_blkno;

				ptr += centry_size;
				list_idx++;
			}

			UnlockReleaseBuffer(cbuf);
			blkno++;
		}
	}

	/* Sort centroids by distance, pick top nprobes */
	qsort(centroid_info, num_lists, sizeof(IvfflatCentroidInfo),
		  ivfflat_centroid_cmp);

	/*
	 * Phase 2: Scan posting lists of the nprobes nearest centroids.
	 * Add all entries to the results pairing heap.
	 */
	oldctx = MemoryContextSwitchTo(so->scan_ctx);

	for (i = 0; i < nprobes; i++)
	{
		BlockNumber posting_blkno = centroid_info[i].first_posting_blkno;

		while (BlockNumberIsValid(posting_blkno))
		{
			Buffer		pbuf;
			Page		ppage;
			char	   *ptr;
			char	   *end;

			CHECK_FOR_INTERRUPTS();

			pbuf = ReadBuffer(index, posting_blkno);
			LockBuffer(pbuf, BUFFER_LOCK_SHARE);
			ppage = BufferGetPage(pbuf);

			ptr = (char *) PageGetContents(ppage);
			end = (char *) ppage + ((PageHeader) ppage)->pd_lower;

			while (ptr + entry_size <= end)
			{
				IvfflatEntryData *entry = (IvfflatEntryData *) ptr;
				Vector	   *vec = IvfflatEntryGetVector(entry);
				double		dist;
				IvfflatScanEntry *result;

				dist = so->distfunc(so->query, vec);

				result = (IvfflatScanEntry *) palloc(sizeof(IvfflatScanEntry));
				result->heap_tid = entry->heap_tid;
				result->distance = dist;

				pairingheap_add(so->results, &result->ph_node);
				so->num_results++;

				ptr += entry_size;
			}

			posting_blkno = IvfflatPageGetOpaque(ppage)->nextblkno;
			UnlockReleaseBuffer(pbuf);
		}
	}

	MemoryContextSwitchTo(oldctx);

	pfree(centroid_info);

	so->scan_done = true;
}

/*
 * ivfflatbeginscan - start an IVFFlat index scan.
 */
IndexScanDesc
ivfflatbeginscan(Relation index, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	IvfflatScanOpaque so;
	MemoryContext scan_ctx;

	scan = RelationGetIndexScan(index, nkeys, norderbys);

	scan_ctx = AllocSetContextCreate(CurrentMemoryContext,
									 "IVFFlat scan",
									 ALLOCSET_DEFAULT_SIZES);

	so = (IvfflatScanOpaque) MemoryContextAllocZero(scan_ctx,
													sizeof(IvfflatScanOpaqueData));
	so->scan_ctx = scan_ctx;
	so->scan_done = false;
	so->num_results = 0;
	so->query = NULL;
	so->results = NULL;
	so->nprobes = ivfflat_probes;

	scan->opaque = so;

	return scan;
}

/*
 * ivfflatrescan - restart an IVFFlat index scan.
 */
void
ivfflatrescan(IndexScanDesc scan, ScanKey keys, int nkeys,
			  ScanKey orderbys, int norderbys)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;

	/* Reset scan state */
	so->scan_done = false;
	so->num_results = 0;

	/* Create a fresh results heap */
	so->results = pairingheap_allocate(ivfflat_result_comparator, NULL);

	/*
	 * Extract query vector and strategy from ORDER BY scan keys.
	 *
	 * For an ORDER BY scan, the orderbys array contains scan keys where
	 * sk_argument is the query vector datum.
	 */
	if (orderbys != NULL && norderbys > 0)
	{
		Datum		query_datum;
		Vector	   *query_vec;

		/* Copy the order by keys */
		memmove(scan->orderByData, orderbys, sizeof(ScanKeyData) * norderbys);

		query_datum = orderbys[0].sk_argument;

		if (DatumGetPointer(query_datum) == NULL)
		{
			so->query = NULL;
			return;
		}

		query_vec = DatumGetVector(query_datum);

		/* Copy the query vector into scan memory context */
		{
			MemoryContext oldctx = MemoryContextSwitchTo(so->scan_ctx);

			so->query = InitVector(query_vec->dim);
			memcpy(so->query->x, query_vec->x, sizeof(float) * query_vec->dim);

			MemoryContextSwitchTo(oldctx);
		}

		so->strategy = orderbys[0].sk_strategy;
		so->distfunc = ivfflat_get_distfunc(so->strategy);
	}
}

/*
 * ivfflatgettuple - return the next matching tuple from the index scan.
 */
bool
ivfflatgettuple(IndexScanDesc scan, ScanDirection dir)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;
	IvfflatScanEntry *entry;

	/* If no query, no results */
	if (so->query == NULL)
		return false;

	/* Perform the scan on first call */
	if (!so->scan_done)
	{
		pgstat_count_index_scan(scan->indexRelation);
		ivfflat_do_scan(scan);
	}

	/* Return next result from the min-heap */
	if (pairingheap_is_empty(so->results))
		return false;

	entry = pairingheap_container(IvfflatScanEntry, ph_node,
								  pairingheap_remove_first(so->results));

	scan->xs_heaptid = entry->heap_tid;
	scan->xs_recheckorderby = false;

	/* Return the distance as the order-by value */
	if (scan->numberOfOrderBys > 0)
	{
		/*
		 * For L2 distance strategy, we stored squared distance during scan
		 * for efficiency.  The actual distance needs to be returned for
		 * correct ORDER BY semantics.  However, since the ordering is
		 * preserved (sqrt is monotonic), we return the raw value from our
		 * distance function, which for L2 is already the squared distance.
		 *
		 * Note: the scan key strategy determines what distance function is
		 * used, and the planner expects the distance function to return
		 * the same type of value as the operator.
		 */
		scan->xs_orderbyvals[0] = Float8GetDatum(entry->distance);
		scan->xs_orderbynulls[0] = false;
	}

	return true;
}

/*
 * ivfflatendscan - end an IVFFlat index scan.
 */
void
ivfflatendscan(IndexScanDesc scan)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;

	if (so->scan_ctx)
		MemoryContextDelete(so->scan_ctx);

	scan->opaque = NULL;
}
