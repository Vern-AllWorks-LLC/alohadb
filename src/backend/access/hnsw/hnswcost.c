/*-------------------------------------------------------------------------
 *
 * hnswcost.c
 *	  Cost estimation for HNSW index scans.
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * IDENTIFICATION
 *	  src/backend/access/hnsw/hnswcost.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "hnsw.h"
#include "nodes/pathnodes.h"
#include "optimizer/optimizer.h"
#include "utils/spccache.h"

/*
 * hnswcostestimate - estimate the cost of an HNSW index scan.
 *
 * The cost model for HNSW is:
 *
 *   indexStartupCost: cost of traversing from top layer to layer 1.
 *     This involves log2(num_elements) steps of single-element greedy search,
 *     each requiring one random page access.
 *
 *   indexTotalCost: startup cost + cost of searching at layer 0 with ef_search.
 *     At layer 0, we visit approximately ef_search elements, each requiring
 *     a CPU index tuple cost for distance computation.
 *
 * This is a simplified model that captures the key characteristics of
 * HNSW search: logarithmic layer traversal + linear ef_search at the bottom.
 */
void
hnswcostestimate(struct PlannerInfo *root,
				 struct IndexPath *path,
				 double loop_count,
				 Cost *indexStartupCost,
				 Cost *indexTotalCost,
				 Selectivity *indexSelectivity,
				 double *indexCorrelation,
				 double *indexPages)
{
	IndexOptInfo *index = path->indexinfo;
	double		numIndexTuples = index->tuples;
	double		numIndexPages = index->pages;
	double		spc_random_page_cost;
	double		layerTraversal;
	int			efSearch = hnsw_ef_search;

	/*
	 * Get the tablespace-specific random page cost.
	 */
	get_tablespace_page_costs(index->reltablespace,
							  &spc_random_page_cost, NULL);

	/*
	 * Estimate the number of layers to traverse.  In an HNSW graph with
	 * N elements and parameter M, the expected number of layers is
	 * approximately log_M(N), which equals log2(N) / log2(M).
	 */
	if (numIndexTuples > 1.0)
		layerTraversal = log2(numIndexTuples);
	else
		layerTraversal = 1.0;

	/*
	 * Startup cost: traversing from the top layer down to layer 1.
	 * At each layer, we do a greedy search with ef=1, which typically
	 * involves a few random page accesses.
	 */
	*indexStartupCost = layerTraversal * spc_random_page_cost;

	/*
	 * Total cost: startup + search at layer 0 with ef_search candidates.
	 * Each candidate requires a distance computation (CPU cost) and
	 * potentially a page access.
	 */
	*indexTotalCost = *indexStartupCost +
		(double) efSearch * cpu_index_tuple_cost;

	/*
	 * For ORDER BY scans, selectivity represents the fraction of tuples
	 * we expect to return.  Since HNSW is typically used with LIMIT,
	 * we estimate a small selectivity.  The actual number of tuples
	 * returned is controlled by the LIMIT clause, not the index.
	 */
	if (numIndexTuples > 0.0)
		*indexSelectivity = (double) efSearch / numIndexTuples;
	else
		*indexSelectivity = 1.0;

	if (*indexSelectivity > 1.0)
		*indexSelectivity = 1.0;

	/*
	 * HNSW does not maintain any correlation with physical heap order.
	 */
	*indexCorrelation = 0.0;

	/*
	 * Estimate the number of index pages that will be accessed.
	 * This is roughly proportional to the number of elements visited
	 * during search.
	 */
	if (numIndexPages > 0.0 && numIndexTuples > 0.0)
		*indexPages = ceil(((double) efSearch / numIndexTuples) * numIndexPages);
	else
		*indexPages = 1.0;

	if (*indexPages < 1.0)
		*indexPages = 1.0;
}
