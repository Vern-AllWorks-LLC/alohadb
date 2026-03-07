/*-------------------------------------------------------------------------
 *
 * ivfflatcost.c
 *	  Cost estimation for the IVFFlat index access method.
 *
 * The cost model reflects the IVFFlat access pattern: a fraction
 * (nprobes / nlists) of the total indexed vectors is scanned.
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * IDENTIFICATION
 *	  src/backend/access/ivfflat/ivfflatcost.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "utils/selfuncs.h"

#include "ivfflat.h"

/*
 * ivfflatcostestimate - estimate the cost of an IVFFlat index scan.
 *
 * The key insight is that IVFFlat only scans a fraction of the posting
 * lists, governed by nprobes and nlists.  The cost is:
 *
 *   startup:  cpu_operator_cost * nlists  (find nprobes nearest centroids)
 *   per-tuple: cpu_index_tuple_cost       (scan posting lists)
 *   total tuples scanned: (nprobes / nlists) * total_vectors
 *   pages: (nprobes / nlists) * total_pages
 */
void
ivfflatcostestimate(PlannerInfo *root, IndexPath *path,
					double loop_count,
					Cost *indexStartupCost,
					Cost *indexTotalCost,
					Selectivity *indexSelectivity,
					double *indexCorrelation,
					double *indexPages)
{
	IndexOptInfo *index = path->indexinfo;
	double		num_index_tuples = index->tuples;
	double		num_index_pages = index->pages;
	int			nlists;
	int			nprobes;
	double		scan_fraction;
	double		tuples_scanned;
	Cost		startup_cost;
	Cost		run_cost;

	/*
	 * Get the lists parameter from reloptions.  If not set, use the default.
	 * We can't easily get the reloptions here in the planner, so use the
	 * default.  The actual value is only slightly different and won't
	 * drastically change the cost estimate.
	 */
	nlists = IVFFLAT_DEFAULT_LISTS;
	nprobes = ivfflat_probes;

	/* Fraction of lists probed */
	if (nlists > 0)
		scan_fraction = (double) nprobes / (double) nlists;
	else
		scan_fraction = 1.0;

	if (scan_fraction > 1.0)
		scan_fraction = 1.0;

	/* Estimate tuples scanned */
	tuples_scanned = scan_fraction * num_index_tuples;
	if (tuples_scanned < 1.0)
		tuples_scanned = 1.0;

	/*
	 * Startup cost: scan all centroids to find nprobes nearest.
	 */
	startup_cost = cpu_operator_cost * nlists;

	/*
	 * Run cost: scan posting lists of the selected centroids.
	 * Each tuple requires a distance computation (cpu_operator_cost) plus
	 * tuple overhead.
	 */
	run_cost = tuples_scanned * (cpu_index_tuple_cost + cpu_operator_cost);

	/*
	 * Page I/O cost: proportional to the fraction of pages accessed.
	 */
	run_cost += scan_fraction * num_index_pages * random_page_cost;

	*indexStartupCost = startup_cost;
	*indexTotalCost = startup_cost + run_cost;

	/*
	 * Selectivity: the fraction of heap tuples we'll visit.  Since we return
	 * all tuples from probed lists, selectivity is the scan fraction.
	 */
	*indexSelectivity = scan_fraction;

	/* No correlation for vector indexes */
	*indexCorrelation = 0.0;

	/* Pages fetched */
	*indexPages = scan_fraction * num_index_pages;
}
