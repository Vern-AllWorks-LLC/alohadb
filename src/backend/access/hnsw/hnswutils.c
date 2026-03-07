/*-------------------------------------------------------------------------
 *
 * hnswutils.c
 *	  HNSW index access method handler and utility functions.
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * IDENTIFICATION
 *	  src/backend/access/hnsw/hnswutils.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/amapi.h"
#include "commands/vacuum.h"
#include "common/pg_prng.h"
#include "hnsw.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"

/* GUC variable */
int			hnsw_ef_search = HNSW_DEFAULT_EF_SEARCH;

/* Whether GUCs have been registered */
static bool hnsw_gucs_registered = false;

/*
 * hnsw_register_gucs - register HNSW GUC variables.
 *
 * This is called from the handler function on first invocation.
 */
static void
hnsw_register_gucs(void)
{
	if (hnsw_gucs_registered)
		return;

	DefineCustomIntVariable("hnsw.ef_search",
							"Sets the size of the dynamic candidate list for search.",
							NULL,
							&hnsw_ef_search,
							HNSW_DEFAULT_EF_SEARCH,
							1,
							1000,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	MarkGUCPrefixReserved("hnsw");

	hnsw_gucs_registered = true;
}

/*
 * hnswhandler -- handler function for the HNSW index access method.
 *
 * Returns an IndexAmRoutine describing the HNSW AM's capabilities and
 * callback functions.
 */
Datum
hnswhandler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	/* Register GUC variables on first call */
	hnsw_register_gucs();

	amroutine->amstrategies = 0;
	amroutine->amsupport = HNSW_NUM_PROCS;
	amroutine->amoptsprocnum = 0;
	amroutine->amcanorder = false;
	amroutine->amcanorderbyop = true;
	amroutine->amcanhash = false;
	amroutine->amconsistentequality = false;
	amroutine->amconsistentordering = false;
	amroutine->amcanbackward = false;
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = false;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = false;
	amroutine->amstorage = true;
	amroutine->amclusterable = false;
	amroutine->ampredlocks = false;
	amroutine->amcanparallel = true;
	amroutine->amcanbuildparallel = true;
	amroutine->amcaninclude = false;
	amroutine->amusemaintenanceworkmem = true;
	amroutine->amsummarizing = false;
	amroutine->amparallelvacuumoptions =
		VACUUM_OPTION_PARALLEL_BULKDEL;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = hnswbuild;
	amroutine->ambuildempty = hnswbuildempty;
	amroutine->aminsert = hnswinsert;
	amroutine->aminsertcleanup = NULL;
	amroutine->ambulkdelete = hnswbulkdelete;
	amroutine->amvacuumcleanup = hnswvacuumcleanup;
	amroutine->amcanreturn = NULL;
	amroutine->amcostestimate = hnswcostestimate;
	amroutine->amgettreeheight = NULL;
	amroutine->amoptions = hnswoptions;
	amroutine->amproperty = NULL;
	amroutine->ambuildphasename = NULL;
	amroutine->amvalidate = hnswvalidate;
	amroutine->amadjustmembers = NULL;
	amroutine->ambeginscan = hnswbeginscan;
	amroutine->amrescan = hnswrescan;
	amroutine->amgettuple = hnswgettuple;
	amroutine->amgetbitmap = NULL;
	amroutine->amendscan = hnswendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;
	amroutine->amtranslatestrategy = NULL;
	amroutine->amtranslatecmptype = NULL;

	PG_RETURN_POINTER(amroutine);
}

/*
 * _PG_init - module initialization for shared library loading.
 *
 * When compiled as a built-in AM, GUCs are registered via the handler.
 * This function is provided for the case where the module is loaded
 * as a shared library.
 */
void
_PG_init(void)
{
	hnsw_register_gucs();
}

/*
 * hnsw_get_distance_func - return the C-level distance function for a strategy.
 */
HnswDistanceFunc
hnsw_get_distance_func(int strategy)
{
	switch (strategy)
	{
		case HNSW_L2_STRATEGY:
			return vector_l2_distance;
		case HNSW_COSINE_STRATEGY:
			return vector_cosine_distance;
		case HNSW_IP_STRATEGY:
			return vector_inner_product;
		default:
			elog(ERROR, "unsupported HNSW distance strategy: %d", strategy);
			return NULL;		/* keep compiler quiet */
	}
}

/*
 * hnsw_calc_distance - compute the distance between two vectors using the
 * specified strategy.
 */
float
hnsw_calc_distance(const Vector *a, const Vector *b, int strategy)
{
	HnswDistanceFunc fn = hnsw_get_distance_func(strategy);

	return (float) fn(a, b);
}

/*
 * hnsw_random_level - generate a random level for a new element.
 *
 * Uses the formula from the HNSW paper:
 *   level = floor(-log(uniform_random) * ml)
 * where ml = 1 / ln(M).
 *
 * The result is clamped to HNSW_MAX_LEVEL.
 */
int
hnsw_random_level(double ml)
{
	double		r = pg_prng_double(&pg_global_prng_state);
	int			level;

	/* Avoid log(0) */
	if (r <= 0.0)
		r = 1e-10;

	level = (int) floor(-log(r) * ml);
	if (level > HNSW_MAX_LEVEL)
		level = HNSW_MAX_LEVEL;
	return level;
}

/*
 * hnsw_get_max_neighbors - return the maximum number of neighbors for a
 * given layer.  Layer 0 gets 2*M connections; all other layers get M.
 */
int
hnsw_get_max_neighbors(int m, int layer)
{
	return (layer == 0) ? 2 * m : m;
}

/*
 * Comparator for the min-heap of HnswCandidate (smallest distance first).
 */
static int
hnsw_candidate_min_cmp(const pairingheap_node *a, const pairingheap_node *b,
					   void *arg)
{
	const HnswCandidate *ca = pairingheap_const_container(HnswCandidate, ph_node, a);
	const HnswCandidate *cb = pairingheap_const_container(HnswCandidate, ph_node, b);

	/* For a min-heap, return > 0 when a < b */
	if (ca->distance < cb->distance)
		return 1;
	if (ca->distance > cb->distance)
		return -1;
	return 0;
}

/*
 * Comparator for the max-heap (largest distance first), used as the
 * "worst candidate" heap during search.
 */
static int
hnsw_candidate_max_cmp(const pairingheap_node *a, const pairingheap_node *b,
					   void *arg)
{
	const HnswCandidate *ca = pairingheap_const_container(HnswCandidate, ph_node, a);
	const HnswCandidate *cb = pairingheap_const_container(HnswCandidate, ph_node, b);

	/* For a max-heap, return > 0 when a > b */
	if (ca->distance > cb->distance)
		return 1;
	if (ca->distance < cb->distance)
		return -1;
	return 0;
}

/*
 * Convert an HnswElement to a Vector for distance computation.
 * The caller must ensure the returned Vector is freed.
 */
static Vector *
hnsw_element_to_vector(HnswElement *element)
{
	Vector	   *v;

	v = InitVector(element->dim);
	memcpy(v->x, element->vec, sizeof(float) * element->dim);
	return v;
}

/*
 * hnsw_search_layer - greedy beam search at a single layer.
 *
 * Starting from entryPoint, find the ef nearest neighbors to the query
 * vector at the given layer.  Returns a list of HnswCandidate* sorted
 * by ascending distance.
 *
 * The 'elements' list is the full set of elements in the graph, used only
 * during build.  During scan, we read from disk instead.
 */
List *
hnsw_search_layer(HnswElement *entryPoint, const Vector *query,
				  int efSearch, int layer, int strategy,
				  List *elements)
{
	pairingheap *candidates;	/* min-heap: next candidate to explore */
	pairingheap *working;		/* max-heap: worst of current ef-best */
	List	   *visited_list = NIL;
	List	   *result = NIL;
	HnswCandidate *entryCandidate;
	Vector	   *entryVec;
	float		entryDist;
	float		worstDist;
	ListCell   *lc;

	/* Compute distance to entry point */
	entryVec = hnsw_element_to_vector(entryPoint);
	entryDist = hnsw_calc_distance(query, entryVec, strategy);
	pfree(entryVec);

	/* Initialize heaps */
	candidates = pairingheap_allocate(hnsw_candidate_min_cmp, NULL);
	working = pairingheap_allocate(hnsw_candidate_max_cmp, NULL);

	entryCandidate = (HnswCandidate *) palloc(sizeof(HnswCandidate));
	entryCandidate->element = entryPoint;
	entryCandidate->distance = entryDist;
	memset(&entryCandidate->ph_node, 0, sizeof(pairingheap_node));

	pairingheap_add(candidates, &entryCandidate->ph_node);

	/* Also add to working set - need separate node */
	{
		HnswCandidate *workCand = (HnswCandidate *) palloc(sizeof(HnswCandidate));

		workCand->element = entryPoint;
		workCand->distance = entryDist;
		memset(&workCand->ph_node, 0, sizeof(pairingheap_node));
		pairingheap_add(working, &workCand->ph_node);
	}

	visited_list = lappend(visited_list, entryPoint);
	worstDist = entryDist;

	while (!pairingheap_is_empty(candidates))
	{
		HnswCandidate *current;
		pairingheap_node *node;
		List	   *neighborList;

		node = pairingheap_remove_first(candidates);
		current = pairingheap_container(HnswCandidate, ph_node, node);

		/* If closest candidate is farther than worst in working set, stop */
		if (current->distance > worstDist)
			break;

		/* Get neighbors at this layer */
		if (current->element->neighbors == NULL ||
			layer >= current->element->level + 1)
		{
			continue;
		}
		neighborList = current->element->neighbors[layer];

		foreach(lc, neighborList)
		{
			HnswCandidate *nc = (HnswCandidate *) lfirst(lc);
			HnswElement *neighbor = nc->element;
			bool		alreadyVisited = false;
			ListCell   *vc;
			Vector	   *nvec;
			float		ndist;

			/* Check if already visited */
			foreach(vc, visited_list)
			{
				if (lfirst(vc) == neighbor)
				{
					alreadyVisited = true;
					break;
				}
			}
			if (alreadyVisited)
				continue;

			visited_list = lappend(visited_list, neighbor);

			nvec = hnsw_element_to_vector(neighbor);
			ndist = hnsw_calc_distance(query, nvec, strategy);
			pfree(nvec);

			/*
			 * Add to candidates and working set if this neighbor is closer
			 * than the current worst, or if we haven't collected enough
			 * candidates yet.
			 */
			if (ndist < worstDist || list_length(visited_list) <= efSearch)
			{
				HnswCandidate *candNew;
				HnswCandidate *workNew;

				candNew = (HnswCandidate *) palloc(sizeof(HnswCandidate));
				candNew->element = neighbor;
				candNew->distance = ndist;
				memset(&candNew->ph_node, 0, sizeof(pairingheap_node));
				pairingheap_add(candidates, &candNew->ph_node);

				workNew = (HnswCandidate *) palloc(sizeof(HnswCandidate));
				workNew->element = neighbor;
				workNew->distance = ndist;
				memset(&workNew->ph_node, 0, sizeof(pairingheap_node));
				pairingheap_add(working, &workNew->ph_node);

				/* Update worst distance from the max-heap root */
				{
					HnswCandidate *worst;

					worst = pairingheap_container(HnswCandidate, ph_node,
												  pairingheap_first(working));
					worstDist = worst->distance;
				}
			}
		}
	}

	/* Extract results from the working (max-heap) set */
	{
		List	   *tmpList = NIL;

		while (!pairingheap_is_empty(working))
		{
			pairingheap_node *node = pairingheap_remove_first(working);
			HnswCandidate *c = pairingheap_container(HnswCandidate, ph_node, node);

			tmpList = lappend(tmpList, c);
		}

		/* tmpList is in descending distance order; reverse for ascending */
		result = NIL;
		for (int i = list_length(tmpList) - 1; i >= 0; i--)
		{
			result = lappend(result, list_nth(tmpList, i));
		}

		/* Keep only the ef-nearest */
		while (list_length(result) > efSearch)
		{
			result = list_delete_last(result);
		}

		list_free(tmpList);
	}

	pairingheap_free(candidates);
	pairingheap_free(working);
	list_free(visited_list);

	return result;
}
