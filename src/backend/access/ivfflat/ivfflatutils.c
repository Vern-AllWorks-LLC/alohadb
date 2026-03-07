/*-------------------------------------------------------------------------
 *
 * ivfflatutils.c
 *	  IVFFlat index access method handler and utility functions.
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * IDENTIFICATION
 *	  src/backend/access/ivfflat/ivfflatutils.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "commands/vacuum.h"
#include "utils/guc.h"
#include "utils/vector.h"

#include "ivfflat.h"

/* GUC variable: number of probes during index scan */
int			ivfflat_probes = IVFFLAT_DEFAULT_PROBES;

/*
 * Initialize an IVFFlat index page.
 *
 * Sets up the page header and special space with the given page type.
 */
void
ivfflat_page_init(Page page, uint16 page_type)
{
	IvfflatPageOpaque opaque;

	PageInit(page, BLCKSZ, sizeof(IvfflatPageOpaqueData));

	/*
	 * Ensure pd_lower is aligned with PageGetContents() so that our
	 * sequential data writes start at the right offset.
	 */
	((PageHeader) page)->pd_lower = MAXALIGN(SizeOfPageHeaderData);

	opaque = IvfflatPageGetOpaque(page);
	opaque->nextblkno = InvalidBlockNumber;
	opaque->page_type = page_type;
	opaque->unused = 0;
}

/*
 * Return the appropriate distance function for the given strategy.
 */
IvfflatDistFunc
ivfflat_get_distfunc(StrategyNumber strategy)
{
	switch (strategy)
	{
		case IVFFLAT_L2_DISTANCE_STRATEGY:
			return vector_l2_squared_distance;
		case IVFFLAT_COSINE_DISTANCE_STRATEGY:
			return vector_cosine_distance;
		case IVFFLAT_IP_DISTANCE_STRATEGY:
			return vector_inner_product;
		default:
			elog(ERROR, "unrecognized ivfflat strategy number: %d", strategy);
			return NULL;		/* keep compiler quiet */
	}
}

/*
 * Compute distance between two vectors using the given strategy.
 */
double
ivfflat_get_distance(StrategyNumber strategy, const Vector *a, const Vector *b)
{
	IvfflatDistFunc func = ivfflat_get_distfunc(strategy);

	return func(a, b);
}

/*
 * ivfflathandler - index access method handler function
 *
 * Returns an IndexAmRoutine struct describing the IVFFlat access method.
 */
Datum
ivfflathandler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = IVFFLAT_NUM_STRATEGIES;
	amroutine->amsupport = IVFFLAT_NUM_SUPPORT;
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
	amroutine->amstorage = false;
	amroutine->amclusterable = false;
	amroutine->ampredlocks = false;
	amroutine->amcanparallel = true;
	amroutine->amcanbuildparallel = false;
	amroutine->amcaninclude = false;
	amroutine->amusemaintenanceworkmem = true;
	amroutine->amsummarizing = false;
	amroutine->amparallelvacuumoptions =
		VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_CLEANUP;
	amroutine->amkeytype = InvalidOid;

	/* Interface functions */
	amroutine->ambuild = ivfflatbuild;
	amroutine->ambuildempty = ivfflatbuildempty;
	amroutine->aminsert = ivfflatinsert;
	amroutine->aminsertcleanup = NULL;
	amroutine->ambulkdelete = ivfflatbulkdelete;
	amroutine->amvacuumcleanup = ivfflatvacuumcleanup;
	amroutine->amcanreturn = NULL;
	amroutine->amcostestimate = ivfflatcostestimate;
	amroutine->amgettreeheight = NULL;
	amroutine->amoptions = ivfflatoptions;
	amroutine->amproperty = NULL;
	amroutine->ambuildphasename = NULL;
	amroutine->amvalidate = ivfflatvalidate;
	amroutine->amadjustmembers = NULL;
	amroutine->ambeginscan = ivfflatbeginscan;
	amroutine->amrescan = ivfflatrescan;
	amroutine->amgettuple = ivfflatgettuple;
	amroutine->amgetbitmap = NULL;
	amroutine->amendscan = ivfflatendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;
	amroutine->amtranslatestrategy = NULL;
	amroutine->amtranslatecmptype = NULL;

	PG_RETURN_POINTER(amroutine);
}
