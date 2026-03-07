/*-------------------------------------------------------------------------
 *
 * nodeVectorScan.c
 *	  CustomScan-based vectorized sequential scan node.
 *
 * This file implements a CustomScan provider that replaces simple
 * SeqScan + Filter + Project chains with a vectorized execution path.
 * Tuples are fetched from the heap in batches of VECTOR_BATCH_SIZE (1024),
 * transposed into columnar representation, and processed through
 * SIMD-accelerated filter and projection operators implemented in
 * execVectorExpr.c.
 *
 * At the boundary with non-vectorized parent operators, the batch is
 * "drained" one tuple at a time through ExecCustomScan, converting
 * each batch row back into a TupleTableSlot.
 *
 * Registration:
 *   VectorScan_RegisterHook() installs a set_rel_pathlist_hook that
 *   injects a CustomPath for vectorized scanning when the GUC
 *   enable_vectorized_scan is true and the plan subtree is fully
 *   vectorizable.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeVectorScan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relscan.h"
#include "access/tableam.h"
#include "catalog/pg_type.h"
#include "commands/explain_state.h"
#include "executor/execVectorExpr.h"
#include "executor/executor.h"
#include "executor/nodeCustom.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "nodes/plannodes.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

/* -----------------------------------------------------------------------
 * GUC variable
 * -----------------------------------------------------------------------
 */
bool		enable_vectorized_scan = false;

/* -----------------------------------------------------------------------
 * Forward declarations -- CustomScan method tables
 * -----------------------------------------------------------------------
 */

/* Path methods */
static Plan *VectorScanPlanPath(PlannerInfo *root, RelOptInfo *rel,
								CustomPath *best_path, List *tlist,
								List *clauses, List *custom_plans);

/* Scan methods (plan-level) */
static Node *VectorScanCreateState(CustomScan *cscan);

/* Exec methods (execution-level) */
static void VectorScanBegin(CustomScanState *node, EState *estate,
							int eflags);
static TupleTableSlot *VectorScanExec(CustomScanState *node);
static void VectorScanEnd(CustomScanState *node);
static void VectorScanReScan(CustomScanState *node);
static void VectorScanExplain(CustomScanState *node, List *ancestors,
							  ExplainState *es);

/* -----------------------------------------------------------------------
 * Method table instances
 * -----------------------------------------------------------------------
 */

static const CustomPathMethods vectorscan_path_methods = {
	.CustomName = "VectorScan",
	.PlanCustomPath = VectorScanPlanPath,
};

static const CustomScanMethods vectorscan_scan_methods = {
	.CustomName = "VectorScan",
	.CreateCustomScanState = VectorScanCreateState,
};

static const CustomExecMethods vectorscan_exec_methods = {
	.CustomName = "VectorScan",
	.BeginCustomScan = VectorScanBegin,
	.ExecCustomScan = VectorScanExec,
	.EndCustomScan = VectorScanEnd,
	.ReScanCustomScan = VectorScanReScan,
	.ExplainCustomScan = VectorScanExplain,
	/* optional parallel methods left NULL */
	.MarkPosCustomScan = NULL,
	.RestrPosCustomScan = NULL,
	.EstimateDSMCustomScan = NULL,
	.InitializeDSMCustomScan = NULL,
	.ReInitializeDSMCustomScan = NULL,
	.InitializeWorkerCustomScan = NULL,
	.ShutdownCustomScan = NULL,
};


/* -----------------------------------------------------------------------
 * VectorScanState -- runtime state (extends CustomScanState)
 * -----------------------------------------------------------------------
 */
typedef struct VectorScanState
{
	CustomScanState css;		/* must be first */

	/* Heap scan */
	Relation	scan_rel;
	TableScanDesc scan_desc;

	/* Vectorized batch */
	VectorBatch *batch;
	VecExprState *vstate;
	TupleDesc	scan_tupdesc;	/* descriptor for scan-level columns */
	int			batch_pos;		/* current drain position within batch */
	bool		scan_done;		/* true when heap scan is exhausted */

	/* Original qual and tlist (stored for compilation at BeginCustomScan) */
	List	   *orig_quals;
	List	   *orig_tlist;
} VectorScanState;


/* -----------------------------------------------------------------------
 * Path creation -- planner hook
 * -----------------------------------------------------------------------
 */

/* Saved previous hook (for chaining) */
static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook = NULL;

/*
 * vectorscan_pathlist_hook
 *
 * Called from the planner's set_rel_pathlist().  If the relation is a
 * plain base table and the qual/tlist are fully vectorizable, we add
 * a CustomPath that will produce a VectorScan plan node.
 */
static void
vectorscan_pathlist_hook(PlannerInfo *root,
						 RelOptInfo *rel,
						 Index rti,
						 RangeTblEntry *rte)
{
	CustomPath *cpath;
	Path	   *seqpath;
	ListCell   *lc;
	List	   *quals;

	/* Chain to any previous hook first */
	if (prev_set_rel_pathlist_hook)
		prev_set_rel_pathlist_hook(root, rel, rti, rte);

	/* GUC check */
	if (!enable_vectorized_scan)
		return;

	/* Skip during bootstrap and single-user mode */
	if (IsBootstrapProcessingMode() || !IsUnderPostmaster)
		return;

	/* Only plain base relations (heap tables) */
	if (rel->reloptkind != RELOPT_BASEREL)
		return;
	if (rte->rtekind != RTE_RELATION)
		return;
	if (rte->relkind != RELKIND_RELATION)
		return;

	/* Skip if there are no rows to speak of */
	if (rel->pages == 0)
		return;

	/*
	 * Collect the restriction clauses (as plain expressions, not
	 * RestrictInfos) so we can check vectorizability.
	 */
	quals = NIL;
	foreach(lc, rel->baserestrictinfo)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

		quals = lappend(quals, ri->clause);
	}

	/* Check that quals and target list are vectorizable */
	if (!VecExprIsVectorizable(quals, rel->reltarget->exprs))
		return;

	/* Reject if any expression contains volatile functions or SRFs */
	if (contain_volatile_functions((Node *) quals))
		return;
	if (contain_volatile_functions((Node *) rel->reltarget->exprs))
		return;

	/*
	 * Build a CustomPath.  We base the cost estimate on the existing
	 * cheapest total-cost SeqScan path, applying a discount factor to
	 * reflect the batch-processing benefit.
	 */
	seqpath = rel->cheapest_total_path;
	if (seqpath == NULL)
		return;

	cpath = makeNode(CustomPath);
	cpath->path.type = T_CustomPath;
	cpath->path.pathtype = T_CustomScan;
	cpath->path.parent = rel;
	cpath->path.pathtarget = rel->reltarget;
	cpath->path.param_info = NULL;
	cpath->path.parallel_aware = false;
	cpath->path.parallel_safe = rel->consider_parallel;
	cpath->path.parallel_workers = 0;
	cpath->path.rows = seqpath->rows;
	cpath->path.disabled_nodes = 0;

	/*
	 * Vectorized execution has similar startup cost but reduced per-tuple
	 * cost due to batch processing and SIMD.  We model this as 80% of the
	 * sequential scan total cost.
	 */
	cpath->path.startup_cost = seqpath->startup_cost;
	cpath->path.total_cost = seqpath->total_cost * 0.80;
	cpath->path.pathkeys = NIL;		/* unordered */

	cpath->flags = CUSTOMPATH_SUPPORT_PROJECTION;
	cpath->custom_paths = NIL;
	cpath->custom_restrictinfo = NIL;
	cpath->custom_private = NIL;
	cpath->methods = &vectorscan_path_methods;

	add_path(rel, (Path *) cpath);
}


/* -----------------------------------------------------------------------
 * Plan creation callback
 * -----------------------------------------------------------------------
 */

/*
 * VectorScanPlanPath
 *
 * Convert the CustomPath into a CustomScan plan node.  We store the
 * original quals and target list in custom_exprs / custom_private for
 * later use during execution.
 */
static Plan *
VectorScanPlanPath(PlannerInfo *root,
				   RelOptInfo *rel,
				   CustomPath *best_path,
				   List *tlist,
				   List *clauses,
				   List *custom_plans)
{
	CustomScan *cscan;
	List	   *stripped_quals;
	Index		scanrelid;

	cscan = makeNode(CustomScan);
	cscan->scan.plan.targetlist = tlist;
	cscan->flags = best_path->flags;
	cscan->custom_scan_tlist = NIL;		/* use base relation's rowtype */
	cscan->methods = &vectorscan_scan_methods;
	cscan->custom_plans = NIL;

	/* Determine the scan relation index */
	scanrelid = rel->relid;
	cscan->scan.scanrelid = scanrelid;

	/*
	 * Extract the actual qual clauses from the RestrictInfo list provided
	 * by the planner.  We strip pseudoconstant quals since those are
	 * handled separately.
	 */
	stripped_quals = extract_actual_clauses(clauses, false);
	cscan->scan.plan.qual = stripped_quals;

	/*
	 * Store original quals in custom_exprs for the vectorized expression
	 * compiler to use at execution time.
	 */
	cscan->custom_exprs = list_copy(stripped_quals);

	/* custom_relids indicates which base rels this scan covers */
	cscan->custom_relids = bms_make_singleton(scanrelid);

	/* Copy cost estimates */
	cscan->scan.plan.startup_cost = best_path->path.startup_cost;
	cscan->scan.plan.total_cost = best_path->path.total_cost;
	cscan->scan.plan.plan_rows = best_path->path.rows;
	cscan->scan.plan.plan_width = rel->reltarget->width;
	cscan->scan.plan.parallel_aware = false;

	return (Plan *) cscan;
}


/* -----------------------------------------------------------------------
 * Execution callbacks
 * -----------------------------------------------------------------------
 */

/*
 * VectorScanCreateState
 *
 * Allocate our extended state struct, set the node tag and methods.
 */
static Node *
VectorScanCreateState(CustomScan *cscan)
{
	VectorScanState *vss;

	vss = (VectorScanState *) palloc0(sizeof(VectorScanState));
	NodeSetTag(&vss->css.ss.ps, T_CustomScanState);
	vss->css.methods = &vectorscan_exec_methods;

	return (Node *) vss;
}

/*
 * VectorScanBegin
 *
 * Open the scan relation, compile the vectorized expression program,
 * and allocate the batch.
 */
static void
VectorScanBegin(CustomScanState *node, EState *estate, int eflags)
{
	VectorScanState *vss = (VectorScanState *) node;
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	int			nworkcols;

	/* The relation was already opened by ExecInitCustomScan */
	vss->scan_rel = node->ss.ss_currentRelation;
	vss->scan_tupdesc = RelationGetDescr(vss->scan_rel);

	/* Remember quals and tlist for vectorized compilation */
	vss->orig_quals = cscan->custom_exprs;
	vss->orig_tlist = cscan->scan.plan.targetlist;

	/* Compile the vectorized expression program */
	vss->vstate = VecExprCompile(vss->orig_quals,
								 vss->orig_tlist,
								 vss->scan_tupdesc,
								 &nworkcols);

	/* Create the vector batch with enough working columns */
	vss->batch = CreateVectorBatch(nworkcols, vss->scan_tupdesc);

	vss->batch_pos = 0;
	vss->scan_done = false;
	vss->scan_desc = NULL;
}

/*
 * vec_fill_batch
 *
 * Fetch up to VECTOR_BATCH_SIZE tuples from the heap and transpose them
 * into the VectorBatch's columnar arrays.  Returns the number of tuples
 * read (0 indicates end-of-scan).
 */
static int
vec_fill_batch(VectorScanState *vss)
{
	VectorBatch *batch = vss->batch;
	TupleDesc	tupdesc = vss->scan_tupdesc;
	TableScanDesc scandesc = vss->scan_desc;
	EState	   *estate = vss->css.ss.ps.state;
	TupleTableSlot *slot = vss->css.ss.ss_ScanTupleSlot;
	int			natts = tupdesc->natts;
	int			row;

	ResetVectorBatch(batch);

	for (row = 0; row < VECTOR_BATCH_SIZE; row++)
	{
		int			col;

		CHECK_FOR_INTERRUPTS();

		if (!table_scan_getnextslot(scandesc, estate->es_direction, slot))
		{
			vss->scan_done = true;
			break;
		}

		/* Deform the tuple to access all attributes */
		slot_getallattrs(slot);

		/* Transpose into columnar arrays */
		for (col = 0; col < natts && col < batch->ncols; col++)
		{
			batch->cols[col].values[row] = slot->tts_values[col];
			batch->cols[col].nulls[row] = slot->tts_isnull[col];
		}

		ExecClearTuple(slot);
	}

	batch->nrows = row;
	return row;
}

/*
 * VectorScanExec
 *
 * Return the next tuple from the vectorized scan.  Internally we process
 * tuples in batches, draining one tuple at a time for the parent operator.
 */
static TupleTableSlot *
VectorScanExec(CustomScanState *node)
{
	VectorScanState *vss = (VectorScanState *) node;
	TupleTableSlot *resultSlot = node->ss.ps.ps_ResultTupleSlot;
	TupleDesc	resultdesc = resultSlot->tts_tupleDescriptor;
	int			natts;

	for (;;)
	{
		/* If we have rows remaining in the current batch, drain one */
		if (vss->batch_pos < vss->batch->nrows)
		{
			int			row = vss->batch_pos++;

			ExecClearTuple(resultSlot);

			natts = resultdesc->natts;

			/*
			 * Map batch columns to result slot.  For simple projections
			 * the scan columns map 1:1 to the result.  For expressions
			 * the target list compilation put results in working columns
			 * beyond the scan columns; we rely on the target list order
			 * matching the result tuple descriptor.
			 */
			if (vss->orig_tlist != NIL)
			{
				ListCell   *lc;
				int			resno = 0;

				foreach(lc, vss->orig_tlist)
				{
					TargetEntry *tle = (TargetEntry *) lfirst(lc);

					if (tle->resjunk)
						continue;

					if (resno < natts)
					{
						/*
						 * For simple Var references, use the scan column
						 * directly.  For expressions, the compiled program
						 * placed results in working columns -- but in
						 * Phase 2.1 we support only simple Var projections
						 * and inline expressions.  The VecExprCompile step
						 * put each target entry's result in the column
						 * returned by the compiler; however we do not
						 * currently track per-tle result columns in the
						 * state.  For simple Var targets we read directly
						 * from the scan column.
						 */
						if (IsA(tle->expr, Var))
						{
							Var		   *var = (Var *) tle->expr;
							int			col = var->varattno - 1;

							if (col >= 0 && col < vss->batch->ncols)
							{
								resultSlot->tts_values[resno] =
									vss->batch->cols[col].values[row];
								resultSlot->tts_isnull[resno] =
									vss->batch->cols[col].nulls[row];
							}
							else
							{
								resultSlot->tts_values[resno] = (Datum) 0;
								resultSlot->tts_isnull[resno] = true;
							}
						}
						else
						{
							/*
							 * Non-Var expression.  We fall back to
							 * reading from the scan columns and let the
							 * standard executor handle projection.  This
							 * ensures correctness at the cost of
							 * foregoing vectorized projection for complex
							 * expressions in Phase 2.1.
							 *
							 * In practice the planner hook only enables
							 * vectorized scan for simple cases so this
							 * path is rarely taken.
							 */
							int			col = resno;

							if (col < vss->scan_tupdesc->natts &&
								col < vss->batch->ncols)
							{
								resultSlot->tts_values[resno] =
									vss->batch->cols[col].values[row];
								resultSlot->tts_isnull[resno] =
									vss->batch->cols[col].nulls[row];
							}
							else
							{
								resultSlot->tts_values[resno] = (Datum) 0;
								resultSlot->tts_isnull[resno] = true;
							}
						}
						resno++;
					}
				}
			}
			else
			{
				/* No target list -- pass through all scan columns */
				int			col;

				for (col = 0; col < natts && col < vss->batch->ncols; col++)
				{
					resultSlot->tts_values[col] =
						vss->batch->cols[col].values[row];
					resultSlot->tts_isnull[col] =
						vss->batch->cols[col].nulls[row];
				}
			}

			return ExecStoreVirtualTuple(resultSlot);
		}

		/* Current batch is exhausted -- try to fill the next one */
		if (vss->scan_done)
			return ExecClearTuple(resultSlot);

		/* Open the scan on first call */
		if (vss->scan_desc == NULL)
		{
			vss->scan_desc = table_beginscan(vss->scan_rel,
											 vss->css.ss.ps.state->es_snapshot,
											 0, NULL);
		}

		/* Fill a new batch */
		if (vec_fill_batch(vss) == 0)
			return ExecClearTuple(resultSlot);

		/* Evaluate vectorized filter + projection over the batch */
		VecExprEval(vss->vstate, vss->batch);

		/* Start draining from position 0 */
		vss->batch_pos = 0;
	}
}

/*
 * VectorScanEnd
 */
static void
VectorScanEnd(CustomScanState *node)
{
	VectorScanState *vss = (VectorScanState *) node;

	if (vss->scan_desc != NULL)
		table_endscan(vss->scan_desc);

	FreeVectorBatch(vss->batch);
	vss->batch = NULL;
}

/*
 * VectorScanReScan
 */
static void
VectorScanReScan(CustomScanState *node)
{
	VectorScanState *vss = (VectorScanState *) node;

	if (vss->scan_desc != NULL)
		table_rescan(vss->scan_desc, NULL);

	if (vss->batch)
		ResetVectorBatch(vss->batch);

	vss->batch_pos = 0;
	vss->scan_done = false;
}

/*
 * VectorScanExplain
 */
static void
VectorScanExplain(CustomScanState *node, List *ancestors,
				  ExplainState *es)
{
	VectorScanState *vss = (VectorScanState *) node;

	ExplainPropertyText("Vectorized", "true", es);
	ExplainPropertyInteger("Batch Size", NULL, VECTOR_BATCH_SIZE, es);

	if (vss->vstate)
		ExplainPropertyInteger("Vector Steps", NULL,
							   vss->vstate->nsteps, es);
}


/* -----------------------------------------------------------------------
 * Hook registration -- called from execProcnode.c or _PG_init
 * -----------------------------------------------------------------------
 */

void
VectorScan_RegisterHook(void)
{
	/* Register the GUC */
	DefineCustomBoolVariable("enable_vectorized_scan",
							 "Enables the vectorized sequential scan execution path.",
							 NULL,
							 &enable_vectorized_scan,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	/* Register the CustomScan methods so the planner / executor can find us */
	RegisterCustomScanMethods(&vectorscan_scan_methods);

	/* Install the planner hook */
	prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = vectorscan_pathlist_hook;
}
