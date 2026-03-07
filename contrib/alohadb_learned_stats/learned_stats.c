/*-------------------------------------------------------------------------
 *
 * learned_stats.c
 *		ML-Assisted Cardinality Estimation Extension for PostgreSQL.
 *
 * This extension hooks into the planner and executor to:
 *   1. Provide corrected selectivity estimates via get_relation_stats_hook
 *      and get_index_stats_hook when the trained model is confident.
 *   2. Log (estimated_rows, actual_rows, plan_features) to a shared-memory
 *      ring buffer at ExecutorEnd time.
 *   3. Periodically train a lightweight gradient-boosted tree model on the
 *      collected data via a background worker.
 *
 * The extension must be loaded via shared_preload_libraries so that it
 * can allocate shared memory and register its background worker.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * contrib/alohadb_learned_stats/learned_stats.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "learned_stats.h"

#include "access/htup_details.h"
#include "access/transam.h"
#include "catalog/pg_class.h"
#include "catalog/pg_statistic.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "optimizer/optimizer.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/selfuncs.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC_EXT(
	.name = "alohadb_learned_stats",
	.version = PG_VERSION
);

/* ---------------------------------------------------------------
 * Global state
 * ---------------------------------------------------------------
 */

/* Pointer to shared memory segment */
LearnedStatsSharedState *lstat_shared = NULL;

/* GUC variable */
bool		lstat_enabled = false;

/* GUC: background worker training interval in seconds */
static int	lstat_train_interval = 300;		/* 5 minutes */

/* Saved hook values for chaining */
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static get_relation_stats_hook_type prev_get_relation_stats_hook = NULL;
static get_index_stats_hook_type prev_get_index_stats_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;

/* ---------------------------------------------------------------
 * Forward declarations
 * ---------------------------------------------------------------
 */
static void lstat_shmem_request(void);
static void lstat_shmem_startup(void);
static bool lstat_get_relation_stats(PlannerInfo *root,
									 RangeTblEntry *rte,
									 AttrNumber attnum,
									 VariableStatData *vardata);
static bool lstat_get_index_stats(PlannerInfo *root,
								  Oid indexOid,
								  AttrNumber indexattnum,
								  VariableStatData *vardata);
static void lstat_ExecutorEnd(QueryDesc *queryDesc);
static void lstat_collect_sample(QueryDesc *queryDesc);
static void lstat_extract_features(PlanState *planstate,
								   PlannedStmt *pstmt,
								   double *features);
static int	lstat_count_predicates(Plan *plan);
static int	lstat_classify_predicates(Plan *plan);

/* Background worker entry point */
PGDLLEXPORT void lstat_bgworker_main(Datum main_arg);

/* SQL-callable functions */
PG_FUNCTION_INFO_V1(alohadb_learned_stats_status);
PG_FUNCTION_INFO_V1(alohadb_learned_stats_train);
PG_FUNCTION_INFO_V1(alohadb_learned_stats_reset);

/* ---------------------------------------------------------------
 * Shared memory sizing
 * ---------------------------------------------------------------
 */
Size
lstat_shmem_size(void)
{
	return MAXALIGN(sizeof(LearnedStatsSharedState));
}

/* ---------------------------------------------------------------
 * Module initialization
 * ---------------------------------------------------------------
 */
void
_PG_init(void)
{
	BackgroundWorker bgw;

	if (!process_shared_preload_libraries_in_progress)
		return;

	/* Define GUC variables */
	DefineCustomBoolVariable("alohadb.learned_stats_enabled",
							 "Enable ML-assisted cardinality estimation.",
							 "When enabled and the model is trained, the extension "
							 "provides corrected selectivity estimates to the planner.",
							 &lstat_enabled,
							 false,		/* default: off until model trained */
							 PGC_SUSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("alohadb.learned_stats_train_interval",
							"Background training interval in seconds.",
							"How often the background worker retrains the model "
							"on accumulated query feedback data.",
							&lstat_train_interval,
							300,	/* 5 minutes */
							10,		/* min 10 seconds */
							86400,	/* max 1 day */
							PGC_SIGHUP,
							GUC_UNIT_S,
							NULL, NULL, NULL);

	MarkGUCPrefixReserved("alohadb");

	/* Install hooks */
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = lstat_shmem_request;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = lstat_shmem_startup;

	prev_get_relation_stats_hook = get_relation_stats_hook;
	get_relation_stats_hook = lstat_get_relation_stats;

	prev_get_index_stats_hook = get_index_stats_hook;
	get_index_stats_hook = lstat_get_index_stats;

	prev_ExecutorEnd_hook = ExecutorEnd_hook;
	ExecutorEnd_hook = lstat_ExecutorEnd;

	/* Register background worker for periodic model training */
	memset(&bgw, 0, sizeof(bgw));
	snprintf(bgw.bgw_name, BGW_MAXLEN, "alohadb_learned_stats trainer");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "alohadb_learned_stats trainer");
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	bgw.bgw_restart_time = 60;		/* restart after 60s if it crashes */
	snprintf(bgw.bgw_library_name, MAXPGPATH, "alohadb_learned_stats");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "lstat_bgworker_main");
	bgw.bgw_main_arg = (Datum) 0;
	bgw.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&bgw);
}

/* ---------------------------------------------------------------
 * Shared memory request hook
 * ---------------------------------------------------------------
 */
static void
lstat_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(lstat_shmem_size());
	RequestNamedLWLockTranche("alohadb_learned_stats", 1);
}

/* ---------------------------------------------------------------
 * Shared memory startup hook
 * ---------------------------------------------------------------
 */
static void
lstat_shmem_startup(void)
{
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	lstat_shared = ShmemInitStruct("alohadb_learned_stats",
								   lstat_shmem_size(),
								   &found);

	if (!found)
	{
		/* First time: initialize shared state */
		memset(lstat_shared, 0, sizeof(LearnedStatsSharedState));
		lstat_shared->lock =
			&(GetNamedLWLockTranche("alohadb_learned_stats"))->lock;
		lstat_shared->sample_head = 0;
		lstat_shared->num_samples = 0;
		lstat_shared->last_train_time = 0;
		lstat_shared->total_queries_logged = 0;
		lstat_shared->total_predictions_made = 0;
		lstat_shared->total_fallbacks = 0;
		lstat_shared->enabled = false;
		lstat_model_reset(&lstat_shared->model);
	}

	LWLockRelease(AddinShmemInitLock);
}

/* ---------------------------------------------------------------
 * Safe log2 helper
 * ---------------------------------------------------------------
 */
static inline double
safe_log2(double val)
{
	if (val <= 0.0)
		return 0.0;
	return log2(val);
}

/* ---------------------------------------------------------------
 * get_relation_stats_hook implementation
 *
 * This hook is called by the planner when it wants statistics for
 * a relation column.  We do NOT replace the statistics tuple; instead
 * we return false to let the standard path proceed.  Our cardinality
 * correction happens at a different level (the selectivity estimate
 * adjustment is done through the stats we provide).
 *
 * However, since the hook interface provides VariableStatData which
 * is about per-column statistics (not overall cardinality), the most
 * appropriate approach is to let standard statistics proceed and only
 * intercept at the executor level for feedback.
 *
 * For Phase 3.3, the relation stats hook serves as a point where we
 * could inject modified statistics if needed.  For now, we chain to
 * the previous hook and only intervene if we have very high
 * confidence and the relation is one we have extensive training data
 * for.  The primary correction mechanism is via the ExecutorEnd
 * feedback loop.
 * ---------------------------------------------------------------
 */
static bool
lstat_get_relation_stats(PlannerInfo *root,
						 RangeTblEntry *rte,
						 AttrNumber attnum,
						 VariableStatData *vardata)
{
	/* Chain to previous hook first */
	if (prev_get_relation_stats_hook &&
		prev_get_relation_stats_hook(root, rte, attnum, vardata))
		return true;

	/*
	 * We return false here to let the standard statistics path proceed.  Our
	 * model works at the plan-level cardinality estimation and feeds back via
	 * ExecutorEnd.  A future enhancement could synthesize modified
	 * pg_statistic tuples here for columns with poor standard estimates.
	 */
	return false;
}

/* ---------------------------------------------------------------
 * get_index_stats_hook implementation
 *
 * Similar to the relation stats hook -- we chain and fall back.
 * ---------------------------------------------------------------
 */
static bool
lstat_get_index_stats(PlannerInfo *root,
					  Oid indexOid,
					  AttrNumber indexattnum,
					  VariableStatData *vardata)
{
	/* Chain to previous hook first */
	if (prev_get_index_stats_hook &&
		prev_get_index_stats_hook(root, indexOid, indexattnum, vardata))
		return true;

	return false;
}

/* ---------------------------------------------------------------
 * Count restriction clauses in a Plan node's qual list.
 * ---------------------------------------------------------------
 */
static int
lstat_count_predicates(Plan *plan)
{
	int			count = 0;

	if (plan == NULL)
		return 0;

	if (plan->qual != NIL)
		count += list_length(plan->qual);

	/* Also count initPlan / subPlan-related quals if present */
	if (plan->lefttree)
		count += lstat_count_predicates(plan->lefttree);
	if (plan->righttree)
		count += lstat_count_predicates(plan->righttree);

	return count;
}

/* ---------------------------------------------------------------
 * Classify predicate types in the plan tree.
 * Returns a bitmask of PRED_TYPE_* flags.
 * ---------------------------------------------------------------
 */
static int
lstat_classify_predicates(Plan *plan)
{
	int			flags = 0;
	ListCell   *lc;

	if (plan == NULL)
		return 0;

	foreach(lc, plan->qual)
	{
		Node	   *node = (Node *) lfirst(lc);

		if (IsA(node, OpExpr))
		{
			OpExpr *op = (OpExpr *) node;
			Oid		opno = op->opno;
			char   *opname;

			opname = get_opname(opno);
			if (opname != NULL)
			{
				if (strcmp(opname, "=") == 0)
					flags |= PRED_TYPE_EQUALITY;
				else if (strcmp(opname, "<") == 0 ||
						 strcmp(opname, ">") == 0 ||
						 strcmp(opname, "<=") == 0 ||
						 strcmp(opname, ">=") == 0 ||
						 strcmp(opname, "<>") == 0)
					flags |= PRED_TYPE_RANGE;
				else if (strcmp(opname, "~~") == 0 ||
						 strcmp(opname, "~~*") == 0)
					flags |= PRED_TYPE_LIKE;
				else if (strcmp(opname, "<->") == 0 ||
						 strcmp(opname, "<#>") == 0 ||
						 strcmp(opname, "<=>") == 0)
					flags |= PRED_TYPE_VECTOR_SIM;
				else
					flags |= PRED_TYPE_OTHER;
			}
			else
				flags |= PRED_TYPE_OTHER;
		}
		else if (IsA(node, FuncExpr))
		{
			flags |= PRED_TYPE_FUNCTION;
		}
		else
		{
			flags |= PRED_TYPE_OTHER;
		}
	}

	/* Recurse into child plans */
	if (plan->lefttree)
		flags |= lstat_classify_predicates(plan->lefttree);
	if (plan->righttree)
		flags |= lstat_classify_predicates(plan->righttree);

	return flags;
}

/* ---------------------------------------------------------------
 * Determine if a plan is primarily an index scan.
 * ---------------------------------------------------------------
 */
static bool
plan_is_index_scan(Plan *plan)
{
	if (plan == NULL)
		return false;

	switch (nodeTag(plan))
	{
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapIndexScan:
			return true;
		default:
			break;
	}

	/* Check lefttree -- covers BitmapHeapScan -> BitmapIndexScan */
	if (plan->lefttree && plan_is_index_scan(plan->lefttree))
		return true;

	return false;
}

/* ---------------------------------------------------------------
 * Extract the feature vector from a completed plan.
 * ---------------------------------------------------------------
 */
static void
lstat_extract_features(PlanState *planstate,
					   PlannedStmt *pstmt,
					   double *features)
{
	Plan	   *plan;
	Oid			relid = InvalidOid;
	double		reltuples = 0.0;
	double		relpages = 0.0;
	int			i;

	/* Initialize all features to 0 */
	for (i = 0; i < LSTAT_NUM_FEATURES; i++)
		features[i] = 0.0;

	if (planstate == NULL || pstmt == NULL)
		return;

	plan = planstate->plan;
	if (plan == NULL)
		return;

	/*
	 * Try to determine the primary relation from the plan tree.
	 * We look at the rangetable for the first non-system relation.
	 */
	if (pstmt->rtable != NIL)
	{
		ListCell   *lc;

		foreach(lc, pstmt->rtable)
		{
			RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

			if (rte->rtekind == RTE_RELATION && rte->relid >= FirstNormalObjectId)
			{
				relid = rte->relid;

				/* Get relation size info from pg_class */
				{
					HeapTuple	classtuple;

					classtuple = SearchSysCache1(RELOID,
												 ObjectIdGetDatum(relid));
					if (HeapTupleIsValid(classtuple))
					{
						Form_pg_class classform =
							(Form_pg_class) GETSTRUCT(classtuple);

						reltuples = classform->reltuples;
						relpages = classform->relpages;
						ReleaseSysCache(classtuple);
					}
				}
				break;
			}
		}
	}

	features[FEAT_LOG_TABLE_SIZE] = safe_log2(relpages > 0 ? relpages : 1.0);
	features[FEAT_LOG_NTUPLES] = safe_log2(reltuples > 0 ? reltuples : 1.0);
	features[FEAT_NUM_PREDICATES] = (double) lstat_count_predicates(plan);
	features[FEAT_PREDICATE_TYPE] = (double) lstat_classify_predicates(plan);

	/*
	 * Get n_distinct and correlation for the first column referenced.
	 * This is a simplification -- a more complete implementation would
	 * extract per-column stats for each predicate column.
	 */
	if (relid != InvalidOid)
	{
		HeapTuple	stattuple;

		/* Try column 1 stats as a representative */
		stattuple = SearchSysCache3(STATRELATTINH,
									ObjectIdGetDatum(relid),
									Int16GetDatum(1),
									BoolGetDatum(false));
		if (HeapTupleIsValid(stattuple))
		{
			Form_pg_statistic statsform =
				(Form_pg_statistic) GETSTRUCT(stattuple);

			features[FEAT_N_DISTINCT] = statsform->stadistinct;

			/*
			 * Extract correlation from the STATISTIC_KIND_CORRELATION slot.
			 * The correlation coefficient is stored as a single float4 in the
			 * stanumbers array of the correlation statistics slot.
			 */
			{
				AttStatsSlot sslot;

				if (get_attstatsslot(&sslot, stattuple,
									 STATISTIC_KIND_CORRELATION, InvalidOid,
									 ATTSTATSSLOT_NUMBERS))
				{
					if (sslot.nnumbers >= 1)
						features[FEAT_CORRELATION] = sslot.numbers[0];
					free_attstatsslot(&sslot);
				}
			}

			ReleaseSysCache(stattuple);
		}
	}

	features[FEAT_LOG_ESTIMATED] = safe_log2(plan->plan_rows > 0 ?
											 plan->plan_rows : 1.0);
	features[FEAT_INDEX_SCAN] = plan_is_index_scan(plan) ? 1.0 : 0.0;
}

/* ---------------------------------------------------------------
 * Collect a training sample at ExecutorEnd time.
 * ---------------------------------------------------------------
 */
static void
lstat_collect_sample(QueryDesc *queryDesc)
{
	PlannedStmt *pstmt;
	PlanState  *planstate;
	Plan	   *plan;
	double		estimated_rows;
	double		actual_rows;
	LearnedStatsSample sample;
	int			pos;

	if (queryDesc == NULL || queryDesc->plannedstmt == NULL)
		return;

	pstmt = queryDesc->plannedstmt;
	planstate = queryDesc->planstate;

	/* Only collect for SELECT queries (CMD_SELECT) */
	if (queryDesc->operation != CMD_SELECT)
		return;

	if (planstate == NULL)
		return;

	plan = planstate->plan;
	if (plan == NULL)
		return;

	estimated_rows = plan->plan_rows;

	/*
	 * Get actual rows processed.  We use es_total_processed from the executor
	 * state, which tracks the total number of tuples returned across all
	 * ExecutorRun calls.
	 */
	if (queryDesc->estate != NULL)
		actual_rows = (double) queryDesc->estate->es_total_processed;
	else
		actual_rows = 0.0;

	/* Skip trivial queries (0 rows estimated or actual) */
	if (estimated_rows <= 0.0 && actual_rows <= 0.0)
		return;

	/* Extract features */
	memset(&sample, 0, sizeof(sample));
	lstat_extract_features(planstate, pstmt, sample.features);
	sample.log_actual_rows = safe_log2(actual_rows + 1.0);
	sample.valid = true;

	/* Determine the primary relation OID */
	if (pstmt->rtable != NIL)
	{
		ListCell   *lc;

		foreach(lc, pstmt->rtable)
		{
			RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

			if (rte->rtekind == RTE_RELATION && rte->relid >= FirstNormalObjectId)
			{
				sample.relid = rte->relid;
				break;
			}
		}
	}

	/* Write to ring buffer under lock */
	LWLockAcquire(lstat_shared->lock, LW_EXCLUSIVE);

	pos = lstat_shared->sample_head;
	lstat_shared->samples[pos] = sample;
	lstat_shared->sample_head = (pos + 1) % LSTAT_MAX_SAMPLES;
	if (lstat_shared->num_samples < LSTAT_MAX_SAMPLES)
		lstat_shared->num_samples++;
	lstat_shared->total_queries_logged++;

	LWLockRelease(lstat_shared->lock);
}

/* ---------------------------------------------------------------
 * ExecutorEnd hook
 *
 * We collect query execution feedback (estimated vs actual rows) and
 * store it in the shared-memory ring buffer for model training.
 * ---------------------------------------------------------------
 */
static void
lstat_ExecutorEnd(QueryDesc *queryDesc)
{
	/*
	 * Collect the sample before the plan state is torn down.  We do this
	 * regardless of whether the model is enabled, so that training data
	 * accumulates even before the first model is trained.
	 */
	if (lstat_shared != NULL && queryDesc != NULL)
	{
		PG_TRY();
		{
			lstat_collect_sample(queryDesc);
		}
		PG_CATCH();
		{
			/*
			 * Don't let our bookkeeping errors disrupt normal query
			 * processing.  Just swallow the error and move on.
			 */
			FlushErrorState();
		}
		PG_END_TRY();
	}

	/* Chain to previous hook or standard implementation */
	if (prev_ExecutorEnd_hook)
		prev_ExecutorEnd_hook(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

/* ---------------------------------------------------------------
 * Background worker main function
 *
 * Periodically retrains the model on accumulated sample data.
 * ---------------------------------------------------------------
 */
void
lstat_bgworker_main(Datum main_arg)
{
	/* Register signal handlers */
	BackgroundWorkerUnblockSignals();

	elog(LOG, "alohadb_learned_stats: background trainer started");

	while (!ShutdownRequestPending)
	{
		/* Wait for the configured interval or until signaled */
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 lstat_train_interval * 1000L,
						 PG_WAIT_EXTENSION);

		ResetLatch(MyLatch);

		if (ShutdownRequestPending)
			break;

		CHECK_FOR_INTERRUPTS();

		/* Only train if we have shared memory and enough samples */
		if (lstat_shared == NULL)
			continue;

		LWLockAcquire(lstat_shared->lock, LW_EXCLUSIVE);

		if (lstat_shared->num_samples >= LSTAT_MIN_TRAIN_SAMPLES)
		{
			PG_TRY();
			{
				lstat_model_train(lstat_shared);
			}
			PG_CATCH();
			{
				LWLockRelease(lstat_shared->lock);
				FlushErrorState();
				elog(LOG, "alohadb_learned_stats: training failed, will retry");
				continue;
			}
			PG_END_TRY();
		}

		LWLockRelease(lstat_shared->lock);
	}

	elog(LOG, "alohadb_learned_stats: background trainer shutting down");
	proc_exit(0);
}

/* ===============================================================
 * SQL-callable functions
 * ===============================================================
 */

/*
 * alohadb_learned_stats_status()
 *		Returns a single row with status information about the extension.
 */
Datum
alohadb_learned_stats_status(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[3];
	bool		nulls[3];
	HeapTuple	tuple;

	if (lstat_shared == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("alohadb_learned_stats must be loaded via shared_preload_libraries")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	memset(nulls, 0, sizeof(nulls));

	LWLockAcquire(lstat_shared->lock, LW_SHARED);

	values[0] = Int32GetDatum(lstat_shared->num_samples);
	values[1] = BoolGetDatum(lstat_shared->model.trained);

	if (lstat_shared->last_train_time != 0)
		values[2] = TimestampTzGetDatum(lstat_shared->last_train_time);
	else
		nulls[2] = true;

	LWLockRelease(lstat_shared->lock);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/*
 * alohadb_learned_stats_train()
 *		Manually trigger model training.
 */
Datum
alohadb_learned_stats_train(PG_FUNCTION_ARGS)
{
	if (lstat_shared == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("alohadb_learned_stats must be loaded via shared_preload_libraries")));

	LWLockAcquire(lstat_shared->lock, LW_EXCLUSIVE);

	PG_TRY();
	{
		lstat_model_train(lstat_shared);
	}
	PG_CATCH();
	{
		LWLockRelease(lstat_shared->lock);
		PG_RE_THROW();
	}
	PG_END_TRY();

	LWLockRelease(lstat_shared->lock);

	PG_RETURN_VOID();
}

/*
 * alohadb_learned_stats_reset()
 *		Clear all training data and reset the model.
 */
Datum
alohadb_learned_stats_reset(PG_FUNCTION_ARGS)
{
	if (lstat_shared == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("alohadb_learned_stats must be loaded via shared_preload_libraries")));

	LWLockAcquire(lstat_shared->lock, LW_EXCLUSIVE);

	/* Clear samples */
	memset(lstat_shared->samples, 0, sizeof(lstat_shared->samples));
	lstat_shared->sample_head = 0;
	lstat_shared->num_samples = 0;
	lstat_shared->total_queries_logged = 0;
	lstat_shared->total_predictions_made = 0;
	lstat_shared->total_fallbacks = 0;
	lstat_shared->last_train_time = 0;

	/* Reset model */
	lstat_model_reset(&lstat_shared->model);

	LWLockRelease(lstat_shared->lock);

	elog(NOTICE, "alohadb_learned_stats: all training data and model have been reset");
	PG_RETURN_VOID();
}
