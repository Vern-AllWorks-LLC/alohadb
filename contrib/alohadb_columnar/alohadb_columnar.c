/*-------------------------------------------------------------------------
 *
 * alohadb_columnar.c
 *	  Columnar storage table access method with zstd compression.
 *
 *	  Provides an append-only columnar table AM.  Data is organized in
 *	  stripes of rows where each stripe stores column values contiguously
 *	  and compresses them with zstd.  This is optimized for bulk-load
 *	  analytical workloads (INSERT + SELECT).  UPDATE, DELETE, and index
 *	  scans are not supported (error stubs provided).
 *
 * Patent note: Based on C-Store (BSD 2005), Hydra columnar (Apache 2.0).
 * Column-oriented storage is a well-established academic concept from the
 * 1970s.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_columnar/alohadb_columnar.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/generic_xlog.h"
#include "access/multixact.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "alohadb_columnar.h"

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_columnar",
					.version = "1.0"
);

PG_FUNCTION_INFO_V1(columnar_tableam_handler);
PG_FUNCTION_INFO_V1(alohadb_columnar_info);

/* GUC variable */
int			columnar_stripe_rows = COLUMNAR_DEFAULT_STRIPE_ROWS;

/*
 * Per-relation write state.  We keep at most one active write state per
 * relation, stored in a simple linked list (fine for typical workloads).
 */
typedef struct ColumnarWriteStateEntry
{
	Oid			relid;
	ColumnarWriteState *wstate;
	struct ColumnarWriteStateEntry *next;
} ColumnarWriteStateEntry;

static ColumnarWriteStateEntry *write_state_list = NULL;

/*
 * Transaction callback: flush pending writes at commit, discard at abort.
 *
 * This is necessary because finish_bulk_insert is only called by COPY and
 * CTAS, not by regular INSERT statements.  Without this callback, rows
 * buffered by regular INSERTs would be lost at transaction end.
 */
static void
columnar_xact_callback(XactEvent event, void *arg)
{
	if (event == XACT_EVENT_PRE_COMMIT ||
		event == XACT_EVENT_PARALLEL_PRE_COMMIT)
	{
		/* Flush all pending write states before commit */
		ColumnarWriteStateEntry *entry;

		for (entry = write_state_list; entry != NULL; entry = entry->next)
		{
			if (entry->wstate != NULL && entry->wstate->buffered_rows > 0)
			{
				Relation	rel;

				rel = table_open(entry->relid, RowExclusiveLock);
				columnar_flush_stripe(rel, entry->wstate);
				table_close(rel, RowExclusiveLock);
			}
		}
	}

	/* On any transaction end (commit or abort), reset the list */
	write_state_list = NULL;
}

/* ----------------------------------------------------------------
 * Write state management
 * ----------------------------------------------------------------
 */

static ColumnarWriteState *
columnar_get_write_state(Relation rel)
{
	ColumnarWriteStateEntry *entry;

	for (entry = write_state_list; entry != NULL; entry = entry->next)
	{
		if (entry->relid == RelationGetRelid(rel))
			return entry->wstate;
	}
	return NULL;
}

static ColumnarWriteState *
columnar_create_write_state(Relation rel)
{
	ColumnarWriteState *wstate;
	MemoryContext mcxt;
	MemoryContext oldcxt;
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int			natts = tupdesc->natts;
	int			max_rows = columnar_stripe_rows;
	ColumnarWriteStateEntry *entry;
	int			i;

	mcxt = AllocSetContextCreate(TopTransactionContext,
								 "columnar write state",
								 ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(mcxt);

	wstate = palloc0(sizeof(ColumnarWriteState));
	wstate->mcxt = mcxt;
	wstate->tupdesc = tupdesc;
	wstate->natts = natts;
	wstate->buffered_rows = 0;
	wstate->max_rows = max_rows;

	wstate->col_values = palloc(sizeof(Datum *) * natts);
	wstate->col_nulls = palloc(sizeof(bool *) * natts);
	for (i = 0; i < natts; i++)
	{
		wstate->col_values[i] = palloc(sizeof(Datum) * max_rows);
		wstate->col_nulls[i] = palloc(sizeof(bool) * max_rows);
	}

	/* Register in the write state list */
	entry = MemoryContextAlloc(TopTransactionContext,
							   sizeof(ColumnarWriteStateEntry));
	entry->relid = RelationGetRelid(rel);
	entry->wstate = wstate;
	entry->next = write_state_list;
	write_state_list = entry;

	MemoryContextSwitchTo(oldcxt);

	return wstate;
}

static void
columnar_buffer_row(Relation rel, ColumnarWriteState *wstate, TupleTableSlot *slot)
{
	MemoryContext oldcxt;
	int			row;
	int			i;

	slot_getallattrs(slot);

	oldcxt = MemoryContextSwitchTo(wstate->mcxt);

	row = wstate->buffered_rows;
	for (i = 0; i < wstate->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(wstate->tupdesc, i);

		if (slot->tts_isnull[i] || attr->attisdropped)
		{
			wstate->col_values[i][row] = (Datum) 0;
			wstate->col_nulls[i][row] = true;
		}
		else
		{
			wstate->col_values[i][row] = datumCopy(slot->tts_values[i],
												   attr->attbyval,
												   attr->attlen);
			wstate->col_nulls[i][row] = false;
		}
	}
	wstate->buffered_rows++;

	MemoryContextSwitchTo(oldcxt);

	/* Flush stripe if full */
	if (wstate->buffered_rows >= wstate->max_rows)
		columnar_flush_stripe(rel, wstate);
}

/* ----------------------------------------------------------------
 * Slot callbacks
 * ----------------------------------------------------------------
 */

static const TupleTableSlotOps *
columnar_slot_callbacks(Relation relation)
{
	return &TTSOpsVirtual;
}

/* ----------------------------------------------------------------
 * Sequential scan callbacks
 * ----------------------------------------------------------------
 */

static TableScanDesc
columnar_scan_begin(Relation rel, Snapshot snapshot,
					int nkeys, struct ScanKeyData *key,
					ParallelTableScanDesc pscan, uint32 flags)
{
	ColumnarScanDesc scan;

	scan = palloc0(sizeof(ColumnarScanDescData));
	scan->base.rs_rd = rel;
	scan->base.rs_snapshot = snapshot;
	scan->base.rs_nkeys = nkeys;
	scan->base.rs_flags = flags;
	scan->base.rs_parallel = pscan;

	scan->current_stripe = 0;
	scan->current_row = 0;
	scan->stripe_rows = 0;
	scan->row_values = NULL;
	scan->row_nulls = NULL;
	scan->stripe_values = NULL;
	scan->stripe_nulls = NULL;
	scan->stripe_mcxt = NULL;

	/*
	 * Flush any pending writes for this relation before scanning, so that
	 * we can see our own inserts.
	 */
	{
		ColumnarWriteState *wstate = columnar_get_write_state(rel);

		if (wstate != NULL && wstate->buffered_rows > 0)
			columnar_flush_stripe(rel, wstate);
	}

	return (TableScanDesc) scan;
}

static void
columnar_scan_end(TableScanDesc sscan)
{
	ColumnarScanDesc scan = (ColumnarScanDesc) sscan;

	if (scan->stripe_mcxt != NULL)
		MemoryContextDelete(scan->stripe_mcxt);

	pfree(scan);
}

static void
columnar_scan_rescan(TableScanDesc sscan, struct ScanKeyData *key,
					 bool set_params, bool allow_strat,
					 bool allow_sync, bool allow_pagemode)
{
	ColumnarScanDesc scan = (ColumnarScanDesc) sscan;

	scan->current_stripe = 0;
	scan->current_row = 0;
	scan->stripe_rows = 0;

	if (scan->stripe_mcxt != NULL)
	{
		MemoryContextDelete(scan->stripe_mcxt);
		scan->stripe_mcxt = NULL;
	}
	scan->stripe_values = NULL;
	scan->stripe_nulls = NULL;
	scan->row_values = NULL;
	scan->row_nulls = NULL;
}

static bool
columnar_scan_getnextslot(TableScanDesc sscan, ScanDirection direction,
						  TupleTableSlot *slot)
{
	ColumnarScanDesc scan = (ColumnarScanDesc) sscan;
	Relation	rel = sscan->rs_rd;
	ColumnarMetaPageData meta;
	int			natts;
	int			i;

	ExecClearTuple(slot);

	/* Empty table check */
	if (ColumnarGetNblocks(rel) == 0)
		return false;

	columnar_read_metadata(rel, &meta);
	natts = meta.natts;

	if (meta.num_stripes == 0)
		return false;

	/* Advance to next row, loading stripes as needed */
	while (scan->current_stripe < meta.num_stripes)
	{
		/* Load stripe if needed */
		if (scan->stripe_values == NULL || scan->current_row >= scan->stripe_rows)
		{
			/* Move to next stripe if we've exhausted the current one */
			if (scan->stripe_values != NULL)
			{
				scan->current_stripe++;
				if (scan->current_stripe >= meta.num_stripes)
					return false;
			}

			/* Decompress the stripe */
			columnar_read_stripe(rel, scan, scan->current_stripe);

			if (scan->stripe_rows == 0)
			{
				scan->current_stripe++;
				continue;
			}
			scan->current_row = 0;
		}

		/* Return the current row */
		for (i = 0; i < natts && i < slot->tts_tupleDescriptor->natts; i++)
		{
			slot->tts_values[i] = scan->stripe_values[i][scan->current_row];
			slot->tts_isnull[i] = scan->stripe_nulls[i][scan->current_row];
		}
		/* Fill any remaining columns with nulls */
		for (; i < slot->tts_tupleDescriptor->natts; i++)
		{
			slot->tts_values[i] = (Datum) 0;
			slot->tts_isnull[i] = true;
		}

		ExecStoreVirtualTuple(slot);
		scan->current_row++;
		return true;
	}

	return false;
}

/* ----------------------------------------------------------------
 * TID range scan stubs
 * ----------------------------------------------------------------
 */

static void
columnar_scan_set_tidrange(TableScanDesc sscan, ItemPointer mintid,
						   ItemPointer maxtid)
{
	/* No meaningful TID range support for columnar */
}

static bool
columnar_scan_getnextslot_tidrange(TableScanDesc sscan,
								   ScanDirection direction,
								   TupleTableSlot *slot)
{
	/* Not supported */
	return false;
}

/* ----------------------------------------------------------------
 * Parallel scan stubs
 * ----------------------------------------------------------------
 */

static Size
columnar_parallelscan_estimate(Relation rel)
{
	return sizeof(ParallelTableScanDescData);
}

static Size
columnar_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	pscan->phs_locator = rel->rd_locator;
	pscan->phs_syncscan = false;
	pscan->phs_snapshot_any = false;
	return sizeof(ParallelTableScanDescData);
}

static void
columnar_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	/* Nothing to reinitialize */
}

/* ----------------------------------------------------------------
 * Index fetch stubs (not supported)
 * ----------------------------------------------------------------
 */

static IndexFetchTableData *
columnar_index_fetch_begin(Relation rel)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support index scans")));
	return NULL;				/* unreachable */
}

static void
columnar_index_fetch_reset(IndexFetchTableData *data)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support index scans")));
}

static void
columnar_index_fetch_end(IndexFetchTableData *data)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support index scans")));
}

static bool
columnar_index_fetch_tuple(IndexFetchTableData *scan, ItemPointer tid,
						   Snapshot snapshot, TupleTableSlot *slot,
						   bool *call_again, bool *all_dead)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support index scans")));
	return false;				/* unreachable */
}

/* ----------------------------------------------------------------
 * Tuple manipulation callbacks
 * ----------------------------------------------------------------
 */

static void
columnar_tuple_insert(Relation rel, TupleTableSlot *slot, CommandId cid,
					  int options, struct BulkInsertStateData *bistate)
{
	ColumnarWriteState *wstate;

	wstate = columnar_get_write_state(rel);
	if (wstate == NULL)
		wstate = columnar_create_write_state(rel);

	columnar_buffer_row(rel, wstate, slot);

	/*
	 * Set a fake TID for the slot.  Columnar doesn't really have meaningful
	 * TIDs, but some callers expect the slot to have one set after insert.
	 */
	ItemPointerSet(&slot->tts_tid, 0, FirstOffsetNumber);
}

static void
columnar_tuple_insert_speculative(Relation rel, TupleTableSlot *slot,
								  CommandId cid, int options,
								  struct BulkInsertStateData *bistate,
								  uint32 specToken)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support speculative insertion")));
}

static void
columnar_tuple_complete_speculative(Relation rel, TupleTableSlot *slot,
									uint32 specToken, bool succeeded)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support speculative insertion")));
}

static void
columnar_multi_insert(Relation rel, TupleTableSlot **slots, int nslots,
					  CommandId cid, int options,
					  struct BulkInsertStateData *bistate)
{
	ColumnarWriteState *wstate;
	int			i;

	wstate = columnar_get_write_state(rel);
	if (wstate == NULL)
		wstate = columnar_create_write_state(rel);

	for (i = 0; i < nslots; i++)
		columnar_buffer_row(rel, wstate, slots[i]);
}

static TM_Result
columnar_tuple_delete(Relation rel, ItemPointer tid, CommandId cid,
					  Snapshot snapshot, Snapshot crosscheck, bool wait,
					  TM_FailureData *tmfd, bool changingPart)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables are append-only and do not support DELETE")));
	return TM_Ok;				/* unreachable */
}

static TM_Result
columnar_tuple_update(Relation rel, ItemPointer otid, TupleTableSlot *slot,
					  CommandId cid, Snapshot snapshot, Snapshot crosscheck,
					  bool wait, TM_FailureData *tmfd,
					  LockTupleMode *lockmode,
					  TU_UpdateIndexes *update_indexes)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables are append-only and do not support UPDATE")));
	return TM_Ok;				/* unreachable */
}

static TM_Result
columnar_tuple_lock(Relation rel, ItemPointer tid, Snapshot snapshot,
					TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
					LockWaitPolicy wait_policy, uint8 flags,
					TM_FailureData *tmfd)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support row locking")));
	return TM_Ok;				/* unreachable */
}

/* ----------------------------------------------------------------
 * Non-modifying tuple operations
 * ----------------------------------------------------------------
 */

static bool
columnar_fetch_row_version(Relation rel, ItemPointer tid,
						   Snapshot snapshot, TupleTableSlot *slot)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support row version fetch")));
	return false;				/* unreachable */
}

static bool
columnar_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	return false;
}

static void
columnar_tuple_get_latest_tid(TableScanDesc scan, ItemPointer tid)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support get_latest_tid")));
}

static bool
columnar_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
								  Snapshot snapshot)
{
	/* Append-only: all committed rows are visible */
	return true;
}

static TransactionId
columnar_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support index tuple deletion")));
	return InvalidTransactionId;	/* unreachable */
}

/* ----------------------------------------------------------------
 * DDL callbacks
 * ----------------------------------------------------------------
 */

static void
columnar_relation_set_new_filelocator(Relation rel,
									  const RelFileLocator *newrlocator,
									  char persistence,
									  TransactionId *freezeXid,
									  MultiXactId *minmulti)
{
	SMgrRelation srel;

	/*
	 * Columnar tables don't use transaction IDs or multixacts, so set these
	 * to invalid/minimal values.
	 */
	*freezeXid = InvalidTransactionId;
	*minmulti = InvalidMultiXactId;

	srel = RelationCreateStorage(*newrlocator, persistence, true);

	/*
	 * If unlogged, create the init fork.
	 */
	if (persistence == RELPERSISTENCE_UNLOGGED)
	{
		Assert(rel->rd_rel->relkind == RELKIND_RELATION ||
			   rel->rd_rel->relkind == RELKIND_TOASTVALUE);
		smgrcreate(srel, INIT_FORKNUM, false);
		log_smgrcreate(newrlocator, INIT_FORKNUM);
	}

	/*
	 * Do NOT initialize the metadata page here.  During
	 * relation_set_new_filelocator, rel->rd_locator may not yet match
	 * *newrlocator, which causes GenericXLog/buffer issues.  The metadata
	 * page is initialized lazily on the first stripe flush (INSERT).
	 * An empty relation (0 blocks) is treated as an empty table by scan
	 * and estimate_size callbacks.
	 */

	smgrclose(srel);
}

static void
columnar_relation_nontransactional_truncate(Relation rel)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	ColumnarMetaPageData *meta;

	/*
	 * Truncate to just the metadata page, then reinitialize it.
	 */
	RelationTruncate(rel, 1);

	buf = ReadBuffer(rel, COLUMNAR_META_BLOCKNO);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	state = GenericXLogStart(rel);
	page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);

	PageInit(page, BLCKSZ, 0);

	meta = (ColumnarMetaPageData *) PageGetContents(page);
	meta->magic = COLUMNAR_MAGIC;
	meta->version = COLUMNAR_VERSION;
	meta->total_rows = 0;
	meta->num_stripes = 0;
	meta->natts = RelationGetDescr(rel)->natts;
	meta->data_start_block = 1;

	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
}

static void
columnar_relation_copy_data(Relation rel, const RelFileLocator *newrlocator)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support COPY DATA")));
}

static void
columnar_relation_copy_for_cluster(Relation OldTable, Relation NewTable,
								   Relation OldIndex, bool use_sort,
								   TransactionId OldestXmin,
								   TransactionId *xid_cutoff,
								   MultiXactId *multi_cutoff,
								   double *num_tuples,
								   double *tups_vacuumed,
								   double *tups_recently_dead)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support CLUSTER")));
}

static void
columnar_relation_vacuum(Relation rel, struct VacuumParams *params,
						 BufferAccessStrategy bstrategy)
{
	/* No-op for append-only columnar tables */
}

/* ----------------------------------------------------------------
 * ANALYZE support
 * ----------------------------------------------------------------
 */

static bool
columnar_scan_analyze_next_block(TableScanDesc sscan, ReadStream *stream)
{
	ColumnarScanDesc scan = (ColumnarScanDesc) sscan;
	Relation	rel = sscan->rs_rd;
	ColumnarMetaPageData meta;

	/* Empty table */
	if (ColumnarGetNblocks(rel) == 0)
		return false;

	columnar_read_metadata(rel, &meta);

	/* If no stripes or already past all stripes, done */
	if (meta.num_stripes == 0 || scan->current_stripe >= meta.num_stripes)
		return false;

	/* Load the next stripe for ANALYZE */
	columnar_read_stripe(rel, scan, scan->current_stripe);
	scan->current_row = 0;

	return true;
}

static bool
columnar_scan_analyze_next_tuple(TableScanDesc sscan, TransactionId OldestXmin,
								 double *liverows, double *deadrows,
								 TupleTableSlot *slot)
{
	ColumnarScanDesc scan = (ColumnarScanDesc) sscan;
	int			natts;
	int			i;

	if (scan->stripe_values == NULL || scan->current_row >= scan->stripe_rows)
	{
		/* Move to next stripe for the next call to analyze_next_block */
		scan->current_stripe++;
		return false;
	}

	natts = scan->base.rs_rd->rd_att->natts;

	ExecClearTuple(slot);
	for (i = 0; i < natts && i < slot->tts_tupleDescriptor->natts; i++)
	{
		slot->tts_values[i] = scan->stripe_values[i][scan->current_row];
		slot->tts_isnull[i] = scan->stripe_nulls[i][scan->current_row];
	}
	for (; i < slot->tts_tupleDescriptor->natts; i++)
	{
		slot->tts_values[i] = (Datum) 0;
		slot->tts_isnull[i] = true;
	}
	ExecStoreVirtualTuple(slot);

	scan->current_row++;
	(*liverows)++;

	return true;
}

/* ----------------------------------------------------------------
 * Index build stubs
 * ----------------------------------------------------------------
 */

static double
columnar_index_build_range_scan(Relation table_rel, Relation index_rel,
								struct IndexInfo *index_info,
								bool allow_sync, bool anyvisible,
								bool progress, BlockNumber start_blockno,
								BlockNumber numblocks,
								IndexBuildCallback callback,
								void *callback_state,
								TableScanDesc scan)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support index creation")));
	return 0;					/* unreachable */
}

static void
columnar_index_validate_scan(Relation table_rel, Relation index_rel,
							 struct IndexInfo *index_info,
							 Snapshot snapshot,
							 struct ValidateIndexState *state)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support index validation")));
}

/* ----------------------------------------------------------------
 * Miscellaneous callbacks
 * ----------------------------------------------------------------
 */

static uint64
columnar_relation_size(Relation rel, ForkNumber forkNumber)
{
	/*
	 * Use smgrnblocks directly.  See ColumnarGetNblocks() comment in header.
	 */
	return (uint64) smgrnblocks(RelationGetSmgr(rel), forkNumber) *
		(uint64) BLCKSZ;
}

static bool
columnar_relation_needs_toast_table(Relation rel)
{
	/* Columnar handles large values internally via compression */
	return false;
}

static Oid
columnar_relation_toast_am(Relation rel)
{
	return InvalidOid;
}

static void
columnar_relation_fetch_toast_slice(Relation toastrel, Oid valueid,
									int32 attrsize, int32 sliceoffset,
									int32 slicelength, struct varlena *result)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not use TOAST")));
}

static void
columnar_relation_estimate_size(Relation rel, int32 *attr_widths,
								BlockNumber *pages, double *tuples,
								double *allvisfrac)
{
	BlockNumber nblocks;
	ColumnarMetaPageData meta;

	nblocks = ColumnarGetNblocks(rel);
	*pages = nblocks;

	if (nblocks == 0)
	{
		*tuples = 0;
		*allvisfrac = 0;
		return;
	}

	columnar_read_metadata(rel, &meta);
	*tuples = (double) meta.total_rows;
	*allvisfrac = 1.0;			/* append-only: all visible */
}

/* ----------------------------------------------------------------
 * Bitmap scan stub (optional callback)
 * ----------------------------------------------------------------
 */

static bool
columnar_scan_bitmap_next_tuple(TableScanDesc scan, TupleTableSlot *slot,
								bool *recheck, uint64 *lossy_pages,
								uint64 *exact_pages)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support bitmap scans")));
	return false;				/* unreachable */
}

/* ----------------------------------------------------------------
 * Sample scan stubs
 * ----------------------------------------------------------------
 */

static bool
columnar_scan_sample_next_block(TableScanDesc scan,
								struct SampleScanState *scanstate)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support TABLESAMPLE")));
	return false;
}

static bool
columnar_scan_sample_next_tuple(TableScanDesc scan,
								struct SampleScanState *scanstate,
								TupleTableSlot *slot)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support TABLESAMPLE")));
	return false;
}

/* ----------------------------------------------------------------
 * Finish bulk insert - flush any remaining buffered rows
 * ----------------------------------------------------------------
 */

static void
columnar_finish_bulk_insert(Relation rel, int options)
{
	ColumnarWriteState *wstate;

	wstate = columnar_get_write_state(rel);
	if (wstate != NULL && wstate->buffered_rows > 0)
		columnar_flush_stripe(rel, wstate);
}

/* ----------------------------------------------------------------
 * The TableAmRoutine definition
 * ----------------------------------------------------------------
 */

static const TableAmRoutine columnar_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = columnar_slot_callbacks,

	.scan_begin = columnar_scan_begin,
	.scan_end = columnar_scan_end,
	.scan_rescan = columnar_scan_rescan,
	.scan_getnextslot = columnar_scan_getnextslot,

	.scan_set_tidrange = columnar_scan_set_tidrange,
	.scan_getnextslot_tidrange = columnar_scan_getnextslot_tidrange,

	.parallelscan_estimate = columnar_parallelscan_estimate,
	.parallelscan_initialize = columnar_parallelscan_initialize,
	.parallelscan_reinitialize = columnar_parallelscan_reinitialize,

	.index_fetch_begin = columnar_index_fetch_begin,
	.index_fetch_reset = columnar_index_fetch_reset,
	.index_fetch_end = columnar_index_fetch_end,
	.index_fetch_tuple = columnar_index_fetch_tuple,

	.tuple_insert = columnar_tuple_insert,
	.tuple_insert_speculative = columnar_tuple_insert_speculative,
	.tuple_complete_speculative = columnar_tuple_complete_speculative,
	.multi_insert = columnar_multi_insert,
	.tuple_delete = columnar_tuple_delete,
	.tuple_update = columnar_tuple_update,
	.tuple_lock = columnar_tuple_lock,

	.finish_bulk_insert = columnar_finish_bulk_insert,

	.tuple_fetch_row_version = columnar_fetch_row_version,
	.tuple_tid_valid = columnar_tuple_tid_valid,
	.tuple_get_latest_tid = columnar_tuple_get_latest_tid,
	.tuple_satisfies_snapshot = columnar_tuple_satisfies_snapshot,
	.index_delete_tuples = columnar_index_delete_tuples,

	.relation_set_new_filelocator = columnar_relation_set_new_filelocator,
	.relation_nontransactional_truncate = columnar_relation_nontransactional_truncate,
	.relation_copy_data = columnar_relation_copy_data,
	.relation_copy_for_cluster = columnar_relation_copy_for_cluster,
	.relation_vacuum = columnar_relation_vacuum,
	.scan_analyze_next_block = columnar_scan_analyze_next_block,
	.scan_analyze_next_tuple = columnar_scan_analyze_next_tuple,
	.index_build_range_scan = columnar_index_build_range_scan,
	.index_validate_scan = columnar_index_validate_scan,

	.relation_size = columnar_relation_size,
	.relation_needs_toast_table = columnar_relation_needs_toast_table,
	.relation_toast_am = columnar_relation_toast_am,
	.relation_fetch_toast_slice = columnar_relation_fetch_toast_slice,

	.relation_estimate_size = columnar_relation_estimate_size,

	.scan_bitmap_next_tuple = columnar_scan_bitmap_next_tuple,
	.scan_sample_next_block = columnar_scan_sample_next_block,
	.scan_sample_next_tuple = columnar_scan_sample_next_tuple,
};

/* ----------------------------------------------------------------
 * Handler function
 * ----------------------------------------------------------------
 */

Datum
columnar_tableam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&columnar_methods);
}

/* ----------------------------------------------------------------
 * Module initialization
 * ----------------------------------------------------------------
 */

void
_PG_init(void)
{
	DefineCustomIntVariable("alohadb.columnar_stripe_row_count",
							"Number of rows per columnar stripe.",
							NULL,
							&columnar_stripe_rows,
							COLUMNAR_DEFAULT_STRIPE_ROWS,
							1000,
							10000000,
							PGC_SUSET,
							0,
							NULL, NULL, NULL);

	MarkGUCPrefixReserved("alohadb.columnar");

	/* Register xact callback to flush pending writes at commit */
	RegisterXactCallback(columnar_xact_callback, NULL);
}

/* ----------------------------------------------------------------
 * alohadb_columnar_info() - introspection function
 * ----------------------------------------------------------------
 */

Datum
alohadb_columnar_info(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Relation	rel;
	ColumnarMetaPageData meta;
	TupleDesc	tupdesc;
	Datum		values[4];
	bool		nulls[4] = {false};
	HeapTuple	htup;
	int64		total_compressed = 0;
	int64		total_uncompressed = 0;
	int			i;
	char		compression_buf[128];

	rel = table_open(relid, AccessShareLock);

	/* Build the result tuple descriptor */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	if (ColumnarGetNblocks(rel) == 0)
	{
		table_close(rel, AccessShareLock);

		values[0] = Int64GetDatum(0);
		values[1] = Int64GetDatum(0);
		values[2] = Int64GetDatum(0);
		values[3] = CStringGetTextDatum("zstd (no data)");

		htup = heap_form_tuple(tupdesc, values, nulls);
		PG_RETURN_DATUM(HeapTupleGetDatum(htup));
	}

	columnar_read_metadata(rel, &meta);

	/* Sum up stripe sizes */
	for (i = 0; i < meta.num_stripes; i++)
	{
		ColumnarStripeInfo sinfo;

		columnar_read_stripe_info(rel, i, &sinfo);
		total_compressed += sinfo.compressed_size;
		total_uncompressed += sinfo.uncompressed_size;
	}

	table_close(rel, AccessShareLock);

	/* Build compression info string */
	if (total_compressed > 0)
		snprintf(compression_buf, sizeof(compression_buf),
				 "zstd (ratio: %.2fx)",
				 (double) total_uncompressed / (double) total_compressed);
	else
		snprintf(compression_buf, sizeof(compression_buf), "zstd (no data)");

	values[0] = Int64GetDatum((int64) meta.num_stripes);
	values[1] = Int64GetDatum(meta.total_rows);
	values[2] = Int64GetDatum(total_compressed);
	values[3] = CStringGetTextDatum(compression_buf);

	htup = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}
