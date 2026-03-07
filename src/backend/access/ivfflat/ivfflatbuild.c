/*-------------------------------------------------------------------------
 *
 * ivfflatbuild.c
 *	  Build routines for the IVFFlat index access method.
 *
 * The build process works in three phases:
 *
 *   1. Sample: scan the heap and collect up to IVFFLAT_KMEANS_SAMPLE_SIZE
 *      vectors for k-means training.  This scan also counts tuples.
 *
 *   2. Train: run Lloyd's k-means algorithm on the sample to produce
 *      num_lists centroid vectors.  The algorithm runs for at most
 *      IVFFLAT_KMEANS_MAX_ITERATIONS iterations, stopping early if
 *      centroids converge.
 *
 *   3. Write: write the metapage and centroid pages first, then scan
 *      the heap a second time to assign each vector to its nearest
 *      centroid and append it to the corresponding posting list pages.
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * IDENTIFICATION
 *	  src/backend/access/ivfflat/ivfflatbuild.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/generic_xlog.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "common/pg_prng.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "utils/memutils.h"
#include "utils/vector.h"

#include "ivfflat.h"

/* ----------
 * Forward declarations
 * ----------
 */
static void ivfflat_build_sample_callback(Relation index, ItemPointer tid,
										  Datum *values, bool *isnull,
										  bool tupleIsAlive, void *state);
static void ivfflat_build_assign_callback(Relation index, ItemPointer tid,
										  Datum *values, bool *isnull,
										  bool tupleIsAlive, void *state);
static void ivfflat_kmeans(IvfflatBuildState *buildstate);
static int	ivfflat_nearest_centroid(IvfflatBuildState *buildstate,
									 const Vector *vec);
static void ivfflat_write_metapage(Relation index,
								   IvfflatBuildState *buildstate);
static void ivfflat_write_centroid_pages(Relation index,
										 IvfflatBuildState *buildstate);
static void ivfflat_append_entry(Relation index,
								 IvfflatBuildState *buildstate,
								 int list_idx, ItemPointer tid,
								 Vector *vec);
static void ivfflat_flush_page(Relation index,
							   IvfflatBuildState *buildstate);

/*
 * Callback for sampling phase -- collect vectors for k-means training.
 */
static void
ivfflat_build_sample_callback(Relation index, ItemPointer tid,
							  Datum *values, bool *isnull,
							  bool tupleIsAlive, void *state)
{
	IvfflatBuildState *buildstate = (IvfflatBuildState *) state;
	Vector	   *vec;
	Vector	   *copy;

	/* Skip NULLs */
	if (isnull[0])
		return;

	vec = DatumGetVector(values[0]);

	/* Set dimensions on first vector */
	if (buildstate->dimensions == 0)
		buildstate->dimensions = vec->dim;
	else if (vec->dim != buildstate->dimensions)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("ivfflat index expects vectors with %d dimensions, got %d",
						buildstate->dimensions, vec->dim)));

	buildstate->heap_tuples++;

	/* Reservoir sampling */
	if (buildstate->num_samples < buildstate->max_samples)
	{
		copy = InitVector(vec->dim);
		memcpy(copy->x, vec->x, sizeof(float) * vec->dim);
		buildstate->samples[buildstate->num_samples++] = copy;
	}
	else
	{
		/*
		 * Replace a random element with probability max_samples/heap_tuples.
		 */
		int64		j = pg_prng_int64_range(&pg_global_prng_state, 0,
											(int64) buildstate->heap_tuples - 1);

		if (j < buildstate->max_samples)
		{
			pfree(buildstate->samples[j]);
			copy = InitVector(vec->dim);
			memcpy(copy->x, vec->x, sizeof(float) * vec->dim);
			buildstate->samples[j] = copy;
		}
	}
}

/*
 * Callback for assignment phase -- assign each vector to nearest centroid
 * and write it to the posting list.
 */
static void
ivfflat_build_assign_callback(Relation index, ItemPointer tid,
							  Datum *values, bool *isnull,
							  bool tupleIsAlive, void *state)
{
	IvfflatBuildState *buildstate = (IvfflatBuildState *) state;
	Vector	   *vec;
	int			list_idx;

	/* Skip NULLs */
	if (isnull[0])
		return;

	vec = DatumGetVector(values[0]);

	/* Find nearest centroid */
	list_idx = ivfflat_nearest_centroid(buildstate, vec);

	/* Append to posting list */
	ivfflat_append_entry(buildstate->index, buildstate, list_idx, tid, vec);

	buildstate->index_tuples++;
}

/*
 * Find the index of the nearest centroid to the given vector.
 */
static int
ivfflat_nearest_centroid(IvfflatBuildState *buildstate, const Vector *vec)
{
	int			best = 0;
	double		best_dist = buildstate->distfunc(vec,
												 buildstate->centroids[0]);
	int			i;

	for (i = 1; i < buildstate->num_lists; i++)
	{
		double		dist = buildstate->distfunc(vec,
												buildstate->centroids[i]);

		if (dist < best_dist)
		{
			best_dist = dist;
			best = i;
		}
	}

	return best;
}

/*
 * Run Lloyd's k-means clustering algorithm on the sample vectors.
 *
 * The result is stored in buildstate->centroids.
 */
static void
ivfflat_kmeans(IvfflatBuildState *buildstate)
{
	int			k = buildstate->num_lists;
	int			n = buildstate->num_samples;
	int			dim = buildstate->dimensions;
	int			iter;
	int		   *assignments;
	int		   *counts;
	double	  **sums;
	bool		converged;
	int			i,
				j;

	/*
	 * If fewer samples than lists, reduce the number of lists.
	 */
	if (n < k)
	{
		elog(NOTICE, "ivfflat: reducing lists from %d to %d "
			 "(not enough training data)", k, n);
		k = n;
		buildstate->num_lists = k;
	}

	/* Allocate centroids */
	buildstate->centroids = (Vector **) palloc(sizeof(Vector *) * k);
	for (i = 0; i < k; i++)
		buildstate->centroids[i] = InitVector(dim);

	/*
	 * Initialize centroids by randomly choosing k distinct samples.
	 * Use Fisher-Yates partial shuffle.
	 */
	{
		int		   *perm = (int *) palloc(sizeof(int) * n);

		for (i = 0; i < n; i++)
			perm[i] = i;

		for (i = 0; i < k; i++)
		{
			int			r = i + (int) pg_prng_int64_range(
							   &pg_global_prng_state, 0, n - 1 - i);
			int			tmp = perm[i];

			perm[i] = perm[r];
			perm[r] = tmp;

			memcpy(buildstate->centroids[i]->x,
				   buildstate->samples[perm[i]]->x,
				   sizeof(float) * dim);
		}

		pfree(perm);
	}

	/* Working arrays for k-means iterations */
	assignments = (int *) palloc(sizeof(int) * n);
	counts = (int *) palloc(sizeof(int) * k);
	sums = (double **) palloc(sizeof(double *) * k);
	for (i = 0; i < k; i++)
		sums[i] = (double *) palloc(sizeof(double) * dim);

	/*
	 * Lloyd's algorithm: iterate until convergence or max iterations.
	 */
	for (iter = 0; iter < IVFFLAT_KMEANS_MAX_ITERATIONS; iter++)
	{
		CHECK_FOR_INTERRUPTS();

		converged = true;

		/* Reset accumulators */
		memset(counts, 0, sizeof(int) * k);
		for (i = 0; i < k; i++)
			memset(sums[i], 0, sizeof(double) * dim);

		/* Assignment step: assign each sample to nearest centroid */
		for (i = 0; i < n; i++)
		{
			int			best = 0;
			double		best_dist;

			best_dist = buildstate->distfunc(buildstate->samples[i],
											 buildstate->centroids[0]);

			for (j = 1; j < k; j++)
			{
				double		dist;

				dist = buildstate->distfunc(buildstate->samples[i],
											buildstate->centroids[j]);

				if (dist < best_dist)
				{
					best_dist = dist;
					best = j;
				}
			}

			if (iter == 0 || assignments[i] != best)
				converged = false;

			assignments[i] = best;
			counts[best]++;

			for (j = 0; j < dim; j++)
				sums[best][j] += (double) buildstate->samples[i]->x[j];
		}

		if (converged)
			break;

		/* Update step: recompute centroids as mean of assigned vectors */
		for (i = 0; i < k; i++)
		{
			if (counts[i] > 0)
			{
				for (j = 0; j < dim; j++)
					buildstate->centroids[i]->x[j] =
						(float) (sums[i][j] / counts[i]);
			}
			/* else: centroid keeps its previous value (empty cluster) */
		}
	}

	/* Cleanup */
	for (i = 0; i < k; i++)
		pfree(sums[i]);
	pfree(sums);
	pfree(counts);
	pfree(assignments);
}

/*
 * Write the metapage (block 0) for the index.
 */
static void
ivfflat_write_metapage(Relation index, IvfflatBuildState *buildstate)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	IvfflatMetaPageData *metadata;

	buf = ReadBuffer(index, P_NEW);
	Assert(BufferGetBlockNumber(buf) == IVFFLAT_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);

	ivfflat_page_init(page, IVFFLAT_PAGE_META);

	metadata = (IvfflatMetaPageData *) PageGetContents(page);
	metadata->magic = IVFFLAT_META_MAGIC;
	metadata->num_lists = buildstate->num_lists;
	metadata->dimensions = buildstate->dimensions;
	metadata->num_vectors = 0;	/* will be updated after assignment */
	metadata->first_centroid_blkno = IVFFLAT_METAPAGE_BLKNO + 1;

	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
}

/*
 * Write centroid pages containing the centroid list descriptors.
 *
 * The posting list pointers (first/last blkno) are initialized to
 * InvalidBlockNumber and will be updated after the assignment phase.
 */
static void
ivfflat_write_centroid_pages(Relation index, IvfflatBuildState *buildstate)
{
	int			dim = buildstate->dimensions;
	Size		entry_size;
	Buffer		buf = InvalidBuffer;
	Page		page = NULL;
	GenericXLogState *xlog_state = NULL;
	int			i;

	entry_size = MAXALIGN(IVFFLAT_LIST_SIZE(dim));

	for (i = 0; i < buildstate->num_lists; i++)
	{
		IvfflatListData *list;
		Vector	   *centroid;
		Size		freespace;

		CHECK_FOR_INTERRUPTS();

		/* Get a page with enough space, or allocate a new one */
		if (!BufferIsValid(buf))
		{
			buf = ReadBuffer(index, P_NEW);
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
			xlog_state = GenericXLogStart(index);
			page = GenericXLogRegisterBuffer(xlog_state, buf,
											 GENERIC_XLOG_FULL_IMAGE);
			ivfflat_page_init(page, IVFFLAT_PAGE_CENTROID);
		}

		freespace = PageGetFreeSpace(page);
		if (freespace < entry_size)
		{
			/* Finish current page and start a new one */
			GenericXLogFinish(xlog_state);
			UnlockReleaseBuffer(buf);

			buf = ReadBuffer(index, P_NEW);
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
			xlog_state = GenericXLogStart(index);
			page = GenericXLogRegisterBuffer(xlog_state, buf,
											 GENERIC_XLOG_FULL_IMAGE);
			ivfflat_page_init(page, IVFFLAT_PAGE_CENTROID);
		}

		/* Write the list descriptor + centroid vector */
		list = (IvfflatListData *) ((char *) page +
									((PageHeader) page)->pd_lower);
		list->first_posting_blkno = InvalidBlockNumber;
		list->last_posting_blkno = InvalidBlockNumber;
		list->num_entries = 0;

		centroid = IvfflatListGetCentroid(list);
		SET_VARSIZE(centroid, VECTOR_SIZE(dim));
		centroid->dim = dim;
		centroid->unused = 0;
		memcpy(centroid->x, buildstate->centroids[i]->x,
			   sizeof(float) * dim);

		((PageHeader) page)->pd_lower += entry_size;
	}

	/* Finish the last page */
	if (BufferIsValid(buf))
	{
		GenericXLogFinish(xlog_state);
		UnlockReleaseBuffer(buf);
	}
}

/*
 * Append a vector entry to the posting list of the given centroid list.
 *
 * During build, posting list pages are allocated sequentially and chained
 * together via the nextblkno field in the page opaque data.
 */
static void
ivfflat_append_entry(Relation index, IvfflatBuildState *buildstate,
					 int list_idx, ItemPointer tid, Vector *vec)
{
	int			dim = buildstate->dimensions;
	Size		entry_size = IVFFLAT_ENTRY_SIZE(dim);
	IvfflatEntryData *entry;
	Vector	   *entry_vec;

	/*
	 * If we don't have a current buffer, or the current page belongs to a
	 * different list, or the current page is full, allocate a new page.
	 */
	if (!BufferIsValid(buildstate->current_buffer) ||
		buildstate->current_list != list_idx ||
		PageGetFreeSpace(buildstate->current_page) < entry_size)
	{
		BlockNumber new_blkno;

		/* Flush current page if we have one */
		if (BufferIsValid(buildstate->current_buffer))
			ivfflat_flush_page(index, buildstate);

		/* Allocate a new posting list page */
		buildstate->current_buffer = ReadBuffer(index, P_NEW);
		new_blkno = BufferGetBlockNumber(buildstate->current_buffer);
		LockBuffer(buildstate->current_buffer, BUFFER_LOCK_EXCLUSIVE);

		buildstate->current_page =
			BufferGetPage(buildstate->current_buffer);
		ivfflat_page_init(buildstate->current_page, IVFFLAT_PAGE_POSTING);

		/* Update the previous tail page's nextblkno to point to new page */
		if (buildstate->list_last_blkno[list_idx] != InvalidBlockNumber)
		{
			Buffer		prev_buf;
			Page		prev_page;
			GenericXLogState *prev_state;

			prev_buf = ReadBuffer(index,
								  buildstate->list_last_blkno[list_idx]);
			LockBuffer(prev_buf, BUFFER_LOCK_EXCLUSIVE);
			prev_state = GenericXLogStart(index);
			prev_page = GenericXLogRegisterBuffer(prev_state, prev_buf, 0);
			IvfflatPageGetOpaque(prev_page)->nextblkno = new_blkno;
			GenericXLogFinish(prev_state);
			UnlockReleaseBuffer(prev_buf);
		}

		/* Track list chain */
		if (buildstate->list_first_blkno[list_idx] == InvalidBlockNumber)
			buildstate->list_first_blkno[list_idx] = new_blkno;

		buildstate->list_last_blkno[list_idx] = new_blkno;
		buildstate->current_list = list_idx;
	}

	/* Write the entry at pd_lower */
	entry = (IvfflatEntryData *) ((char *) buildstate->current_page +
								  ((PageHeader) buildstate->current_page)->pd_lower);
	entry->heap_tid = *tid;

	entry_vec = IvfflatEntryGetVector(entry);
	SET_VARSIZE(entry_vec, VECTOR_SIZE(dim));
	entry_vec->dim = dim;
	entry_vec->unused = 0;
	memcpy(entry_vec->x, vec->x, sizeof(float) * dim);

	((PageHeader) buildstate->current_page)->pd_lower += entry_size;

	buildstate->list_counts[list_idx]++;
}

/*
 * Flush the current posting list page to disk using WAL.
 */
static void
ivfflat_flush_page(Relation index, IvfflatBuildState *buildstate)
{
	GenericXLogState *state;

	if (!BufferIsValid(buildstate->current_buffer))
		return;

	state = GenericXLogStart(index);
	GenericXLogRegisterBuffer(state, buildstate->current_buffer,
							 GENERIC_XLOG_FULL_IMAGE);
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buildstate->current_buffer);

	buildstate->current_buffer = InvalidBuffer;
	buildstate->current_page = NULL;
}

/*
 * Update centroid pages with the posting list block numbers determined
 * during the assignment phase.
 */
static void
ivfflat_update_centroid_pages(Relation index, IvfflatBuildState *buildstate)
{
	int			dim = buildstate->dimensions;
	Size		centry_size = MAXALIGN(IVFFLAT_LIST_SIZE(dim));
	BlockNumber cblkno;
	int			list_idx = 0;

	cblkno = IVFFLAT_METAPAGE_BLKNO + 1;

	while (list_idx < buildstate->num_lists)
	{
		Buffer		cbuf;
		Page		cpage;
		GenericXLogState *cstate;
		char	   *ptr;
		char	   *end;

		cbuf = ReadBuffer(index, cblkno);
		LockBuffer(cbuf, BUFFER_LOCK_EXCLUSIVE);
		cstate = GenericXLogStart(index);
		cpage = GenericXLogRegisterBuffer(cstate, cbuf, 0);

		ptr = (char *) PageGetContents(cpage);
		end = (char *) cpage + ((PageHeader) cpage)->pd_lower;

		while (ptr + centry_size <= end && list_idx < buildstate->num_lists)
		{
			IvfflatListData *list = (IvfflatListData *) ptr;

			list->first_posting_blkno =
				buildstate->list_first_blkno[list_idx];
			list->last_posting_blkno =
				buildstate->list_last_blkno[list_idx];
			list->num_entries = buildstate->list_counts[list_idx];

			ptr += centry_size;
			list_idx++;
		}

		GenericXLogFinish(cstate);
		UnlockReleaseBuffer(cbuf);
		cblkno++;
	}
}

/*
 * Update the metapage with the final vector count.
 */
static void
ivfflat_update_metapage(Relation index, uint32 num_vectors)
{
	Buffer		meta_buf;
	Page		meta_page;
	GenericXLogState *meta_state;
	IvfflatMetaPageData *metadata;

	meta_buf = ReadBuffer(index, IVFFLAT_METAPAGE_BLKNO);
	LockBuffer(meta_buf, BUFFER_LOCK_EXCLUSIVE);
	meta_state = GenericXLogStart(index);
	meta_page = GenericXLogRegisterBuffer(meta_state, meta_buf, 0);

	metadata = (IvfflatMetaPageData *) PageGetContents(meta_page);
	metadata->num_vectors = num_vectors;

	GenericXLogFinish(meta_state);
	UnlockReleaseBuffer(meta_buf);
}

/*
 * ivfflatbuild - build a new IVFFlat index.
 */
IndexBuildResult *
ivfflatbuild(Relation heap, Relation index, struct IndexInfo *indexInfo)
{
	IndexBuildResult *result;
	IvfflatBuildState buildstate;
	MemoryContext oldctx;
	MemoryContext buildctx;
	int			i;

	buildctx = AllocSetContextCreate(CurrentMemoryContext,
									 "IVFFlat build",
									 ALLOCSET_DEFAULT_SIZES);
	oldctx = MemoryContextSwitchTo(buildctx);

	/* Initialize build state */
	buildstate.heap = heap;
	buildstate.index = index;
	buildstate.indexInfo = indexInfo;
	buildstate.dimensions = 0;
	buildstate.num_lists = IvfflatGetLists(index);
	buildstate.max_samples = IVFFLAT_KMEANS_SAMPLE_SIZE;
	buildstate.num_samples = 0;
	buildstate.samples = (Vector **) palloc0(sizeof(Vector *) *
											 buildstate.max_samples);
	buildstate.centroids = NULL;
	buildstate.heap_tuples = 0;
	buildstate.index_tuples = 0;
	buildstate.current_buffer = InvalidBuffer;
	buildstate.current_page = NULL;
	buildstate.current_list = -1;

	/* Default to L2 squared distance for training and assignment */
	buildstate.distfunc = vector_l2_squared_distance;

	/*
	 * Phase 1: Sample vectors from heap for k-means training.
	 */
	table_index_build_scan(heap, index, indexInfo,
						   true, false,
						   ivfflat_build_sample_callback,
						   &buildstate, NULL);

	/* If no vectors found, write a minimal index */
	if (buildstate.num_samples == 0 || buildstate.dimensions == 0)
	{
		buildstate.dimensions = 1;	/* placeholder */
		buildstate.num_lists = 1;
		buildstate.centroids = (Vector **) palloc(sizeof(Vector *));
		buildstate.centroids[0] = InitVector(buildstate.dimensions);
		buildstate.centroids[0]->x[0] = 0.0f;
	}
	else
	{
		/*
		 * Phase 2: Run k-means clustering on the sample.
		 */
		ivfflat_kmeans(&buildstate);
	}

	/*
	 * Initialize per-list tracking arrays.
	 */
	buildstate.list_first_blkno = (BlockNumber *) palloc(
		sizeof(BlockNumber) * buildstate.num_lists);
	buildstate.list_last_blkno = (BlockNumber *) palloc(
		sizeof(BlockNumber) * buildstate.num_lists);
	buildstate.list_counts = (uint32 *) palloc0(
		sizeof(uint32) * buildstate.num_lists);

	for (i = 0; i < buildstate.num_lists; i++)
	{
		buildstate.list_first_blkno[i] = InvalidBlockNumber;
		buildstate.list_last_blkno[i] = InvalidBlockNumber;
	}

	/*
	 * Phase 3a: Write metapage and centroid pages.  These occupy the first
	 * blocks of the index file, in order: metapage (block 0), then one or
	 * more centroid pages.
	 */
	ivfflat_write_metapage(index, &buildstate);
	ivfflat_write_centroid_pages(index, &buildstate);

	/*
	 * Phase 3b: Scan heap again, assign each vector to its nearest centroid,
	 * and write posting list pages.  Since the metapage and centroid pages
	 * have already been written, posting list pages are allocated at
	 * sequentially increasing block numbers after them.
	 */
	buildstate.heap_tuples = 0;
	buildstate.index_tuples = 0;

	table_index_build_scan(heap, index, indexInfo,
						   true, true,
						   ivfflat_build_assign_callback,
						   &buildstate, NULL);

	/* Flush any remaining buffered page */
	if (BufferIsValid(buildstate.current_buffer))
		ivfflat_flush_page(index, &buildstate);

	/*
	 * Phase 4: Update the centroid pages with posting list block numbers
	 * and the metapage with the total vector count.
	 */
	ivfflat_update_centroid_pages(index, &buildstate);
	ivfflat_update_metapage(index, (uint32) buildstate.index_tuples);

	/* Build result */
	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples = buildstate.heap_tuples;
	result->index_tuples = buildstate.index_tuples;

	MemoryContextSwitchTo(oldctx);
	MemoryContextDelete(buildctx);

	return result;
}

/*
 * ivfflatbuildempty - build an empty IVFFlat index on an unlogged table.
 */
void
ivfflatbuildempty(Relation index)
{
	Buffer		metabuf;
	Page		page;
	IvfflatMetaPageData *metadata;

	/* An empty IVFFlat index has a metapage only. */
	metabuf = ExtendBufferedRel(BMR_REL(index), INIT_FORKNUM, NULL,
								EB_LOCK_FIRST | EB_SKIP_EXTENSION_LOCK);

	START_CRIT_SECTION();

	page = BufferGetPage(metabuf);
	ivfflat_page_init(page, IVFFLAT_PAGE_META);

	metadata = (IvfflatMetaPageData *) PageGetContents(page);
	metadata->magic = IVFFLAT_META_MAGIC;
	metadata->num_lists = 0;
	metadata->dimensions = 0;
	metadata->num_vectors = 0;
	metadata->first_centroid_blkno = InvalidBlockNumber;

	MarkBufferDirty(metabuf);
	log_newpage_buffer(metabuf, true);

	END_CRIT_SECTION();

	UnlockReleaseBuffer(metabuf);
}
