/*-------------------------------------------------------------------------
 *
 * vector_ivfflat.c
 *	  IVFFlat (Inverted File with Flat quantization) index for vectors.
 *
 *	  Algorithm lineage (all prior art, no active patents):
 *	    k-means training: Lloyd's algorithm (S. P. Lloyd, 1957 Bell
 *	      Labs memo; published IEEE Trans. Inf. Theory, 1982).
 *	    Inverted file search: Salton et al., 1968.  Widespread since
 *	      the 1970s.
 *
 *	  HNSW (US10380099) is deliberately NOT implemented.
 *
 *	  Index layout (SQL tables, not a native index AM):
 *	    alohadb_vector_indexes   — metadata per index
 *	    alohadb_vector_centroids — k cluster centroids
 *	    alohadb_vector_entries   — (centroid_id, entry_id, vec)
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_vector/vector_ivfflat.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"

#include "alohadb_vector.h"

#include <math.h>
#include <stdlib.h>
#include <string.h>

/*
 * Note: the AlohaDB server binary exports its own vector_in(cstring, oid,
 * typmod), which the dynamic linker resolves ahead of this module's
 * one-argument version.  Always pass all three arguments.
 */
PG_FUNCTION_INFO_V1(vector_index_insert);
PG_FUNCTION_INFO_V1(vector_index_search);
PG_FUNCTION_INFO_V1(vector_index_rebuild);

/* ----------------------------------------------------------------
 * find_nearest_centroid
 *
 * Given a query vector and an array of centroids, return the index
 * of the nearest centroid under the specified metric.
 * ---------------------------------------------------------------- */
static int
find_nearest_centroid(Vector *query,
					  Vector **centroids, int n_centroids,
					  const char *metric)
{
	int		best = 0;
	float8	best_dist;
	int		i;

	if (n_centroids == 0)
		return -1;

	if (strcmp(metric, "cosine") == 0)
		best_dist = vector_cosine_dist(query, centroids[0]);
	else if (strcmp(metric, "ip") == 0)
		best_dist = -vector_dot_product(query, centroids[0]);
	else
		best_dist = vector_l2_dist(query, centroids[0]);

	for (i = 1; i < n_centroids; i++)
	{
		float8 d;

		if (strcmp(metric, "cosine") == 0)
			d = vector_cosine_dist(query, centroids[i]);
		else if (strcmp(metric, "ip") == 0)
			d = -vector_dot_product(query, centroids[i]);
		else
			d = vector_l2_dist(query, centroids[i]);

		if (d < best_dist)
		{
			best_dist = d;
			best = i;
		}
	}
	return best;
}

/* ----------------------------------------------------------------
 * vector_index_insert
 *
 * Insert a single vector into a trained IVFFlat index.
 * Finds the nearest centroid and writes to alohadb_vector_entries.
 * ---------------------------------------------------------------- */
Datum
vector_index_insert(PG_FUNCTION_ARGS)
{
	char	   *index_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *entry_id   = text_to_cstring(PG_GETARG_TEXT_PP(1));
	Vector	   *vec        = PG_GETARG_VECTOR(2);

	int				ret;
	uint64			ncentroids;
	Vector		  **centroids;
	char		   *metric;
	int				best_centroid;
	StringInfoData	sql;
	uint64			i;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* Load centroids */
	initStringInfo(&sql);
	appendStringInfo(&sql,
		"SELECT c.centroid_id, c.centroid::text, ix.distance_metric "
		"FROM   public.alohadb_vector_centroids c "
		"JOIN   public.alohadb_vector_indexes ix "
		"       ON ix.index_name = c.index_name "
		"WHERE  c.index_name = %s "
		"ORDER BY c.centroid_id",
		quote_literal_cstr(index_name));

	ret = SPI_execute(sql.data, true, 0);
	pfree(sql.data);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
	{
		PopActiveSnapshot();
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_NO_DATA_FOUND),
				 errmsg("vector_index_insert: index \"%s\" not found or not trained",
						index_name)));
	}

	ncentroids = SPI_processed;
	centroids  = (Vector **) palloc(sizeof(Vector *) * ncentroids);
	metric     = SPI_getvalue(SPI_tuptable->vals[0],
							  SPI_tuptable->tupdesc, 3);
	metric     = pstrdup(metric ? metric : "l2");

	for (i = 0; i < ncentroids; i++)
	{
		char   *vec_str = SPI_getvalue(SPI_tuptable->vals[i],
									   SPI_tuptable->tupdesc, 2);
		centroids[i] = (Vector *) DirectFunctionCall3(
			vector_in, CStringGetDatum(vec_str),
			ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
	}

	best_centroid = find_nearest_centroid(vec, centroids,
										  (int) ncentroids, metric);

	/* Upsert into entries */
	initStringInfo(&sql);
	appendStringInfo(&sql,
		"INSERT INTO public.alohadb_vector_entries "
		"    (index_name, centroid_id, entry_id, vec) "
		"VALUES (%s, %d, %s, %s) "
		"ON CONFLICT (index_name, entry_id) "
		"DO UPDATE SET centroid_id = EXCLUDED.centroid_id, "
		"              vec = EXCLUDED.vec",
		quote_literal_cstr(index_name),
		best_centroid,
		quote_literal_cstr(entry_id),
		quote_literal_cstr(DatumGetCString(
			DirectFunctionCall1(vector_out, PointerGetDatum(vec)))));

	SPI_execute(sql.data, false, 0);
	pfree(sql.data);

	/* Update vector count */
	initStringInfo(&sql);
	appendStringInfo(&sql,
		"UPDATE public.alohadb_vector_indexes "
		"SET vector_count = ("
		"    SELECT count(*) FROM public.alohadb_vector_entries "
		"    WHERE index_name = %s"
		") WHERE index_name = %s",
		quote_literal_cstr(index_name),
		quote_literal_cstr(index_name));
	SPI_execute(sql.data, false, 0);
	pfree(sql.data);

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * vector_index_search
 *
 * ANN search: probe n_probes centroids and return the top-k entries
 * by distance.
 *
 * Uses the inverted-file approach: for each probed centroid, load
 * all its entries and compute exact distances, then return top-k.
 * ---------------------------------------------------------------- */
Datum
vector_index_search(PG_FUNCTION_ARGS)
{
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext	per_query_ctx;
	MemoryContext	oldcxt;

	char	   *index_name;
	Vector	   *query;
	int			k;
	int			n_probes;
	char	   *metric;

	/* Result heap: (distance, entry_id) — max-heap for top-k */
	typedef struct HeapEntry
	{
		float8	dist;
		char	entry_id[256];
	} HeapEntry;

	HeapEntry  *heap = NULL;
	int			heap_size = 0;
	int			heap_cap;

	StringInfoData sql;
	int			ret;
	uint64		i;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required")));

	/* Not STRICT (p_n_probes defaults to NULL): check the required args */
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("vector_index_search: index name, query vector and k must not be NULL")));

	index_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	query      = PG_GETARG_VECTOR(1);
	k          = PG_GETARG_INT32(2);
	n_probes   = PG_ARGISNULL(3) ? -1 : PG_GETARG_INT32(3);


	tupdesc = CreateTemplateTupleDesc(2);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "entry_id", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "distance", FLOAT8OID, -1, 0);

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcxt = MemoryContextSwitchTo(per_query_ctx);
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult  = tupstore;
	rsinfo->setDesc    = BlessTupleDesc(tupdesc);
	MemoryContextSwitchTo(oldcxt);

	if (k <= 0)
		return (Datum) 0;		/* empty result */

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* Get index metadata */
	initStringInfo(&sql);
	appendStringInfo(&sql,
		"SELECT n_probes, distance_metric "
		"FROM   public.alohadb_vector_indexes "
		"WHERE  index_name = %s",
		quote_literal_cstr(index_name));

	ret = SPI_execute(sql.data, true, 1);
	pfree(sql.data);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
	{
		PopActiveSnapshot();
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_NO_DATA_FOUND),
				 errmsg("vector_index_search: index \"%s\" not found", index_name)));
	}

	if (n_probes < 0)
	{
		bool	isnull;
		n_probes = DatumGetInt32(
			SPI_getbinval(SPI_tuptable->vals[0],
						  SPI_tuptable->tupdesc, 1, &isnull));
		if (isnull) n_probes = 10;
	}

	metric = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);
	metric = pstrdup(metric ? metric : "l2");

	/* Load all centroids, pick n_probes nearest to query */
	initStringInfo(&sql);
	appendStringInfo(&sql,
		"SELECT centroid_id, centroid::text "
		"FROM   public.alohadb_vector_centroids "
		"WHERE  index_name = %s "
		"ORDER BY centroid_id",
		quote_literal_cstr(index_name));

	ret = SPI_execute(sql.data, true, 0);
	pfree(sql.data);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
	{
		PopActiveSnapshot();
		SPI_finish();
		return (Datum) 0;
	}

	{
		uint64		ncentroids = SPI_processed;
		typedef struct CentDist { int centroid_id; float8 dist; } CentDist;
		CentDist   *cdists;
		int		   *probe_ids;

		cdists = palloc(sizeof(CentDist) * ncentroids);

		for (i = 0; i < ncentroids; i++)
		{
			bool	isnull;
			char   *vec_str;
			Vector *c;

			cdists[i].centroid_id = DatumGetInt32(
				SPI_getbinval(SPI_tuptable->vals[i],
							  SPI_tuptable->tupdesc, 1, &isnull));
			vec_str = SPI_getvalue(SPI_tuptable->vals[i],
								   SPI_tuptable->tupdesc, 2);
			c = (Vector *) DirectFunctionCall3(
				vector_in, CStringGetDatum(vec_str),
			ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));

			if (strcmp(metric, "cosine") == 0)
				cdists[i].dist = vector_cosine_dist(query, c);
			else if (strcmp(metric, "ip") == 0)
				cdists[i].dist = -vector_dot_product(query, c);
			else
				cdists[i].dist = vector_l2_dist(query, c);
		}

		/* Partial sort: bubble up n_probes smallest */
		if ((uint64) n_probes > ncentroids)
			n_probes = (int) ncentroids;

		for (i = 0; i < (uint64) n_probes; i++)
		{
			uint64 min_j = i;
			uint64 j;
			CentDist tmp;

			for (j = i + 1; j < ncentroids; j++)
				if (cdists[j].dist < cdists[min_j].dist)
					min_j = j;

			if (min_j != i)
			{
				tmp = cdists[i];
				cdists[i] = cdists[min_j];
				cdists[min_j] = tmp;
			}
		}

		probe_ids = palloc(sizeof(int) * n_probes);
		for (i = 0; i < (uint64) n_probes; i++)
			probe_ids[i] = cdists[i].centroid_id;

		heap_cap = k * 2 + 16;
		/* Must outlive SPI_finish(): allocate in the caller's context */
		heap     = MemoryContextAlloc(oldcxt, sizeof(HeapEntry) * heap_cap);

		/* For each probe centroid, load entries and compute exact distances */
		for (i = 0; i < (uint64) n_probes; i++)
		{
			uint64	j;

			initStringInfo(&sql);
			appendStringInfo(&sql,
				"SELECT entry_id, vec::text "
				"FROM   public.alohadb_vector_entries "
				"WHERE  index_name = %s AND centroid_id = %d",
				quote_literal_cstr(index_name), probe_ids[i]);

			ret = SPI_execute(sql.data, true, 0);
			pfree(sql.data);

			if (ret != SPI_OK_SELECT)
				continue;

			for (j = 0; j < SPI_processed; j++)
			{
				char   *eid;
				char   *vec_str;
				Vector *ev;
				float8	dist;
				int		h;

				eid     = SPI_getvalue(SPI_tuptable->vals[j],
									  SPI_tuptable->tupdesc, 1);
				vec_str = SPI_getvalue(SPI_tuptable->vals[j],
									  SPI_tuptable->tupdesc, 2);
				if (!eid || !vec_str) continue;

				ev = (Vector *) DirectFunctionCall3(
					vector_in, CStringGetDatum(vec_str),
			ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));

				if (strcmp(metric, "cosine") == 0)
					dist = vector_cosine_dist(query, ev);
				else if (strcmp(metric, "ip") == 0)
					dist = -vector_dot_product(query, ev);
				else
					dist = vector_l2_dist(query, ev);

				/* Maintain a max-heap of size k */
				if (heap_size < k)
				{
					strlcpy(heap[heap_size].entry_id, eid,
							sizeof(heap[heap_size].entry_id));
					heap[heap_size].dist = dist;
					heap_size++;

					/* Bubble up if needed (simple insertion sort) */
					for (h = heap_size - 1; h > 0; h--)
					{
						if (heap[h].dist > heap[h-1].dist)
						{
							HeapEntry tmp = heap[h];
							heap[h] = heap[h-1];
							heap[h-1] = tmp;
						}
						else break;
					}
				}
				else if (dist < heap[0].dist)
				{
					/* Replace worst entry */
					strlcpy(heap[0].entry_id, eid,
							sizeof(heap[0].entry_id));
					heap[0].dist = dist;

					/* Re-sort descending (worst at index 0) */
					for (h = 0; h < heap_size - 1; h++)
					{
						if (heap[h].dist < heap[h+1].dist)
						{
							HeapEntry tmp = heap[h];
							heap[h] = heap[h+1];
							heap[h+1] = tmp;
						}
						else break;
					}
				}
			}
		}
	}

	PopActiveSnapshot();
	SPI_finish();

	/* Sort heap ascending by distance and emit rows */
	if (heap && heap_size > 0)
	{
		int h, j;

		/* Simple insertion sort ascending */
		for (h = 1; h < heap_size; h++)
		{
			HeapEntry tmp = heap[h];
			for (j = h - 1; j >= 0 && heap[j].dist > tmp.dist; j--)
				heap[j+1] = heap[j];
			heap[j+1] = tmp;
		}

		for (h = 0; h < heap_size; h++)
		{
			Datum	values[2];
			bool	nulls[2] = {false, false};

			values[0] = CStringGetTextDatum(heap[h].entry_id);
			values[1] = Float8GetDatum(heap[h].dist);
			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}
	}

	return (Datum) 0;
}

/* ----------------------------------------------------------------
 * vector_index_rebuild
 *
 * Re-train the IVFFlat index using Lloyd's k-means algorithm.
 *
 * Lloyd's algorithm (1957/1982):
 *   1. Pick n_lists random initial centroids (from data).
 *   2. Assign each vector to nearest centroid.
 *   3. Recompute centroids as mean of assigned vectors.
 *   4. Repeat until convergence or max iterations.
 * ---------------------------------------------------------------- */
Datum
vector_index_rebuild(PG_FUNCTION_ARGS)
{
	char	   *index_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	int			n_lists, ndim, n_iter = 25;
	char	   *source_table, *vector_column, *id_column, *metric;
	StringInfoData sql;
	int			ret;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* Fetch index config */
	initStringInfo(&sql);
	appendStringInfo(&sql,
		"SELECT n_lists, ndim, source_table, vector_column, "
		"       id_column, distance_metric "
		"FROM   public.alohadb_vector_indexes "
		"WHERE  index_name = %s",
		quote_literal_cstr(index_name));

	ret = SPI_execute(sql.data, true, 1);
	pfree(sql.data);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
	{
		PopActiveSnapshot();
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_NO_DATA_FOUND),
				 errmsg("vector_index_rebuild: index \"%s\" not found", index_name)));
	}

	{
		bool	isnull;
		n_lists       = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
		ndim          = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull));
		source_table  = pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3));
		vector_column = pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 4));
		id_column     = pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 5));
		metric        = pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 6));
		(void) ndim;	/* used implicitly via vector I/O */
	}

	/* Load all vectors from source table */
	initStringInfo(&sql);
	/* %I is a format() specifier, not a C one: quote identifiers here */
	appendStringInfo(&sql,
		"SELECT %s::text, %s::text FROM %s WHERE %s IS NOT NULL",
		quote_identifier(id_column), quote_identifier(vector_column),
		quote_identifier(source_table), quote_identifier(vector_column));

	ret = SPI_execute(sql.data, true, 0);
	pfree(sql.data);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
	{
		PopActiveSnapshot();
		SPI_finish();
		elog(WARNING, "vector_index_rebuild: no vectors found in %s.%s",
			 source_table, vector_column);
		PG_RETURN_VOID();
	}

	{
		uint64		nvecs = SPI_processed;
		Vector	  **vecs;
		char	  **ids;
		float4	  **centroids;	/* [n_lists][ndim] */
		int		   *assignments;
		int			iter, changed, c;
		uint64		j;

		if ((uint64) n_lists > nvecs)
			n_lists = (int) nvecs;

		vecs        = palloc(sizeof(Vector *) * nvecs);
		ids         = palloc(sizeof(char *)   * nvecs);
		assignments = palloc(sizeof(int)      * nvecs);
		centroids   = palloc(sizeof(float4 *) * n_lists);

		for (c = 0; c < n_lists; c++)
			centroids[c] = palloc0(sizeof(float4) * ndim);

		/* Load vectors */
		for (j = 0; j < nvecs; j++)
		{
			char *id_str  = SPI_getvalue(SPI_tuptable->vals[j], SPI_tuptable->tupdesc, 1);
			char *vec_str = SPI_getvalue(SPI_tuptable->vals[j], SPI_tuptable->tupdesc, 2);

			ids[j]  = pstrdup(id_str  ? id_str  : "");
			vecs[j] = (Vector *) DirectFunctionCall3(vector_in, CStringGetDatum(vec_str),
			ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
			assignments[j] = j % n_lists;	/* initial assignment */
		}

		/* Initialize centroids from first n_lists vectors */
		for (c = 0; c < n_lists; c++)
			memcpy(centroids[c], vecs[c]->data, ndim * sizeof(float4));

		/* Lloyd's iterations */
		for (iter = 0; iter < n_iter; iter++)
		{
			changed = 0;

			/* Assignment step */
			for (j = 0; j < nvecs; j++)
			{
				int		best = 0;
				float8	best_dist = 1e300;
				int		ci;

				for (ci = 0; ci < n_lists; ci++)
				{
					/* Build a temporary Vector for the centroid */
					Vector *ctmp = (Vector *) palloc(VECTOR_SIZE(ndim));
					float8	d;

					SET_VARSIZE(ctmp, VECTOR_SIZE(ndim));
					ctmp->ndim    = (int16) ndim;
					ctmp->_unused = 0;
					memcpy(ctmp->data, centroids[ci], ndim * sizeof(float4));

					if (strcmp(metric, "cosine") == 0)
						d = vector_cosine_dist(vecs[j], ctmp);
					else if (strcmp(metric, "ip") == 0)
						d = -vector_dot_product(vecs[j], ctmp);
					else
						d = vector_l2_dist(vecs[j], ctmp);

					pfree(ctmp);

					if (d < best_dist)
					{
						best_dist = d;
						best = ci;
					}
				}

				if (assignments[j] != best)
				{
					assignments[j] = best;
					changed++;
				}
			}

			if (changed == 0)
				break;	/* converged */

			/* Update step: recompute centroids */
			{
				int	   *counts = palloc0(sizeof(int) * n_lists);

				for (c = 0; c < n_lists; c++)
					memset(centroids[c], 0, ndim * sizeof(float4));

				for (j = 0; j < nvecs; j++)
				{
					int		ci = assignments[j];
					int		d;

					for (d = 0; d < ndim; d++)
						centroids[ci][d] += vecs[j]->data[d];
					counts[ci]++;
				}

				for (c = 0; c < n_lists; c++)
				{
					int d;
					if (counts[c] == 0)
					{
						/* Re-seed empty centroid from a random vector */
						uint64 src = (uint64)(c * 7919) % nvecs;
						memcpy(centroids[c], vecs[src]->data,
							   ndim * sizeof(float4));
					}
					else
					{
						for (d = 0; d < ndim; d++)
							centroids[c][d] /= (float4) counts[c];
					}
				}
				pfree(counts);
			}
		}

		/* Write centroids to catalog */
		SPI_execute(
			psprintf("DELETE FROM public.alohadb_vector_centroids "
					 "WHERE index_name = %s",
					 quote_literal_cstr(index_name)),
			false, 0);

		for (c = 0; c < n_lists; c++)
		{
			Vector *ctmp = (Vector *) palloc(VECTOR_SIZE(ndim));
			char   *vec_str;

			SET_VARSIZE(ctmp, VECTOR_SIZE(ndim));
			ctmp->ndim    = (int16) ndim;
			ctmp->_unused = 0;
			memcpy(ctmp->data, centroids[c], ndim * sizeof(float4));

			vec_str = DatumGetCString(
				DirectFunctionCall1(vector_out, PointerGetDatum(ctmp)));

			initStringInfo(&sql);
			appendStringInfo(&sql,
				"INSERT INTO public.alohadb_vector_centroids "
				"    (index_name, centroid_id, centroid) "
				"VALUES (%s, %d, %s)",
				quote_literal_cstr(index_name), c,
				quote_literal_cstr(vec_str));
			SPI_execute(sql.data, false, 0);
			pfree(sql.data);
			pfree(ctmp);
		}

		/* Write entries */
		SPI_execute(
			psprintf("DELETE FROM public.alohadb_vector_entries "
					 "WHERE index_name = %s",
					 quote_literal_cstr(index_name)),
			false, 0);

		for (j = 0; j < nvecs; j++)
		{
			char *vec_str = DatumGetCString(
				DirectFunctionCall1(vector_out, PointerGetDatum(vecs[j])));

			initStringInfo(&sql);
			appendStringInfo(&sql,
				"INSERT INTO public.alohadb_vector_entries "
				"    (index_name, centroid_id, entry_id, vec) "
				"VALUES (%s, %d, %s, %s)",
				quote_literal_cstr(index_name),
				assignments[j],
				quote_literal_cstr(ids[j]),
				quote_literal_cstr(vec_str));
			SPI_execute(sql.data, false, 0);
			pfree(sql.data);
		}
	}

	/* Mark as trained */
	initStringInfo(&sql);
	appendStringInfo(&sql,
		"UPDATE public.alohadb_vector_indexes "
		"SET trained = true, "
		"    last_trained_at = now(), "
		"    vector_count = ("
		"        SELECT count(*) FROM public.alohadb_vector_entries "
		"        WHERE index_name = %s"
		"    ) "
		"WHERE index_name = %s",
		quote_literal_cstr(index_name),
		quote_literal_cstr(index_name));
	SPI_execute(sql.data, false, 0);
	pfree(sql.data);

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_VOID();
}
