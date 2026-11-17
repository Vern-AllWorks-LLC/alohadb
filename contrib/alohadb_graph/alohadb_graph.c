/*-------------------------------------------------------------------------
 *
 * alohadb_graph.c
 *	  Property graphs over ordinary tables.
 *
 *	  Graph definitions (vertex labels, edge labels, property rules) live in
 *	  catalog tables managed by the PL/pgSQL functions of the extension
 *	  script.  The functions in this file load the edges of the requested
 *	  edge labels through SPI -- as the calling user, so table privileges
 *	  and row-level security of the underlying tables apply -- into an
 *	  in-memory CSR adjacency structure and then run traversals and graph
 *	  algorithms on it.  Every function returns a set, so results compose
 *	  with CTEs, joins and window functions.
 *
 *	  Point-in-time ("as of") traversal: when an edge label has valid_from /
 *	  valid_to columns, an as_of argument restricts the graph to edges with
 *	  valid_from <= as_of < valid_to (a NULL bound is open).
 *
 *	  Prior art / patent notes:
 *	  Only long-established, published methods are implemented.
 *	  Breadth-first search, Dijkstra's algorithm (1959) with a binary heap,
 *	  and union-find (Galler & Fischer 1964; Tarjan 1975) are textbook
 *	  algorithms.  PageRank by power iteration follows Page, Brin, Motwani
 *	  & Winograd (1998); the corresponding patent US 6,285,999 expired in
 *	  2018.  Earliest-arrival time-respecting paths follow Kempe, Kleinberg
 *	  & Kumar, "Connectivity and inference problems for temporal networks"
 *	  (STOC 2000).  Neighbor sampling is plain layer-wise uniform sampling
 *	  without replacement as published for GraphSAGE (Hamilton, Ying &
 *	  Leskovec, NeurIPS 2017).  Random-walk/visit-count based importance
 *	  neighborhoods (PinSage-style top-T selection) are deliberately NOT
 *	  implemented.  No SQL grammar (GRAPH_TABLE / MATCH) is added.
 *
 * Copyright (c) 2026, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_graph/alohadb_graph.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "common/pg_prng.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#include "alohadb_graph.h"

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_graph",
					.version = "1.0"
);

PG_FUNCTION_INFO_V1(graph_neighbors);
PG_FUNCTION_INFO_V1(graph_bfs);
PG_FUNCTION_INFO_V1(graph_shortest_path);
PG_FUNCTION_INFO_V1(graph_temporal_paths);
PG_FUNCTION_INFO_V1(graph_pagerank);
PG_FUNCTION_INFO_V1(graph_connected_components);
PG_FUNCTION_INFO_V1(graph_degree);
PG_FUNCTION_INFO_V1(graph_sample_neighbors);
PG_FUNCTION_INFO_V1(graph_node_features);

/* GUC: upper bound on edges loaded into memory by one function call */
static int	graph_max_edges = 10000000;

#define GRAPH_FETCH_BATCH 10000

void
_PG_init(void)
{
	DefineCustomIntVariable("alohadb_graph.max_edges",
							"Maximum number of edges one alohadb_graph function loads into memory.",
							"Each loaded edge needs roughly 80 bytes including adjacency.",
							&graph_max_edges,
							10000000,
							1000,
							INT_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);
	MarkGUCPrefixReserved("alohadb_graph");
}

/* ----------------------------------------------------------------
 * Loading
 * ----------------------------------------------------------------
 */
typedef struct LoadOptions
{
	const char *graph;
	ArrayType  *edge_labels;	/* NULL = all */
	bool		has_as_of;
	TimestampTz as_of;
	bool		load_vertices;	/* also load vertex-table keys */
	bool		has_vertex_filter;	/* only edges incident to one vertex */
	int64		filter_vertex;
	GraphDirection filter_dir;
} LoadOptions;

/* Quoted name of the schema holding the extension's catalog tables. */
static char *
graph_catalog_schema(void)
{
	Oid			extoid = get_extension_oid("alohadb_graph", false);
	Oid			nsp = get_extension_schema(extoid);
	char	   *nspname = get_namespace_name(nsp);

	if (nspname == NULL)
		elog(ERROR, "could not find schema of extension alohadb_graph");
	return pstrdup(quote_identifier(nspname));
}

static char *
text_arg(FunctionCallInfo fcinfo, int argno, const char *what)
{
	if (PG_ARGISNULL(argno))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("%s must not be NULL", what)));
	return text_to_cstring(PG_GETARG_TEXT_PP(argno));
}

static GraphDirection
direction_arg(FunctionCallInfo fcinfo, int argno)
{
	char	   *s;

	if (PG_ARGISNULL(argno))
		return GDIR_OUT;
	s = text_to_cstring(PG_GETARG_TEXT_PP(argno));
	if (pg_strcasecmp(s, "out") == 0)
		return GDIR_OUT;
	if (pg_strcasecmp(s, "in") == 0)
		return GDIR_IN;
	if (pg_strcasecmp(s, "both") == 0)
		return GDIR_BOTH;
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("invalid direction \"%s\"", s),
			 errhint("Valid directions are \"out\", \"in\" and \"both\".")));
	return GDIR_OUT;			/* keep compiler quiet */
}

/* Reject NULL elements in the edge_labels array. */
static void
check_label_array(ArrayType *arr)
{
	if (arr == NULL)
		return;
	if (ARR_NDIM(arr) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("edge_labels must be a one-dimensional array")));
	if (array_contains_nulls(arr))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("edge_labels must not contain NULL")));
}

static void
graph_add_edge(LoadedGraph *g, GraphEdge *e)
{
	if (g->nedges >= graph_max_edges)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("graph has more than %d edges", graph_max_edges),
				 errhint("Restrict edge_labels or raise alohadb_graph.max_edges.")));
	if (g->nedges >= g->edges_alloc)
	{
		g->edges_alloc *= 2;
		g->edges = (GraphEdge *) repalloc_huge(g->edges, sizeof(GraphEdge) * g->edges_alloc);
	}
	g->edges[g->nedges++] = *e;
}

static void
graph_add_extra_vertex(LoadedGraph *g, int64 id)
{
	if (g->nextra >= graph_max_edges)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("graph has more than %d vertices", graph_max_edges),
				 errhint("Raise alohadb_graph.max_edges.")));
	if (g->nextra >= g->extra_alloc)
	{
		g->extra_alloc *= 2;
		g->extra_ids = (int64 *) repalloc_huge(g->extra_ids, sizeof(int64) * g->extra_alloc);
	}
	g->extra_ids[g->nextra++] = id;
}

/* Run a query through a cursor, calling cb for every fetched tuple. */
typedef void (*row_callback) (LoadedGraph *g, HeapTuple tup, TupleDesc desc, void *arg);

static void
run_cursor(const char *query, int nargs, Oid *argtypes, Datum *values,
		   const char *nulls, LoadedGraph *g, row_callback cb, void *arg)
{
	Portal		portal;

	portal = SPI_cursor_open_with_args(NULL, query, nargs, argtypes, values,
									   nulls, true, 0);
	for (;;)
	{
		uint64		i;

		SPI_cursor_fetch(portal, true, GRAPH_FETCH_BATCH);
		if (SPI_processed == 0)
			break;
		for (i = 0; i < SPI_processed; i++)
		{
			MemoryContext old = MemoryContextSwitchTo(g->cxt);

			cb(g, SPI_tuptable->vals[i], SPI_tuptable->tupdesc, arg);
			MemoryContextSwitchTo(old);
		}
		SPI_freetuptable(SPI_tuptable);
		CHECK_FOR_INTERRUPTS();
	}
	SPI_cursor_close(portal);
}

typedef struct EdgeRowArg
{
	int16		label;
	bool		directed;
} EdgeRowArg;

static void
edge_row_cb(LoadedGraph *g, HeapTuple tup, TupleDesc desc, void *arg)
{
	EdgeRowArg *ea = (EdgeRowArg *) arg;
	GraphEdge	e;
	bool		isnull;
	Datum		d;

	memset(&e, 0, sizeof(e));
	e.src = DatumGetInt64(SPI_getbinval(tup, desc, 1, &isnull));
	e.dst = DatumGetInt64(SPI_getbinval(tup, desc, 2, &isnull));
	d = SPI_getbinval(tup, desc, 3, &isnull);
	e.weight = isnull ? 1.0 : DatumGetFloat8(d);
	d = SPI_getbinval(tup, desc, 4, &isnull);
	e.vf_null = isnull;
	e.valid_from = isnull ? 0 : DatumGetTimestampTz(d);
	d = SPI_getbinval(tup, desc, 5, &isnull);
	e.vt_null = isnull;
	e.valid_to = isnull ? 0 : DatumGetTimestampTz(d);
	e.label = ea->label;
	e.directed = ea->directed;
	graph_add_edge(g, &e);
}

static void
vertex_row_cb(LoadedGraph *g, HeapTuple tup, TupleDesc desc, void *arg)
{
	bool		isnull;
	Datum		d = SPI_getbinval(tup, desc, 1, &isnull);

	if (!isnull)
		graph_add_extra_vertex(g, DatumGetInt64(d));
}

static char *
spi_text(HeapTuple tup, TupleDesc desc, int col)
{
	return SPI_getvalue(tup, desc, col);	/* NULL if the value is NULL */
}

/*
 * Load the edges of the requested edge labels (and optionally the keys of
 * the vertex labels they connect).  SPI must be connected.  The returned
 * graph lives in a new child context of 'parent'; it has its vertex index
 * built but no adjacency yet.
 */
static LoadedGraph *
graph_load(LoadOptions *opt, MemoryContext parent)
{
	LoadedGraph *g;
	char	   *schema = graph_catalog_schema();
	StringInfoData q;
	Oid			cat_types[2] = {TEXTOID, TEXTARRAYOID};
	Datum		cat_vals[2];
	char		cat_nulls[2];
	int			ret;
	uint64		nlabels;
	uint64		i;
	SPITupleTable *labels;

	check_label_array(opt->edge_labels);

	g = (LoadedGraph *) MemoryContextAllocZero(parent, sizeof(LoadedGraph));
	g->cxt = AllocSetContextCreate(parent, "alohadb_graph", ALLOCSET_DEFAULT_SIZES);
	g->edges_alloc = 1024;
	g->edges = (GraphEdge *) MemoryContextAlloc(g->cxt, sizeof(GraphEdge) * g->edges_alloc);
	g->extra_alloc = 256;
	g->extra_ids = (int64 *) MemoryContextAlloc(g->cxt, sizeof(int64) * g->extra_alloc);

	cat_vals[0] = CStringGetTextDatum(opt->graph);
	cat_nulls[0] = ' ';
	cat_vals[1] = opt->edge_labels ? PointerGetDatum(opt->edge_labels) : (Datum) 0;
	cat_nulls[1] = opt->edge_labels ? ' ' : 'n';

	/* graph must exist */
	initStringInfo(&q);
	appendStringInfo(&q, "SELECT 1 FROM %s.alohadb_graph_graphs WHERE graph_name = $1", schema);
	ret = SPI_execute_with_args(q.data, 1, cat_types, cat_vals, cat_nulls, true, 1);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute_with_args failed: %s", SPI_result_code_string(ret));
	if (SPI_processed == 0)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("graph \"%s\" does not exist", opt->graph)));

	/* every requested edge label must exist */
	if (opt->edge_labels)
	{
		resetStringInfo(&q);
		appendStringInfo(&q,
						 "SELECT l FROM pg_catalog.unnest($2) AS l "
						 "WHERE NOT EXISTS (SELECT 1 FROM %s.alohadb_graph_edge_labels e "
						 "WHERE e.graph_name = $1 AND e.label = l) LIMIT 1",
						 schema);
		ret = SPI_execute_with_args(q.data, 2, cat_types, cat_vals, cat_nulls, true, 1);
		if (ret != SPI_OK_SELECT)
			elog(ERROR, "SPI_execute_with_args failed: %s", SPI_result_code_string(ret));
		if (SPI_processed > 0)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("edge label \"%s\" does not exist in graph \"%s\"",
							spi_text(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1),
							opt->graph)));
	}

	/* edge label definitions */
	resetStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT e.label, e.table_name::text, e.src_column, e.dst_column, "
					 "e.weight_column, e.valid_from_column, e.valid_to_column, "
					 "e.directed::text "
					 "FROM %s.alohadb_graph_edge_labels e "
					 "WHERE e.graph_name = $1 AND ($2 IS NULL OR e.label = ANY($2)) "
					 "ORDER BY e.label",
					 schema);
	ret = SPI_execute_with_args(q.data, 2, cat_types, cat_vals, cat_nulls, true, 0);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute_with_args failed: %s", SPI_result_code_string(ret));
	labels = SPI_tuptable;
	nlabels = SPI_processed;
	if (nlabels > PG_INT16_MAX)
		elog(ERROR, "too many edge labels");

	g->nlabels = (int) nlabels;
	g->label_names = (char **) MemoryContextAllocZero(g->cxt, sizeof(char *) * Max(nlabels, 1));

	for (i = 0; i < nlabels; i++)
	{
		HeapTuple	tup = labels->vals[i];
		TupleDesc	desc = labels->tupdesc;
		char	   *label = spi_text(tup, desc, 1);
		char	   *tbl = spi_text(tup, desc, 2);
		char	   *scol = spi_text(tup, desc, 3);
		char	   *dcol = spi_text(tup, desc, 4);
		char	   *wcol = spi_text(tup, desc, 5);
		char	   *vfcol = spi_text(tup, desc, 6);
		char	   *vtcol = spi_text(tup, desc, 7);
		bool		directed = (strcmp(spi_text(tup, desc, 8), "true") == 0);
		Oid			argtypes[2] = {TIMESTAMPTZOID, INT8OID};
		Datum		values[2];
		char		nulls[2];
		EdgeRowArg	ea;

		g->label_names[i] = MemoryContextStrdup(g->cxt, label);

		resetStringInfo(&q);
		appendStringInfo(&q, "SELECT t.%s::pg_catalog.int8, t.%s::pg_catalog.int8, ",
						 quote_identifier(scol), quote_identifier(dcol));
		if (wcol)
			appendStringInfo(&q, "t.%s::pg_catalog.float8, ", quote_identifier(wcol));
		else
			appendStringInfoString(&q, "NULL::pg_catalog.float8, ");
		if (vfcol)
			appendStringInfo(&q, "t.%s::pg_catalog.timestamptz, ", quote_identifier(vfcol));
		else
			appendStringInfoString(&q, "NULL::pg_catalog.timestamptz, ");
		if (vtcol)
			appendStringInfo(&q, "t.%s::pg_catalog.timestamptz ", quote_identifier(vtcol));
		else
			appendStringInfoString(&q, "NULL::pg_catalog.timestamptz ");
		/* tbl is regclass output, already quoted as needed */
		appendStringInfo(&q, "FROM %s t WHERE t.%s IS NOT NULL AND t.%s IS NOT NULL",
						 tbl, quote_identifier(scol), quote_identifier(dcol));

		if (opt->has_as_of)
		{
			if (vfcol)
				appendStringInfo(&q, " AND (t.%s IS NULL OR t.%s <= $1)",
								 quote_identifier(vfcol), quote_identifier(vfcol));
			if (vtcol)
				appendStringInfo(&q, " AND (t.%s IS NULL OR t.%s > $1)",
								 quote_identifier(vtcol), quote_identifier(vtcol));
		}
		if (opt->has_vertex_filter)
		{
			bool		match_src = (!directed || opt->filter_dir != GDIR_IN);
			bool		match_dst = (!directed || opt->filter_dir != GDIR_OUT);

			if (match_src && match_dst)
				appendStringInfo(&q, " AND (t.%s = $2 OR t.%s = $2)",
								 quote_identifier(scol), quote_identifier(dcol));
			else if (match_src)
				appendStringInfo(&q, " AND t.%s = $2", quote_identifier(scol));
			else
				appendStringInfo(&q, " AND t.%s = $2", quote_identifier(dcol));
		}

		values[0] = TimestampTzGetDatum(opt->as_of);
		nulls[0] = opt->has_as_of ? ' ' : 'n';
		values[1] = Int64GetDatum(opt->filter_vertex);
		nulls[1] = opt->has_vertex_filter ? ' ' : 'n';

		ea.label = (int16) i;
		ea.directed = directed;
		run_cursor(q.data, 2, argtypes, values, nulls, g, edge_row_cb, &ea);
	}

	/* vertex keys (so isolated vertices take part in whole-graph algorithms) */
	if (opt->load_vertices)
	{
		SPITupleTable *vl;
		uint64		nvl;

		resetStringInfo(&q);
		appendStringInfo(&q,
						 "SELECT v.table_name::text, v.key_column FROM %s.alohadb_graph_vertex_labels v "
						 "WHERE v.graph_name = $1 AND ($2 IS NULL OR v.label IN "
						 "(SELECT e.src_label FROM %s.alohadb_graph_edge_labels e "
						 " WHERE e.graph_name = $1 AND e.label = ANY($2) "
						 " UNION SELECT e.dst_label FROM %s.alohadb_graph_edge_labels e "
						 " WHERE e.graph_name = $1 AND e.label = ANY($2))) "
						 "ORDER BY v.label",
						 schema, schema, schema);
		ret = SPI_execute_with_args(q.data, 2, cat_types, cat_vals, cat_nulls, true, 0);
		if (ret != SPI_OK_SELECT)
			elog(ERROR, "SPI_execute_with_args failed: %s", SPI_result_code_string(ret));
		vl = SPI_tuptable;
		nvl = SPI_processed;
		for (i = 0; i < nvl; i++)
		{
			char	   *tbl = spi_text(vl->vals[i], vl->tupdesc, 1);
			char	   *kcol = spi_text(vl->vals[i], vl->tupdesc, 2);

			resetStringInfo(&q);
			appendStringInfo(&q, "SELECT t.%s::pg_catalog.int8 FROM %s t WHERE t.%s IS NOT NULL",
							 quote_identifier(kcol), tbl, quote_identifier(kcol));
			run_cursor(q.data, 0, NULL, NULL, NULL, g, vertex_row_cb, NULL);
		}
	}

	graph_build_index(g);
	return g;
}

/* Common argument handling for (edge_labels, as_of) pairs. */
static void
load_opts_init(LoadOptions *opt, FunctionCallInfo fcinfo, int graph_arg,
			   int labels_arg, int as_of_arg)
{
	memset(opt, 0, sizeof(*opt));
	opt->graph = text_arg(fcinfo, graph_arg, "graph");
	if (labels_arg >= 0 && !PG_ARGISNULL(labels_arg))
		opt->edge_labels = PG_GETARG_ARRAYTYPE_P(labels_arg);
	if (as_of_arg >= 0 && !PG_ARGISNULL(as_of_arg))
	{
		opt->has_as_of = true;
		opt->as_of = PG_GETARG_TIMESTAMPTZ(as_of_arg);
	}
}

/* Build an int8[] from a path of dense indexes (stored in reverse). */
static Datum
make_int8_array(int64 *keys, int n)
{
	Datum	   *d = (Datum *) palloc(sizeof(Datum) * Max(n, 1));
	int			i;

	for (i = 0; i < n; i++)
		d[i] = Int64GetDatum(keys[i]);
	return PointerGetDatum(construct_array_builtin(d, n, INT8OID));
}

static Datum
make_float8_array(double *vals, int n)
{
	Datum	   *d = (Datum *) palloc(sizeof(Datum) * Max(n, 1));
	int			i;

	for (i = 0; i < n; i++)
		d[i] = Float8GetDatum(vals[i]);
	return PointerGetDatum(construct_array_builtin(d, n, FLOAT8OID));
}

/* ----------------------------------------------------------------
 * graph_neighbors(graph, vertex, edge_labels, direction, as_of)
 * ----------------------------------------------------------------
 */
Datum
graph_neighbors(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	LoadOptions opt;
	LoadedGraph *g;
	GraphDirection dir;
	int64		vid;
	int32		v;

	InitMaterializedSRF(fcinfo, 0);
	load_opts_init(&opt, fcinfo, 0, 2, 4);
	if (PG_ARGISNULL(1))
		return (Datum) 0;
	vid = PG_GETARG_INT64(1);
	dir = direction_arg(fcinfo, 3);

	opt.has_vertex_filter = true;
	opt.filter_vertex = vid;
	opt.filter_dir = dir;

	SPI_connect();
	g = graph_load(&opt, CurrentMemoryContext);
	graph_build_adjacency(g, dir);

	v = graph_vertex_index(g, vid);
	if (v >= 0)
	{
		int64		a;

		for (a = g->adj_off[v]; a < g->adj_off[v + 1]; a++)
		{
			GraphEdge  *e = &g->edges[g->adj_edge[a]];
			Datum		values[6];
			bool		nulls[6] = {false, false, false, false, false, false};
			const char *edir;

			if (!e->directed)
				edir = "undirected";
			else if (dir == GDIR_OUT)
				edir = "out";
			else if (dir == GDIR_IN)
				edir = "in";
			else
				edir = (e->si == v) ? "out" : "in";

			values[0] = Int64GetDatum(g->ids[g->adj_to[a]]);
			values[1] = CStringGetTextDatum(g->label_names[e->label]);
			values[2] = CStringGetTextDatum(edir);
			values[3] = Float8GetDatum(e->weight);
			values[4] = TimestampTzGetDatum(e->valid_from);
			nulls[4] = e->vf_null;
			values[5] = TimestampTzGetDatum(e->valid_to);
			nulls[5] = e->vt_null;
			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		}
	}

	MemoryContextDelete(g->cxt);
	SPI_finish();
	return (Datum) 0;
}

/* ----------------------------------------------------------------
 * graph_bfs(graph, start, max_depth, edge_labels, as_of, direction)
 * ----------------------------------------------------------------
 */
Datum
graph_bfs(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	LoadOptions opt;
	LoadedGraph *g;
	GraphDirection dir;
	int64		start;
	int32		max_depth;
	int32		s;
	int32	   *queue;
	int32	   *depth;
	int32	   *parent;
	int64	   *pathbuf;
	int32		qhead = 0,
				qtail = 0;

	InitMaterializedSRF(fcinfo, 0);
	load_opts_init(&opt, fcinfo, 0, 3, 4);
	if (PG_ARGISNULL(1))
		return (Datum) 0;
	start = PG_GETARG_INT64(1);
	max_depth = PG_ARGISNULL(2) ? PG_INT32_MAX : PG_GETARG_INT32(2);
	if (max_depth < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("max_depth must not be negative")));
	dir = direction_arg(fcinfo, 5);

	SPI_connect();
	g = graph_load(&opt, CurrentMemoryContext);
	graph_build_adjacency(g, dir);

	s = graph_vertex_index(g, start);
	if (s < 0)
	{
		/* not an endpoint of any loaded edge: only the start itself */
		Datum		values[3];
		bool		nulls[3] = {false, false, false};

		values[0] = Int64GetDatum(start);
		values[1] = Int32GetDatum(0);
		values[2] = make_int8_array(&start, 1);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		MemoryContextDelete(g->cxt);
		SPI_finish();
		return (Datum) 0;
	}

	queue = (int32 *) MemoryContextAllocHuge(g->cxt, sizeof(int32) * g->nverts);
	depth = (int32 *) MemoryContextAllocHuge(g->cxt, sizeof(int32) * g->nverts);
	parent = (int32 *) MemoryContextAllocHuge(g->cxt, sizeof(int32) * g->nverts);
	for (int32 v = 0; v < g->nverts; v++)
		depth[v] = -1;

	depth[s] = 0;
	parent[s] = -1;
	queue[qtail++] = s;
	while (qhead < qtail)
	{
		int32		u = queue[qhead++];
		int64		a;

		CHECK_FOR_INTERRUPTS();
		if (depth[u] >= max_depth)
			continue;
		for (a = g->adj_off[u]; a < g->adj_off[u + 1]; a++)
		{
			int32		w = g->adj_to[a];

			if (depth[w] < 0)
			{
				depth[w] = depth[u] + 1;
				parent[w] = u;
				queue[qtail++] = w;
			}
		}
	}

	pathbuf = (int64 *) MemoryContextAllocHuge(g->cxt, sizeof(int64) * ((Size) g->nverts + 1));
	for (int32 i = 0; i < qtail; i++)
	{
		int32		v = queue[i];
		int32		len = depth[v] + 1;
		int32		k = len;
		int32		x;
		Datum		values[3];
		bool		nulls[3] = {false, false, false};

		for (x = v; x >= 0; x = parent[x])
			pathbuf[--k] = g->ids[x];

		values[0] = Int64GetDatum(g->ids[v]);
		values[1] = Int32GetDatum(depth[v]);
		values[2] = make_int8_array(pathbuf, len);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	MemoryContextDelete(g->cxt);
	SPI_finish();
	return (Datum) 0;
}

/* ----------------------------------------------------------------
 * graph_shortest_path(graph, src, dst, edge_labels, as_of, direction)
 * ----------------------------------------------------------------
 */
Datum
graph_shortest_path(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	LoadOptions opt;
	LoadedGraph *g;
	GraphDirection dir;
	int64		src,
				dst;
	int32		s,
				t;
	double	   *dist;
	int32	   *parent;
	int32	   *path;
	int32		len = 0;

	InitMaterializedSRF(fcinfo, 0);
	load_opts_init(&opt, fcinfo, 0, 3, 4);
	if (PG_ARGISNULL(1) || PG_ARGISNULL(2))
		return (Datum) 0;
	src = PG_GETARG_INT64(1);
	dst = PG_GETARG_INT64(2);
	dir = direction_arg(fcinfo, 5);

	if (src == dst)
	{
		Datum		values[3];
		bool		nulls[3] = {false, false, false};

		/* still validate graph / labels */
		SPI_connect();
		g = graph_load(&opt, CurrentMemoryContext);
		MemoryContextDelete(g->cxt);
		SPI_finish();

		values[0] = Int32GetDatum(0);
		values[1] = Int64GetDatum(src);
		values[2] = Float8GetDatum(0.0);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		return (Datum) 0;
	}

	SPI_connect();
	g = graph_load(&opt, CurrentMemoryContext);

	for (int64 i = 0; i < g->nedges; i++)
	{
		if (!(g->edges[i].weight >= 0.0) || isinf(g->edges[i].weight))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("edge weights must be finite and non-negative for shortest paths"),
					 errdetail("Edge %lld -> %lld of label \"%s\" has weight %g.",
							   (long long) g->edges[i].src, (long long) g->edges[i].dst,
							   g->label_names[g->edges[i].label], g->edges[i].weight)));
	}
	graph_build_adjacency(g, dir);

	s = graph_vertex_index(g, src);
	t = graph_vertex_index(g, dst);
	if (s >= 0 && t >= 0)
	{
		dist = (double *) MemoryContextAllocHuge(g->cxt, sizeof(double) * g->nverts);
		parent = (int32 *) MemoryContextAllocHuge(g->cxt, sizeof(int32) * g->nverts);
		graph_dijkstra(g, s, t, dist, parent);

		if (!isinf(dist[t]))
		{
			int32		x;

			for (x = t; x >= 0; x = parent[x])
				len++;
			path = (int32 *) MemoryContextAlloc(g->cxt, sizeof(int32) * len);
			x = t;
			for (int32 k = len - 1; k >= 0; k--)
			{
				path[k] = x;
				x = parent[x];
			}
			for (int32 k = 0; k < len; k++)
			{
				Datum		values[3];
				bool		nulls[3] = {false, false, false};

				values[0] = Int32GetDatum(k);
				values[1] = Int64GetDatum(g->ids[path[k]]);
				values[2] = Float8GetDatum(dist[path[k]]);
				tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
			}
		}
	}

	MemoryContextDelete(g->cxt);
	SPI_finish();
	return (Datum) 0;
}

/* ----------------------------------------------------------------
 * graph_temporal_paths(graph, start, start_time, max_hops, edge_labels,
 *						direction)
 *
 * Earliest-arrival time-respecting paths.  Each edge is a contact at time
 * valid_from (edges without valid_from are ignored).  A path
 * start = v0 -e1-> v1 -e2-> ... -ek-> vk is time-respecting when
 * start_time <= t(e1) <= t(e2) <= ... <= t(ek); its arrival time is t(ek).
 *
 * Hop-bounded Bellman-Ford style relaxation: round k computes the earliest
 * arrival using at most k hops, from the arrivals of round k-1 (kept in
 * separate arrays so a path found in round k has exactly <= k hops).
 * Among paths with the same earliest arrival the one with fewest hops is
 * reported.
 * ----------------------------------------------------------------
 */
Datum
graph_temporal_paths(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	LoadOptions opt;
	LoadedGraph *g;
	GraphDirection dir;
	int64		start;
	TimestampTz start_time;
	int32		max_hops;
	int32		s;
	int32		n;
	bool	   *reached;
	TimestampTz *arr_prev,
			   *arr_cur;
	int32	   *hops;
	int32	   *pred;
	int64	  **path_prev,
			  **path_cur;
	bool	   *active,
			   *changed;
	bool		any_active;
	int32		k;

	InitMaterializedSRF(fcinfo, 0);
	load_opts_init(&opt, fcinfo, 0, 4, -1);
	if (PG_ARGISNULL(1))
		return (Datum) 0;
	start = PG_GETARG_INT64(1);
	if (PG_ARGISNULL(2))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("start_time must not be NULL")));
	start_time = PG_GETARG_TIMESTAMPTZ(2);
	max_hops = PG_ARGISNULL(3) ? PG_INT32_MAX : PG_GETARG_INT32(3);
	if (max_hops < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("max_hops must not be negative")));
	dir = direction_arg(fcinfo, 5);

	SPI_connect();
	g = graph_load(&opt, CurrentMemoryContext);
	graph_build_adjacency(g, dir);
	n = g->nverts;

	s = graph_vertex_index(g, start);
	if (s < 0)
	{
		Datum		values[4];
		bool		nulls[4] = {false, false, false, false};

		values[0] = Int64GetDatum(start);
		values[1] = TimestampTzGetDatum(start_time);
		values[2] = Int32GetDatum(0);
		values[3] = make_int8_array(&start, 1);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		MemoryContextDelete(g->cxt);
		SPI_finish();
		return (Datum) 0;
	}

	reached = (bool *) MemoryContextAllocHuge(g->cxt, sizeof(bool) * n);
	arr_prev = (TimestampTz *) MemoryContextAllocHuge(g->cxt, sizeof(TimestampTz) * n);
	arr_cur = (TimestampTz *) MemoryContextAllocHuge(g->cxt, sizeof(TimestampTz) * n);
	hops = (int32 *) MemoryContextAllocHuge(g->cxt, sizeof(int32) * n);
	pred = (int32 *) MemoryContextAllocHuge(g->cxt, sizeof(int32) * n);
	path_prev = (int64 **) MemoryContextAllocHuge(g->cxt, sizeof(int64 *) * n);
	path_cur = (int64 **) MemoryContextAllocHuge(g->cxt, sizeof(int64 *) * n);
	active = (bool *) MemoryContextAllocHuge(g->cxt, sizeof(bool) * n);
	changed = (bool *) MemoryContextAllocHuge(g->cxt, sizeof(bool) * n);
	for (int32 v = 0; v < n; v++)
	{
		reached[v] = false;
		active[v] = false;
		path_prev[v] = path_cur[v] = NULL;
	}

	reached[s] = true;
	arr_prev[s] = arr_cur[s] = start_time;
	hops[s] = 0;
	path_prev[s] = path_cur[s] = (int64 *) MemoryContextAlloc(g->cxt, sizeof(int64));
	path_prev[s][0] = start;
	active[s] = true;
	any_active = true;

	for (k = 1; k <= max_hops && any_active; k++)
	{
		for (int32 v = 0; v < n; v++)
			changed[v] = false;

		for (int32 u = 0; u < n; u++)
		{
			if (!active[u])
				continue;
			CHECK_FOR_INTERRUPTS();
			for (int64 a = g->adj_off[u]; a < g->adj_off[u + 1]; a++)
			{
				GraphEdge  *e = &g->edges[g->adj_edge[a]];
				int32		w = g->adj_to[a];

				if (e->vf_null || e->valid_from < arr_prev[u])
					continue;
				if (!reached[w] || e->valid_from < arr_cur[w])
				{
					reached[w] = true;
					arr_cur[w] = e->valid_from;
					hops[w] = k;
					pred[w] = u;
					changed[w] = true;
				}
			}
		}

		/* materialize paths of improved vertices from round k-1 paths */
		any_active = false;
		for (int32 w = 0; w < n; w++)
		{
			if (changed[w])
			{
				int64	   *p = (int64 *) MemoryContextAlloc(g->cxt, sizeof(int64) * (k + 1));

				memcpy(p, path_prev[pred[w]], sizeof(int64) * k);
				p[k] = g->ids[w];
				path_cur[w] = p;
				any_active = true;
			}
			active[w] = changed[w];
		}
		for (int32 w = 0; w < n; w++)
		{
			if (changed[w])
			{
				arr_prev[w] = arr_cur[w];
				path_prev[w] = path_cur[w];
			}
		}
	}

	for (int32 v = 0; v < n; v++)
	{
		Datum		values[4];
		bool		nulls[4] = {false, false, false, false};

		if (!reached[v])
			continue;
		values[0] = Int64GetDatum(g->ids[v]);
		values[1] = TimestampTzGetDatum(arr_cur[v]);
		values[2] = Int32GetDatum(hops[v]);
		values[3] = make_int8_array(path_cur[v], hops[v] + 1);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	MemoryContextDelete(g->cxt);
	SPI_finish();
	return (Datum) 0;
}

/* ----------------------------------------------------------------
 * graph_pagerank(graph, damping, iterations, edge_labels, as_of)
 * ----------------------------------------------------------------
 */
Datum
graph_pagerank(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	LoadOptions opt;
	LoadedGraph *g;
	double		damping;
	int32		iterations;
	double	   *rank;

	InitMaterializedSRF(fcinfo, 0);
	load_opts_init(&opt, fcinfo, 0, 3, 4);
	damping = PG_ARGISNULL(1) ? 0.85 : PG_GETARG_FLOAT8(1);
	iterations = PG_ARGISNULL(2) ? 20 : PG_GETARG_INT32(2);
	if (!(damping >= 0.0 && damping <= 1.0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("damping must be between 0 and 1")));
	if (iterations < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("iterations must not be negative")));
	opt.load_vertices = true;

	SPI_connect();
	g = graph_load(&opt, CurrentMemoryContext);
	graph_build_adjacency(g, GDIR_OUT);
	rank = graph_pagerank_compute(g, damping, iterations);

	for (int32 v = 0; v < g->nverts; v++)
	{
		Datum		values[2];
		bool		nulls[2] = {false, false};

		values[0] = Int64GetDatum(g->ids[v]);
		values[1] = Float8GetDatum(rank[v]);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	MemoryContextDelete(g->cxt);
	SPI_finish();
	return (Datum) 0;
}

/* ----------------------------------------------------------------
 * graph_connected_components(graph, edge_labels, as_of)
 * ----------------------------------------------------------------
 */
Datum
graph_connected_components(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	LoadOptions opt;
	LoadedGraph *g;
	int64	   *comp;

	InitMaterializedSRF(fcinfo, 0);
	load_opts_init(&opt, fcinfo, 0, 1, 2);
	opt.load_vertices = true;

	SPI_connect();
	g = graph_load(&opt, CurrentMemoryContext);
	comp = graph_components_compute(g);

	for (int32 v = 0; v < g->nverts; v++)
	{
		Datum		values[2];
		bool		nulls[2] = {false, false};

		values[0] = Int64GetDatum(g->ids[v]);
		values[1] = Int64GetDatum(comp[v]);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	MemoryContextDelete(g->cxt);
	SPI_finish();
	return (Datum) 0;
}

/* ----------------------------------------------------------------
 * graph_degree(graph, edge_labels, as_of)
 *
 * out_degree / in_degree count arcs leaving / entering the vertex (an
 * undirected edge counts in both); degree counts incident edges, a
 * self-loop once.
 * ----------------------------------------------------------------
 */
Datum
graph_degree(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	LoadOptions opt;
	LoadedGraph *g;
	int64	   *indeg,
			   *outdeg,
			   *deg;

	InitMaterializedSRF(fcinfo, 0);
	load_opts_init(&opt, fcinfo, 0, 1, 2);
	opt.load_vertices = true;

	SPI_connect();
	g = graph_load(&opt, CurrentMemoryContext);

	indeg = (int64 *) MemoryContextAllocHuge(g->cxt, sizeof(int64) * Max(g->nverts, 1));
	outdeg = (int64 *) MemoryContextAllocHuge(g->cxt, sizeof(int64) * Max(g->nverts, 1));
	deg = (int64 *) MemoryContextAllocHuge(g->cxt, sizeof(int64) * Max(g->nverts, 1));
	memset(indeg, 0, sizeof(int64) * g->nverts);
	memset(outdeg, 0, sizeof(int64) * g->nverts);
	memset(deg, 0, sizeof(int64) * g->nverts);

	for (int64 i = 0; i < g->nedges; i++)
	{
		GraphEdge  *e = &g->edges[i];

		outdeg[e->si]++;
		indeg[e->di]++;
		deg[e->si]++;
		if (e->di != e->si)
		{
			deg[e->di]++;
			if (!e->directed)
			{
				outdeg[e->di]++;
				indeg[e->si]++;
			}
		}
	}

	for (int32 v = 0; v < g->nverts; v++)
	{
		Datum		values[4];
		bool		nulls[4] = {false, false, false, false};

		values[0] = Int64GetDatum(g->ids[v]);
		values[1] = Int64GetDatum(indeg[v]);
		values[2] = Int64GetDatum(outdeg[v]);
		values[3] = Int64GetDatum(deg[v]);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	MemoryContextDelete(g->cxt);
	SPI_finish();
	return (Datum) 0;
}

/*
 * Collect the distinct neighbors of v (in adjacency order, i.e. ascending
 * key) into buf; returns their count.  mark[] must hold no value equal to
 * stamp on entry.
 */
static int64
distinct_neighbors(LoadedGraph *g, int32 v, int32 *buf, int64 *mark, int64 stamp)
{
	int64		n = 0;

	for (int64 a = g->adj_off[v]; a < g->adj_off[v + 1]; a++)
	{
		int32		w = g->adj_to[a];

		if (mark[w] != stamp)
		{
			mark[w] = stamp;
			buf[n++] = w;
		}
	}
	return n;
}

static int
cmp_int32(const void *a, const void *b)
{
	int32		x = *(const int32 *) a;
	int32		y = *(const int32 *) b;

	return (x > y) - (x < y);
}

/* ----------------------------------------------------------------
 * graph_sample_neighbors(graph, seeds, fanouts, edge_labels, as_of, seed,
 *						  direction)
 *
 * Layer-wise uniform neighbor sampling without replacement (GraphSAGE).
 * Hop h expands every vertex of frontier h (frontier 1 = the distinct
 * seeds) by sampling min(fanouts[h], #distinct neighbors) distinct
 * neighbors uniformly at random (a fanout of -1 takes all neighbors).
 * Frontier h+1 is the set of distinct vertices sampled in hop h.  Output
 * rows are (hop, src = expanded vertex, dst = sampled neighbor).
 * ----------------------------------------------------------------
 */
Datum
graph_sample_neighbors(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	LoadOptions opt;
	LoadedGraph *g;
	GraphDirection dir;
	ArrayType  *seeds_arr;
	ArrayType  *fan_arr;
	Datum	   *seed_d;
	bool	   *seed_n;
	int			nseeds;
	Datum	   *fan_d;
	bool	   *fan_n;
	int			nfan;
	pg_prng_state prng;
	int32	   *frontier;
	int64		nfrontier = 0;
	int32	   *next;
	int64		nnext;
	int64	   *mark;
	int64	   *in_next;
	int32	   *nbuf;
	int64		stamp = 0;

	InitMaterializedSRF(fcinfo, 0);
	load_opts_init(&opt, fcinfo, 0, 3, 4);
	if (PG_ARGISNULL(1))
		return (Datum) 0;
	if (PG_ARGISNULL(2))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("fanouts must not be NULL")));
	seeds_arr = PG_GETARG_ARRAYTYPE_P(1);
	fan_arr = PG_GETARG_ARRAYTYPE_P(2);
	dir = direction_arg(fcinfo, 6);

	deconstruct_array(seeds_arr, INT8OID, sizeof(int64), FLOAT8PASSBYVAL,
					  TYPALIGN_DOUBLE, &seed_d, &seed_n, &nseeds);
	deconstruct_array_builtin(fan_arr, INT4OID, &fan_d, &fan_n, &nfan);
	for (int i = 0; i < nfan; i++)
	{
		if (fan_n[i])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("fanouts must not contain NULL")));
		if (DatumGetInt32(fan_d[i]) < -1)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("fanout must be -1 (all neighbors) or >= 0")));
	}

	if (PG_ARGISNULL(5))
	{
		if (!pg_prng_strong_seed(&prng))
			pg_prng_seed(&prng, (uint64) GetCurrentTimestamp());
	}
	else
		pg_prng_seed(&prng, (uint64) (int64) PG_GETARG_INT32(5));

	SPI_connect();
	g = graph_load(&opt, CurrentMemoryContext);
	graph_build_adjacency(g, dir);

	if (g->nverts == 0)
	{
		MemoryContextDelete(g->cxt);
		SPI_finish();
		return (Datum) 0;
	}

	frontier = (int32 *) MemoryContextAllocHuge(g->cxt, sizeof(int32) * g->nverts);
	next = (int32 *) MemoryContextAllocHuge(g->cxt, sizeof(int32) * g->nverts);
	mark = (int64 *) MemoryContextAllocHuge(g->cxt, sizeof(int64) * g->nverts);
	in_next = (int64 *) MemoryContextAllocHuge(g->cxt, sizeof(int64) * g->nverts);
	nbuf = (int32 *) MemoryContextAllocHuge(g->cxt, sizeof(int32) * g->nverts);
	for (int32 v = 0; v < g->nverts; v++)
	{
		mark[v] = -1;
		in_next[v] = -1;
	}

	/* frontier 1: distinct seeds that exist in the loaded graph, in order */
	for (int i = 0; i < nseeds; i++)
	{
		int32		v;

		if (seed_n[i])
			continue;
		v = graph_vertex_index(g, DatumGetInt64(seed_d[i]));
		if (v >= 0 && in_next[v] != 0)
		{
			in_next[v] = 0;
			frontier[nfrontier++] = v;
		}
	}

	for (int h = 0; h < nfan && nfrontier > 0; h++)
	{
		int32		fanout = DatumGetInt32(fan_d[h]);

		nnext = 0;
		for (int64 f = 0; f < nfrontier; f++)
		{
			int32		u = frontier[f];
			int64		n;
			int64		take;

			CHECK_FOR_INTERRUPTS();
			n = distinct_neighbors(g, u, nbuf, mark, stamp++);
			take = (fanout < 0 || fanout > n) ? n : fanout;

			/* partial Fisher-Yates: first 'take' entries become the sample */
			if (take < n)
			{
				for (int64 i = 0; i < take; i++)
				{
					int64		j = (int64) pg_prng_uint64_range(&prng, (uint64) i, (uint64) (n - 1));
					int32		tmp = nbuf[i];

					nbuf[i] = nbuf[j];
					nbuf[j] = tmp;
				}
				qsort(nbuf, take, sizeof(int32), cmp_int32);
			}

			for (int64 i = 0; i < take; i++)
			{
				int32		w = nbuf[i];
				Datum		values[3];
				bool		nulls[3] = {false, false, false};

				values[0] = Int32GetDatum(h + 1);
				values[1] = Int64GetDatum(g->ids[u]);
				values[2] = Int64GetDatum(g->ids[w]);
				tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);

				if (in_next[w] != h + 1)
				{
					in_next[w] = h + 1;
					next[nnext++] = w;
				}
			}
		}

		/* swap frontier buffers */
		{
			int32	   *tmp = frontier;

			frontier = next;
			next = tmp;
			nfrontier = nnext;
		}
	}

	MemoryContextDelete(g->cxt);
	SPI_finish();
	return (Datum) 0;
}

/* ----------------------------------------------------------------
 * graph_node_features(graph, vertices, feature_column, edge_labels, as_of,
 *					   direction)
 *
 * For each requested vertex: degree (number of arcs in the given
 * direction), feature_count (distinct neighbors with a non-NULL feature)
 * and feature_mean (element-wise mean of those neighbors' features).
 *
 * The feature column is looked up in every vertex label table of the
 * graph that has a column of that name.  Numeric scalars become 1-element
 * vectors, numeric arrays are used as is, and any other type (e.g. a
 * vector type) is converted through its text form, accepting '[...]' or
 * '{...}' notation.
 * ----------------------------------------------------------------
 */
typedef struct FeatureEntry
{
	int64		id;				/* hash key */
	int32		dim;
	double	   *vals;
} FeatureEntry;

typedef struct FeatureRowArg
{
	HTAB	   *features;
} FeatureRowArg;

static void
feature_row_cb(LoadedGraph *g, HeapTuple tup, TupleDesc desc, void *arg)
{
	FeatureRowArg *fa = (FeatureRowArg *) arg;
	bool		isnull;
	int64		id;
	ArrayType  *arr;
	Datum	   *elems;
	bool	   *enulls;
	int			nelems;
	FeatureEntry *fe;
	bool		found;

	id = DatumGetInt64(SPI_getbinval(tup, desc, 1, &isnull));
	if (isnull)
		return;
	arr = DatumGetArrayTypeP(SPI_getbinval(tup, desc, 2, &isnull));
	if (isnull)
		return;

	fe = (FeatureEntry *) hash_search(fa->features, &id, HASH_ENTER, &found);
	if (found)
		return;					/* first vertex label wins on key collision */

	deconstruct_array_builtin(arr, FLOAT8OID, &elems, &enulls, &nelems);
	fe->dim = nelems;
	fe->vals = (double *) palloc(sizeof(double) * Max(nelems, 1));
	for (int i = 0; i < nelems; i++)
	{
		if (enulls[i])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("feature of vertex %lld contains NULL elements", (long long) id)));
		fe->vals[i] = DatumGetFloat8(elems[i]);
	}
}

Datum
graph_node_features(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	LoadOptions opt;
	LoadedGraph *g;
	GraphDirection dir;
	ArrayType  *varr;
	Datum	   *vd;
	bool	   *vn;
	int			nv;
	char	   *fcol;
	char	   *schema;
	int64	   *mark;
	int32	   *nbuf;
	int64		stamp = 0;
	int64	   *needed;
	int64		nneeded = 0;
	int64	   *needed_mark;
	HASHCTL		ctl;
	FeatureRowArg fa;
	StringInfoData q;
	Oid			cat_types[2] = {TEXTOID, TEXTOID};
	Datum		cat_vals[2];
	SPITupleTable *cols;
	uint64		ncols;
	int			ret;

	InitMaterializedSRF(fcinfo, 0);
	load_opts_init(&opt, fcinfo, 0, 3, 4);
	if (PG_ARGISNULL(1))
		return (Datum) 0;
	varr = PG_GETARG_ARRAYTYPE_P(1);
	fcol = text_arg(fcinfo, 2, "feature_column");
	dir = direction_arg(fcinfo, 5);
	deconstruct_array(varr, INT8OID, sizeof(int64), FLOAT8PASSBYVAL,
					  TYPALIGN_DOUBLE, &vd, &vn, &nv);

	SPI_connect();
	g = graph_load(&opt, CurrentMemoryContext);
	graph_build_adjacency(g, dir);

	mark = (int64 *) MemoryContextAllocHuge(g->cxt, sizeof(int64) * Max(g->nverts, 1));
	needed_mark = (int64 *) MemoryContextAllocHuge(g->cxt, sizeof(int64) * Max(g->nverts, 1));
	nbuf = (int32 *) MemoryContextAllocHuge(g->cxt, sizeof(int32) * Max(g->nverts, 1));
	needed = (int64 *) MemoryContextAllocHuge(g->cxt, sizeof(int64) * Max(g->nverts, 1));
	for (int32 v = 0; v < g->nverts; v++)
		mark[v] = needed_mark[v] = -1;

	/* collect all distinct neighbors of the requested vertices */
	for (int i = 0; i < nv; i++)
	{
		int32		v;
		int64		n;

		if (vn[i])
			continue;
		v = graph_vertex_index(g, DatumGetInt64(vd[i]));
		if (v < 0)
			continue;
		n = distinct_neighbors(g, v, nbuf, mark, stamp++);
		for (int64 j = 0; j < n; j++)
		{
			if (needed_mark[nbuf[j]] != 0)
			{
				needed_mark[nbuf[j]] = 0;
				needed[nneeded++] = g->ids[nbuf[j]];
			}
		}
	}

	/* feature lookup */
	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(int64);
	ctl.entrysize = sizeof(FeatureEntry);
	ctl.hcxt = g->cxt;
	fa.features = hash_create("alohadb_graph features", Max(nneeded, 16), &ctl,
							  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	schema = graph_catalog_schema();
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT v.table_name::text, v.key_column, t.typcategory::text, "
					 "et.typcategory::text "
					 "FROM %s.alohadb_graph_vertex_labels v "
					 "JOIN pg_catalog.pg_attribute a ON a.attrelid = v.table_name "
					 " AND a.attname = $2::pg_catalog.name AND a.attnum > 0 AND NOT a.attisdropped "
					 "JOIN pg_catalog.pg_type t ON t.oid = a.atttypid "
					 "LEFT JOIN pg_catalog.pg_type et ON t.typcategory = 'A' AND et.oid = t.typelem "
					 "WHERE v.graph_name = $1 ORDER BY v.label",
					 schema);
	cat_vals[0] = CStringGetTextDatum(opt.graph);
	cat_vals[1] = CStringGetTextDatum(fcol);
	ret = SPI_execute_with_args(q.data, 2, cat_types, cat_vals, NULL, true, 0);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute_with_args failed: %s", SPI_result_code_string(ret));
	cols = SPI_tuptable;
	ncols = SPI_processed;
	if (ncols == 0)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("no vertex label of graph \"%s\" has a column \"%s\"",
						opt.graph, fcol)));

	if (nneeded > 0)
	{
		Datum		needed_arr = make_int8_array(needed, (int) nneeded);
		Oid			argtypes[1] = {INT8ARRAYOID};

		for (uint64 i = 0; i < ncols; i++)
		{
			char	   *tbl = spi_text(cols->vals[i], cols->tupdesc, 1);
			char	   *kcol = spi_text(cols->vals[i], cols->tupdesc, 2);
			char	   *cat = spi_text(cols->vals[i], cols->tupdesc, 3);
			char	   *ecat = spi_text(cols->vals[i], cols->tupdesc, 4);
			const char *qcol = quote_identifier(fcol);

			resetStringInfo(&q);
			appendStringInfo(&q, "SELECT t.%s::pg_catalog.int8, ", quote_identifier(kcol));
			if (strcmp(cat, "N") == 0)
				appendStringInfo(&q, "ARRAY[t.%s::pg_catalog.float8]", qcol);
			else if (strcmp(cat, "A") == 0 && ecat && strcmp(ecat, "N") == 0)
				appendStringInfo(&q, "t.%s::pg_catalog.float8[]", qcol);
			else
				appendStringInfo(&q,
								 "pg_catalog.translate(t.%s::pg_catalog.text, '[]', '{}')::pg_catalog.float8[]",
								 qcol);
			appendStringInfo(&q, " FROM %s t WHERE t.%s = ANY($1) AND t.%s IS NOT NULL",
							 tbl, quote_identifier(kcol), qcol);
			run_cursor(q.data, 1, argtypes, &needed_arr, NULL, g, feature_row_cb, &fa);
		}
	}

	/* aggregate */
	for (int i = 0; i < nv; i++)
	{
		int64		vid;
		int32		v;
		Datum		values[4];
		bool		nulls[4] = {false, false, false, false};
		int32		degree = 0;
		int32		count = 0;
		int32		dim = -1;
		double	   *sum = NULL;

		if (vn[i])
			continue;
		vid = DatumGetInt64(vd[i]);
		v = graph_vertex_index(g, vid);
		if (v >= 0)
		{
			int64		n;

			degree = (int32) (g->adj_off[v + 1] - g->adj_off[v]);
			n = distinct_neighbors(g, v, nbuf, mark, stamp++);
			for (int64 j = 0; j < n; j++)
			{
				int64		nid = g->ids[nbuf[j]];
				FeatureEntry *fe = (FeatureEntry *) hash_search(fa.features, &nid,
																HASH_FIND, NULL);

				if (fe == NULL)
					continue;
				if (dim < 0)
				{
					dim = fe->dim;
					sum = (double *) MemoryContextAllocZero(g->cxt, sizeof(double) * Max(dim, 1));
				}
				else if (fe->dim != dim)
					ereport(ERROR,
							(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
							 errmsg("feature dimensions differ (%d vs %d) among neighbors of vertex %lld",
									dim, fe->dim, (long long) vid)));
				for (int32 d = 0; d < dim; d++)
					sum[d] += fe->vals[d];
				count++;
			}
		}

		values[0] = Int64GetDatum(vid);
		values[1] = Int32GetDatum(degree);
		values[2] = Int32GetDatum(count);
		if (count > 0)
		{
			for (int32 d = 0; d < dim; d++)
				sum[d] /= count;
			values[3] = make_float8_array(sum, dim);
		}
		else
			nulls[3] = true;
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	MemoryContextDelete(g->cxt);
	SPI_finish();
	return (Datum) 0;
}
