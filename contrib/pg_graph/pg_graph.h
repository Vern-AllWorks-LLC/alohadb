/*-------------------------------------------------------------------------
 *
 * pg_graph.h
 *	  Shared declarations for the pg_graph extension.
 *
 * Copyright (c) 2026, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_graph/pg_graph.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ALOHADB_GRAPH_H
#define ALOHADB_GRAPH_H

#include "datatype/timestamp.h"
#include "utils/hsearch.h"

/* Which arcs of a directed edge are followed. */
typedef enum GraphDirection
{
	GDIR_OUT,
	GDIR_IN,
	GDIR_BOTH
} GraphDirection;

/* One edge row as loaded from an edge table. */
typedef struct GraphEdge
{
	int64		src;			/* source vertex key */
	int64		dst;			/* destination vertex key */
	int32		si;				/* dense index of src */
	int32		di;				/* dense index of dst */
	double		weight;			/* 1.0 when the label has no weight column */
	TimestampTz valid_from;		/* only meaningful if !vf_null */
	TimestampTz valid_to;		/* only meaningful if !vt_null */
	int16		label;			/* index into LoadedGraph.label_names */
	bool		directed;
	bool		vf_null;
	bool		vt_null;
} GraphEdge;

typedef struct VertexMapEntry
{
	int64		id;				/* hash key */
	int32		idx;
} VertexMapEntry;

/*
 * In-memory snapshot of (part of) a graph.  Vertices are numbered densely
 * 0..nverts-1 in ascending key order; adjacency is stored in CSR form,
 * each vertex's arcs sorted by target key so results are deterministic.
 */
typedef struct LoadedGraph
{
	MemoryContext cxt;

	int			nlabels;
	char	  **label_names;

	int64		nedges;
	int64		edges_alloc;
	GraphEdge  *edges;

	int32		nverts;
	int64	   *ids;			/* dense index -> key */
	HTAB	   *idmap;			/* key -> dense index */

	/* extra vertex keys (e.g. isolated vertices from vertex tables) */
	int64		nextra;
	int64		extra_alloc;
	int64	   *extra_ids;

	/* CSR adjacency (built by graph_build_adjacency) */
	bool		has_adj;
	GraphDirection adj_dir;
	int64	   *adj_off;		/* nverts + 1 */
	int32	   *adj_to;			/* target dense index */
	int64	   *adj_edge;		/* index into edges */
} LoadedGraph;

/* graph_algo.c */
extern void graph_build_index(LoadedGraph *g);
extern void graph_build_adjacency(LoadedGraph *g, GraphDirection dir);
extern int32 graph_vertex_index(LoadedGraph *g, int64 id);
extern void graph_dijkstra(LoadedGraph *g, int32 s, int32 t,
						   double *dist, int32 *parent);
extern double *graph_pagerank_compute(LoadedGraph *g, double damping,
									  int iterations);
extern int64 *graph_components_compute(LoadedGraph *g);

#endif							/* ALOHADB_GRAPH_H */
