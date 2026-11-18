/*-------------------------------------------------------------------------
 *
 * graph_algo.c
 *	  In-memory graph structures and textbook graph algorithms for
 *	  pg_graph: dense vertex numbering, CSR adjacency, Dijkstra with
 *	  a binary heap, PageRank power iteration and union-find.
 *
 *	  All memory is allocated in the LoadedGraph's private context, so a
 *	  caller frees everything by deleting that context.
 *
 * Copyright (c) 2026, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_graph/graph_algo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "miscadmin.h"
#include "utils/memutils.h"

#include "pg_graph.h"

static int
cmp_int64(const void *a, const void *b)
{
	int64		x = *(const int64 *) a;
	int64		y = *(const int64 *) b;

	return (x > y) - (x < y);
}

/*
 * Assign dense indexes (in ascending key order) to every key that appears
 * as an edge endpoint or in g->extra_ids, and fill in GraphEdge.si/di.
 */
void
graph_build_index(LoadedGraph *g)
{
	MemoryContext old = MemoryContextSwitchTo(g->cxt);
	int64		total = g->nedges * 2 + g->nextra;
	int64	   *all;
	int64		n = 0;
	int64		i;
	HASHCTL		ctl;

	all = (int64 *) MemoryContextAllocHuge(g->cxt, sizeof(int64) * Max(total, 1));
	for (i = 0; i < g->nedges; i++)
	{
		all[n++] = g->edges[i].src;
		all[n++] = g->edges[i].dst;
	}
	for (i = 0; i < g->nextra; i++)
		all[n++] = g->extra_ids[i];

	qsort(all, n, sizeof(int64), cmp_int64);

	/* unique */
	if (n > 0)
	{
		int64		w = 1;

		for (i = 1; i < n; i++)
			if (all[i] != all[w - 1])
				all[w++] = all[i];
		n = w;
	}
	if (n > PG_INT32_MAX - 1)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("graph has too many vertices")));

	g->nverts = (int32) n;
	g->ids = all;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(int64);
	ctl.entrysize = sizeof(VertexMapEntry);
	ctl.hcxt = g->cxt;
	g->idmap = hash_create("pg_graph vertex map", Max(n, 16), &ctl,
						   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	for (i = 0; i < n; i++)
	{
		VertexMapEntry *e;
		bool		found;

		e = (VertexMapEntry *) hash_search(g->idmap, &all[i], HASH_ENTER, &found);
		e->idx = (int32) i;
		if ((i & 0xFFFF) == 0)
			CHECK_FOR_INTERRUPTS();
	}

	for (i = 0; i < g->nedges; i++)
	{
		g->edges[i].si = graph_vertex_index(g, g->edges[i].src);
		g->edges[i].di = graph_vertex_index(g, g->edges[i].dst);
	}

	MemoryContextSwitchTo(old);
}

/* Dense index of a key, or -1 if the key is not part of the loaded graph. */
int32
graph_vertex_index(LoadedGraph *g, int64 id)
{
	VertexMapEntry *e;

	if (g->idmap == NULL)
		return -1;
	e = (VertexMapEntry *) hash_search(g->idmap, &id, HASH_FIND, NULL);
	return e ? e->idx : -1;
}

/* Sort arcs of one vertex by (target, label, weight, valid_from). */
typedef struct ArcSortItem
{
	int32		to;
	int64		edge;
} ArcSortItem;

static int
cmp_arc(const void *a, const void *b, void *arg)
{
	const ArcSortItem *x = (const ArcSortItem *) a;
	const ArcSortItem *y = (const ArcSortItem *) b;
	GraphEdge  *edges = (GraphEdge *) arg;
	GraphEdge  *ex,
			   *ey;

	if (x->to != y->to)
		return (x->to > y->to) - (x->to < y->to);
	ex = &edges[x->edge];
	ey = &edges[y->edge];
	if (ex->label != ey->label)
		return (ex->label > ey->label) - (ex->label < ey->label);
	if (ex->weight != ey->weight)
		return (ex->weight > ey->weight) - (ex->weight < ey->weight);
	if (ex->vf_null != ey->vf_null)
		return ex->vf_null ? -1 : 1;
	if (!ex->vf_null && ex->valid_from != ey->valid_from)
		return (ex->valid_from > ey->valid_from) - (ex->valid_from < ey->valid_from);
	return (x->edge > y->edge) - (x->edge < y->edge);
}

/*
 * Build CSR adjacency for the given traversal direction.
 *
 * A directed edge s->d yields arc s->d for GDIR_OUT, d->s for GDIR_IN and
 * both for GDIR_BOTH.  An undirected edge always yields both arcs.  A
 * self-loop yields a single arc.
 */
void
graph_build_adjacency(LoadedGraph *g, GraphDirection dir)
{
	int64	   *deg;
	int64	   *pos;
	ArcSortItem *arcs;
	int64		narcs;
	int64		i;
	int32		v;

	g->adj_off = (int64 *) MemoryContextAllocHuge(g->cxt, sizeof(int64) * ((Size) g->nverts + 1));
	deg = (int64 *) MemoryContextAllocHuge(g->cxt, sizeof(int64) * ((Size) g->nverts + 1));
	memset(deg, 0, sizeof(int64) * ((Size) g->nverts + 1));

	/* pass 1: count */
	for (i = 0; i < g->nedges; i++)
	{
		GraphEdge  *e = &g->edges[i];
		bool		fwd = (!e->directed || dir != GDIR_IN);
		bool		bwd = (!e->directed || dir != GDIR_OUT);

		if (e->si == e->di)
		{
			deg[e->si]++;
			continue;
		}
		if (fwd)
			deg[e->si]++;
		if (bwd)
			deg[e->di]++;
	}

	g->adj_off[0] = 0;
	for (v = 0; v < g->nverts; v++)
		g->adj_off[v + 1] = g->adj_off[v] + deg[v];
	narcs = g->adj_off[g->nverts];

	arcs = (ArcSortItem *) MemoryContextAllocHuge(g->cxt, sizeof(ArcSortItem) * Max(narcs, 1));
	pos = deg;					/* reuse as fill cursor */
	for (v = 0; v < g->nverts; v++)
		pos[v] = g->adj_off[v];

	/* pass 2: fill */
	for (i = 0; i < g->nedges; i++)
	{
		GraphEdge  *e = &g->edges[i];
		bool		fwd = (!e->directed || dir != GDIR_IN);
		bool		bwd = (!e->directed || dir != GDIR_OUT);

		if (e->si == e->di)
		{
			arcs[pos[e->si]].to = e->si;
			arcs[pos[e->si]++].edge = i;
			continue;
		}
		if (fwd)
		{
			arcs[pos[e->si]].to = e->di;
			arcs[pos[e->si]++].edge = i;
		}
		if (bwd)
		{
			arcs[pos[e->di]].to = e->si;
			arcs[pos[e->di]++].edge = i;
		}
	}

	/* sort each vertex's arcs for deterministic traversal order */
	for (v = 0; v < g->nverts; v++)
	{
		int64		cnt = g->adj_off[v + 1] - g->adj_off[v];

		if (cnt > 1)
			qsort_arg(&arcs[g->adj_off[v]], cnt, sizeof(ArcSortItem), cmp_arc, g->edges);
		if ((v & 0xFFF) == 0)
			CHECK_FOR_INTERRUPTS();
	}

	g->adj_to = (int32 *) MemoryContextAllocHuge(g->cxt, sizeof(int32) * Max(narcs, 1));
	g->adj_edge = (int64 *) MemoryContextAllocHuge(g->cxt, sizeof(int64) * Max(narcs, 1));
	for (i = 0; i < narcs; i++)
	{
		g->adj_to[i] = arcs[i].to;
		g->adj_edge[i] = arcs[i].edge;
	}
	pfree(arcs);
	pfree(deg);

	g->has_adj = true;
	g->adj_dir = dir;
}

/* ----------------------------------------------------------------
 * Dijkstra with a binary min-heap (lazy deletion).
 * ----------------------------------------------------------------
 */
typedef struct HeapItem
{
	double		dist;
	int32		v;
} HeapItem;

typedef struct MinHeap
{
	HeapItem   *items;
	int64		n;
	int64		cap;
	MemoryContext cxt;
} MinHeap;

static void
heap_push(MinHeap *h, double dist, int32 v)
{
	int64		i;

	if (h->n >= h->cap)
	{
		h->cap *= 2;
		h->items = (HeapItem *) repalloc_huge(h->items, sizeof(HeapItem) * h->cap);
	}
	i = h->n++;
	while (i > 0)
	{
		int64		p = (i - 1) / 2;

		if (h->items[p].dist <= dist)
			break;
		h->items[i] = h->items[p];
		i = p;
	}
	h->items[i].dist = dist;
	h->items[i].v = v;
}

static HeapItem
heap_pop(MinHeap *h)
{
	HeapItem	top = h->items[0];
	HeapItem	last = h->items[--h->n];
	int64		i = 0;

	while (true)
	{
		int64		c = 2 * i + 1;

		if (c >= h->n)
			break;
		if (c + 1 < h->n && h->items[c + 1].dist < h->items[c].dist)
			c++;
		if (last.dist <= h->items[c].dist)
			break;
		h->items[i] = h->items[c];
		i = c;
	}
	if (h->n > 0)
		h->items[i] = last;
	return top;
}

/*
 * Single-source shortest paths from s, stopping early once t is settled
 * (t may be -1 to compute all).  dist[] receives costs (INFINITY when
 * unreachable) and parent[] the predecessor index (-1 for none).
 */
void
graph_dijkstra(LoadedGraph *g, int32 s, int32 t, double *dist, int32 *parent)
{
	MinHeap		h;
	bool	   *done;
	int32		v;

	Assert(g->has_adj);

	done = (bool *) MemoryContextAllocHuge(g->cxt, sizeof(bool) * Max(g->nverts, 1));
	for (v = 0; v < g->nverts; v++)
	{
		dist[v] = INFINITY;
		parent[v] = -1;
		done[v] = false;
	}

	h.cap = 64;
	h.n = 0;
	h.cxt = g->cxt;
	h.items = (HeapItem *) MemoryContextAlloc(g->cxt, sizeof(HeapItem) * h.cap);

	dist[s] = 0.0;
	heap_push(&h, 0.0, s);

	while (h.n > 0)
	{
		HeapItem	it = heap_pop(&h);
		int64		a;

		CHECK_FOR_INTERRUPTS();

		if (done[it.v])
			continue;
		done[it.v] = true;
		if (it.v == t)
			break;

		for (a = g->adj_off[it.v]; a < g->adj_off[it.v + 1]; a++)
		{
			int32		w = g->adj_to[a];
			double		nd = it.dist + g->edges[g->adj_edge[a]].weight;

			if (!done[w] && nd < dist[w])
			{
				dist[w] = nd;
				parent[w] = it.v;
				heap_push(&h, nd, w);
			}
		}
	}

	pfree(h.items);
	pfree(done);
}

/* ----------------------------------------------------------------
 * PageRank (power iteration).
 *
 * r_{k+1}(v) = (1-d)/N + d * ( sum_{u->v} r_k(u)/outdeg(u) + D_k/N )
 * where D_k is the total rank of vertices without out-arcs (dangling
 * mass, redistributed uniformly).  Multi-edges count with multiplicity.
 * Requires adjacency built with GDIR_OUT.
 * ----------------------------------------------------------------
 */
double *
graph_pagerank_compute(LoadedGraph *g, double damping, int iterations)
{
	int32		n = g->nverts;
	double	   *r;
	double	   *nr;
	int32		v;
	int			it;

	Assert(g->has_adj && g->adj_dir == GDIR_OUT);

	r = (double *) MemoryContextAllocHuge(g->cxt, sizeof(double) * Max(n, 1));
	nr = (double *) MemoryContextAllocHuge(g->cxt, sizeof(double) * Max(n, 1));
	if (n == 0)
		return r;

	for (v = 0; v < n; v++)
		r[v] = 1.0 / n;

	for (it = 0; it < iterations; it++)
	{
		double		dangling = 0.0;
		double		base;

		CHECK_FOR_INTERRUPTS();

		for (v = 0; v < n; v++)
			nr[v] = 0.0;

		for (v = 0; v < n; v++)
		{
			int64		outdeg = g->adj_off[v + 1] - g->adj_off[v];
			int64		a;

			if (outdeg == 0)
			{
				dangling += r[v];
				continue;
			}
			for (a = g->adj_off[v]; a < g->adj_off[v + 1]; a++)
				nr[g->adj_to[a]] += r[v] / outdeg;
		}

		base = (1.0 - damping) / n + damping * dangling / n;
		for (v = 0; v < n; v++)
			nr[v] = base + damping * nr[v];

		/* swap */
		{
			double	   *tmp = r;

			r = nr;
			nr = tmp;
		}
	}

	pfree(nr);
	return r;
}

/* ----------------------------------------------------------------
 * Weakly connected components: union-find with path halving and
 * union by size.  Returns, per dense index, the smallest vertex key of
 * its component.
 * ----------------------------------------------------------------
 */
static int32
uf_find(int32 *parent, int32 x)
{
	while (parent[x] != x)
	{
		parent[x] = parent[parent[x]];
		x = parent[x];
	}
	return x;
}

int64 *
graph_components_compute(LoadedGraph *g)
{
	int32		n = g->nverts;
	int32	   *parent;
	int32	   *size;
	int64	   *minkey;
	int64	   *result;
	int64		i;
	int32		v;

	parent = (int32 *) MemoryContextAllocHuge(g->cxt, sizeof(int32) * Max(n, 1));
	size = (int32 *) MemoryContextAllocHuge(g->cxt, sizeof(int32) * Max(n, 1));
	minkey = (int64 *) MemoryContextAllocHuge(g->cxt, sizeof(int64) * Max(n, 1));
	result = (int64 *) MemoryContextAllocHuge(g->cxt, sizeof(int64) * Max(n, 1));

	for (v = 0; v < n; v++)
	{
		parent[v] = v;
		size[v] = 1;
	}

	for (i = 0; i < g->nedges; i++)
	{
		int32		a = uf_find(parent, g->edges[i].si);
		int32		b = uf_find(parent, g->edges[i].di);

		if ((i & 0xFFFF) == 0)
			CHECK_FOR_INTERRUPTS();
		if (a == b)
			continue;
		if (size[a] < size[b])
		{
			int32		t = a;

			a = b;
			b = t;
		}
		parent[b] = a;
		size[a] += size[b];
	}

	/* ids[] is ascending, so the first member seen is the minimum key */
	for (v = 0; v < n; v++)
		minkey[v] = PG_INT64_MAX;
	for (v = 0; v < n; v++)
	{
		int32		r = uf_find(parent, v);

		if (minkey[r] == PG_INT64_MAX)
			minkey[r] = g->ids[v];
		result[v] = minkey[r];
	}

	pfree(parent);
	pfree(size);
	pfree(minkey);
	return result;
}
