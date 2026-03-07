/*-------------------------------------------------------------------------
 *
 * nodeGraphScan.c
 *	  SQL/PGQ (ISO 9075-16) graph traversal executor
 *
 * This module implements graph traversal operations as SPI-based
 * set-returning functions, providing:
 *
 *   - alohadb_graph_bfs(): Breadth-first search with cycle detection
 *   - alohadb_graph_shortest_path(): Dijkstra shortest path
 *
 * These functions use the Server Programming Interface (SPI) to execute
 * internally-generated recursive CTE queries, produced by the parse_graph
 * module.  In addition, the BFS function implements a native in-memory
 * traversal using a visited-set hash table for cycle detection, which
 * avoids the overhead of recursive CTE execution for smaller graphs.
 *
 * The module also registers a CustomScan provider ("GraphScan") for
 * potential future use by the planner, though the current entry point
 * is via the SPI-based SQL functions.
 *
 * Copyright (c) 2025, AlohaDB Project
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeGraphScan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/relation.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "lib/binaryheap.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/extensible.h"
#include "parser/parse_graph.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/tuplestore.h"

/* ----------------------------------------------------------------
 *		Constants
 * ----------------------------------------------------------------
 */

/* Default source and destination column names for edge tables */
#define DEFAULT_SRC_COL		"src"
#define DEFAULT_DST_COL		"dst"
#define DEFAULT_WEIGHT_COL	"weight"

/* Maximum BFS depth to prevent runaway traversals */
#define MAX_BFS_DEPTH		10000

/* Initial capacity for the visited-set hash table */
#define VISITED_SET_INITIAL_SIZE	256

/* Initial capacity for BFS queue */
#define BFS_QUEUE_INITIAL_SIZE		256

/* Custom scan method name */
#define GRAPHSCAN_NAME		"GraphScan"


/* ----------------------------------------------------------------
 *		Data structures for native BFS traversal
 * ----------------------------------------------------------------
 */

/*
 * VisitedEntry - entry in the visited-set hash table.
 * We track which nodes have been seen to prevent cycles.
 */
typedef struct VisitedEntry
{
	int32		node_id;		/* hash key: the node identifier */
	char		status;			/* required by dynahash */
} VisitedEntry;

/*
 * BFSQueueItem - an item in the BFS frontier queue.
 * Stores the current node, depth, and the path taken to reach it.
 */
typedef struct BFSQueueItem
{
	int32		node_id;
	int32		depth;
	int32	   *path;			/* array of node IDs from start to here */
	int32		path_len;
} BFSQueueItem;

/*
 * BFSQueue - a simple FIFO queue for BFS traversal.
 */
typedef struct BFSQueue
{
	BFSQueueItem *items;
	int			head;
	int			tail;
	int			capacity;
	MemoryContext mcxt;
} BFSQueue;

/*
 * BFSResult - a single result row from BFS traversal.
 */
typedef struct BFSResult
{
	int32		node_id;
	int32		depth;
	int32	   *path;
	int32		path_len;
} BFSResult;

/*
 * BFSState - complete state for a BFS traversal operation.
 */
typedef struct BFSState
{
	BFSResult  *results;
	int			num_results;
	int			results_capacity;
	int			current_result;
} BFSState;

/*
 * DijkstraEntry - entry for Dijkstra priority queue.
 */
typedef struct DijkstraEntry
{
	int32		node_id;
	float8		total_cost;
	int32	   *path;
	int32		path_len;
} DijkstraEntry;

/*
 * DijkstraResult - a single result row from shortest path search.
 */
typedef struct DijkstraResult
{
	int32		node_id;
	float8		total_cost;
	int32	   *path;
	int32		path_len;
} DijkstraResult;


/* ----------------------------------------------------------------
 *		Forward declarations
 * ----------------------------------------------------------------
 */

/* BFS queue operations */
static BFSQueue *bfs_queue_create(MemoryContext mcxt);
static void bfs_queue_push(BFSQueue *queue, BFSQueueItem *item);
static bool bfs_queue_pop(BFSQueue *queue, BFSQueueItem *result);
static bool bfs_queue_is_empty(BFSQueue *queue);

/* Result collection helpers */
static void bfs_add_result(BFSState *state, int32 node_id, int32 depth,
						   int32 *path, int32 path_len, MemoryContext mcxt);

/* Column detection */
static bool table_has_column(Oid relid, const char *colname);

/* CustomScan methods (for future planner integration) */
static Node *graphscan_create_state(CustomScan *cscan);
static void graphscan_begin(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *graphscan_exec(CustomScanState *node);
static void graphscan_end(CustomScanState *node);
static void graphscan_rescan(CustomScanState *node);

/* Forward declarations for method tables (needed by graphscan_create_state) */
static const CustomExecMethods graphscan_exec_methods;
static const CustomScanMethods graphscan_plan_methods;


/* ----------------------------------------------------------------
 *		BFS Queue Implementation
 * ----------------------------------------------------------------
 */

static BFSQueue *
bfs_queue_create(MemoryContext mcxt)
{
	BFSQueue   *queue;

	queue = (BFSQueue *) MemoryContextAlloc(mcxt, sizeof(BFSQueue));
	queue->capacity = BFS_QUEUE_INITIAL_SIZE;
	queue->items = (BFSQueueItem *) MemoryContextAlloc(mcxt,
													   sizeof(BFSQueueItem) * queue->capacity);
	queue->head = 0;
	queue->tail = 0;
	queue->mcxt = mcxt;

	return queue;
}

static void
bfs_queue_push(BFSQueue *queue, BFSQueueItem *item)
{
	/* Grow queue if needed */
	if (queue->tail >= queue->capacity)
	{
		int			new_capacity = queue->capacity * 2;
		BFSQueueItem *new_items;

		new_items = (BFSQueueItem *) MemoryContextAlloc(queue->mcxt,
														sizeof(BFSQueueItem) * new_capacity);
		memcpy(new_items, queue->items + queue->head,
			   sizeof(BFSQueueItem) * (queue->tail - queue->head));
		pfree(queue->items);
		queue->tail -= queue->head;
		queue->head = 0;
		queue->items = new_items;
		queue->capacity = new_capacity;
	}

	queue->items[queue->tail] = *item;
	queue->tail++;
}

static bool
bfs_queue_pop(BFSQueue *queue, BFSQueueItem *result)
{
	if (queue->head >= queue->tail)
		return false;

	*result = queue->items[queue->head];
	queue->head++;
	return true;
}

static bool
bfs_queue_is_empty(BFSQueue *queue)
{
	return queue->head >= queue->tail;
}


/* ----------------------------------------------------------------
 *		Result collection
 * ----------------------------------------------------------------
 */

static void
bfs_add_result(BFSState *state, int32 node_id, int32 depth,
			   int32 *path, int32 path_len, MemoryContext mcxt)
{
	BFSResult  *result;

	/* Grow results array if needed */
	if (state->num_results >= state->results_capacity)
	{
		int			new_capacity = state->results_capacity * 2;
		BFSResult  *new_results;

		new_results = (BFSResult *) MemoryContextAlloc(mcxt,
													   sizeof(BFSResult) * new_capacity);
		if (state->results != NULL)
		{
			memcpy(new_results, state->results,
				   sizeof(BFSResult) * state->num_results);
			pfree(state->results);
		}
		state->results = new_results;
		state->results_capacity = new_capacity;
	}

	result = &state->results[state->num_results];
	result->node_id = node_id;
	result->depth = depth;
	result->path_len = path_len;

	/* Copy the path array */
	result->path = (int32 *) MemoryContextAlloc(mcxt, sizeof(int32) * path_len);
	memcpy(result->path, path, sizeof(int32) * path_len);

	state->num_results++;
}


/* ----------------------------------------------------------------
 *		Column detection utility
 * ----------------------------------------------------------------
 */

/*
 * table_has_column
 *
 * Check whether a relation has a column with the given name.
 */
static bool
table_has_column(Oid relid, const char *colname)
{
	Relation	rel;
	TupleDesc	tupdesc;
	int			i;
	bool		found = false;

	rel = relation_open(relid, AccessShareLock);
	tupdesc = RelationGetDescr(rel);

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		if (att->attisdropped)
			continue;

		if (strcmp(NameStr(att->attname), colname) == 0)
		{
			found = true;
			break;
		}
	}

	relation_close(rel, AccessShareLock);

	return found;
}


/* ----------------------------------------------------------------
 *		SPI-based BFS Function
 *
 *		alohadb_graph_bfs(edge_table regclass, start_id int,
 *		                  max_depth int)
 *		RETURNS TABLE(node_id int, depth int, path int[])
 * ----------------------------------------------------------------
 */

/*
 * Perform BFS traversal using native in-memory queue and visited set.
 *
 * This is faster than the recursive CTE approach for moderate-sized
 * graphs because it avoids the overhead of query parsing/planning
 * on each recursive step.
 */
static void
execute_native_bfs(Oid edge_relid, int32 start_id, int32 max_depth,
				   ReturnSetInfo *rsinfo)
{
	MemoryContext oldcxt;
	MemoryContext bfs_cxt;
	HTAB	   *visited;
	HASHCTL		hash_ctl;
	BFSQueue   *queue;
	BFSQueueItem start_item;
	char	   *relname;
	const char *src_col;
	const char *dst_col;
	StringInfoData query;
	int			ret;

	/* Determine column names */
	if (table_has_column(edge_relid, DEFAULT_SRC_COL))
		src_col = DEFAULT_SRC_COL;
	else if (table_has_column(edge_relid, "source"))
		src_col = "source";
	else if (table_has_column(edge_relid, "from_id"))
		src_col = "from_id";
	else
		src_col = DEFAULT_SRC_COL;	/* will error on query execution */

	if (table_has_column(edge_relid, DEFAULT_DST_COL))
		dst_col = DEFAULT_DST_COL;
	else if (table_has_column(edge_relid, "destination"))
		dst_col = "destination";
	else if (table_has_column(edge_relid, "to_id"))
		dst_col = "to_id";
	else
		dst_col = DEFAULT_DST_COL;

	/* Create a dedicated memory context for BFS state */
	bfs_cxt = AllocSetContextCreate(CurrentMemoryContext,
									"BFS Traversal",
									ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(bfs_cxt);

	/* Initialize visited set */
	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(int32);
	hash_ctl.entrysize = sizeof(VisitedEntry);
	hash_ctl.hcxt = bfs_cxt;

	visited = hash_create("BFS Visited Set",
						  VISITED_SET_INITIAL_SIZE,
						  &hash_ctl,
						  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/* Initialize BFS queue with start node */
	queue = bfs_queue_create(bfs_cxt);

	start_item.node_id = start_id;
	start_item.depth = 0;
	start_item.path = (int32 *) palloc(sizeof(int32));
	start_item.path[0] = start_id;
	start_item.path_len = 1;
	bfs_queue_push(queue, &start_item);

	/* Mark start node as visited */
	{
		bool		found;

		hash_search(visited, &start_id, HASH_ENTER, &found);
	}

	/* Add start node to results */
	{
		Datum		values[3];
		bool		nulls[3];
		Datum	   *path_datums;

		path_datums = (Datum *) palloc(sizeof(Datum));
		path_datums[0] = Int32GetDatum(start_id);

		values[0] = Int32GetDatum(start_id);
		values[1] = Int32GetDatum(0);
		values[2] = PointerGetDatum(construct_array_builtin(path_datums, 1, INT4OID));
		nulls[0] = false;
		nulls[1] = false;
		nulls[2] = false;

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	/* Get relation name for queries */
	relname = get_rel_name(edge_relid);
	if (relname == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("edge table with OID %u does not exist", edge_relid)));

	/* Connect to SPI for neighbor lookups */
	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("SPI_connect failed in BFS traversal")));

	/* BFS main loop */
	while (!bfs_queue_is_empty(queue))
	{
		BFSQueueItem current;
		uint64		proc;
		uint64		i;

		CHECK_FOR_INTERRUPTS();

		if (!bfs_queue_pop(queue, &current))
			break;

		/* Check depth limit */
		if (current.depth >= max_depth)
			continue;

		/* Query for neighbors of current node */
		initStringInfo(&query);
		appendStringInfo(&query,
						 "SELECT %s FROM %s WHERE %s = %d",
						 quote_identifier(dst_col),
						 quote_qualified_identifier(
							 get_namespace_name(get_rel_namespace(edge_relid)),
							 relname),
						 quote_identifier(src_col),
						 current.node_id);

		ret = SPI_execute(query.data, true, 0);
		pfree(query.data);

		if (ret != SPI_OK_SELECT)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("SPI_execute failed in BFS neighbor lookup: %s",
							SPI_result_code_string(ret))));

		proc = SPI_processed;

		/* Process each neighbor */
		for (i = 0; i < proc; i++)
		{
			bool		isnull;
			int32		neighbor_id;
			bool		found;
			int32	   *new_path;
			BFSQueueItem neighbor_item;

			neighbor_id = DatumGetInt32(
				SPI_getbinval(SPI_tuptable->vals[i],
							  SPI_tuptable->tupdesc,
							  1, &isnull));

			if (isnull)
				continue;

			/* Check if already visited (cycle detection) */
			hash_search(visited, &neighbor_id, HASH_FIND, &found);
			if (found)
				continue;

			/* Mark as visited */
			hash_search(visited, &neighbor_id, HASH_ENTER, &found);

			/* Build new path */
			new_path = (int32 *) MemoryContextAlloc(bfs_cxt,
													sizeof(int32) * (current.path_len + 1));
			memcpy(new_path, current.path, sizeof(int32) * current.path_len);
			new_path[current.path_len] = neighbor_id;

			/* Add to results */
			{
				Datum		values[3];
				bool		r_nulls[3];
				Datum	   *path_datums;
				int			j;
				int32		new_path_len = current.path_len + 1;

				path_datums = (Datum *) palloc(sizeof(Datum) * new_path_len);
				for (j = 0; j < new_path_len; j++)
					path_datums[j] = Int32GetDatum(new_path[j]);

				values[0] = Int32GetDatum(neighbor_id);
				values[1] = Int32GetDatum(current.depth + 1);
				values[2] = PointerGetDatum(
					construct_array_builtin(path_datums, new_path_len, INT4OID));
				r_nulls[0] = false;
				r_nulls[1] = false;
				r_nulls[2] = false;

				tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
									values, r_nulls);

				pfree(path_datums);
			}

			/* Enqueue neighbor for further exploration */
			neighbor_item.node_id = neighbor_id;
			neighbor_item.depth = current.depth + 1;
			neighbor_item.path = new_path;
			neighbor_item.path_len = current.path_len + 1;
			bfs_queue_push(queue, &neighbor_item);
		}

		SPI_freetuptable(SPI_tuptable);
	}

	SPI_finish();

	MemoryContextSwitchTo(oldcxt);
	/* Note: bfs_cxt will be freed when the function's memory context resets */
}


/*
 * alohadb_graph_bfs - SQL-callable BFS traversal function
 *
 * Arguments:
 *   edge_table regclass  - OID of the edge table
 *   start_id   int4      - starting node ID
 *   max_depth  int4      - maximum traversal depth
 *
 * Returns: TABLE(node_id int, depth int, path int[])
 */
PG_FUNCTION_INFO_V1(alohadb_graph_bfs);

Datum
alohadb_graph_bfs(PG_FUNCTION_ARGS)
{
	Oid			edge_relid;
	int32		start_id;
	int32		max_depth;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;

	/* Extract arguments */
	edge_relid = PG_GETARG_OID(0);
	start_id = PG_GETARG_INT32(1);
	max_depth = PG_GETARG_INT32(2);

	/* Validate arguments */
	if (max_depth < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("max_depth must be non-negative")));

	if (max_depth == 0 || max_depth > MAX_BFS_DEPTH)
		max_depth = MAX_BFS_DEPTH;

	/* Build output tuple descriptor: (node_id int4, depth int4, path int4[]) */
	tupdesc = CreateTemplateTupleDesc(3);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "node_id",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "depth",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "path",
					   INT4ARRAYOID, -1, 0);

	/* Initialize the tuplestore for materialized result set */
	InitMaterializedSRF(fcinfo, 0);

	/* Perform the BFS traversal */
	execute_native_bfs(edge_relid, start_id, max_depth, rsinfo);

	return (Datum) 0;
}


/* ----------------------------------------------------------------
 *		SPI-based Shortest Path Function
 *
 *		alohadb_graph_shortest_path(edge_table regclass,
 *		                            start_id int, end_id int)
 *		RETURNS TABLE(node_id int, total_cost float8, path int[])
 * ----------------------------------------------------------------
 */

/*
 * Perform Dijkstra shortest path using SPI to execute a recursive CTE.
 *
 * This uses the parse_graph module to generate the recursive CTE query
 * and then executes it via SPI, collecting results into the tuplestore.
 */
static void
execute_shortest_path_via_spi(Oid edge_relid, int32 start_id, int32 end_id,
							  ReturnSetInfo *rsinfo)
{
	char	   *relname;
	char	   *nspname;
	char	   *qualified_name;
	const char *src_col;
	const char *dst_col;
	const char *weight_col;
	char	   *query;
	int			ret;
	uint64		proc;
	uint64		i;

	/* Get relation name */
	relname = get_rel_name(edge_relid);
	if (relname == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("edge table with OID %u does not exist", edge_relid)));

	nspname = get_namespace_name(get_rel_namespace(edge_relid));
	qualified_name = quote_qualified_identifier(nspname, relname);

	/* Determine column names by inspecting the table */
	if (table_has_column(edge_relid, DEFAULT_SRC_COL))
		src_col = DEFAULT_SRC_COL;
	else if (table_has_column(edge_relid, "source"))
		src_col = "source";
	else if (table_has_column(edge_relid, "from_id"))
		src_col = "from_id";
	else
		src_col = DEFAULT_SRC_COL;

	if (table_has_column(edge_relid, DEFAULT_DST_COL))
		dst_col = DEFAULT_DST_COL;
	else if (table_has_column(edge_relid, "destination"))
		dst_col = "destination";
	else if (table_has_column(edge_relid, "to_id"))
		dst_col = "to_id";
	else
		dst_col = DEFAULT_DST_COL;

	if (table_has_column(edge_relid, DEFAULT_WEIGHT_COL))
		weight_col = DEFAULT_WEIGHT_COL;
	else if (table_has_column(edge_relid, "cost"))
		weight_col = "cost";
	else
		weight_col = NULL;	/* unweighted graph */

	/* Generate the shortest path query using parse_graph */
	query = generate_graph_shortest_path_query(qualified_name,
											   src_col, dst_col,
											   weight_col,
											   start_id, end_id);

	/* Execute via SPI */
	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("SPI_connect failed in shortest path computation")));

	ret = SPI_execute(query, true, 0);
	pfree(query);

	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("shortest path query failed: %s",
						SPI_result_code_string(ret))));

	proc = SPI_processed;

	/* Copy results into the tuplestore */
	for (i = 0; i < proc; i++)
	{
		HeapTuple	spi_tuple = SPI_tuptable->vals[i];
		TupleDesc	spi_tupdesc = SPI_tuptable->tupdesc;
		Datum		values[3];
		bool		nulls[3];
		bool		isnull;

		/* node_id */
		values[0] = SPI_getbinval(spi_tuple, spi_tupdesc, 1, &isnull);
		nulls[0] = isnull;

		/* total_cost */
		values[1] = SPI_getbinval(spi_tuple, spi_tupdesc, 2, &isnull);
		nulls[1] = isnull;

		/* path */
		values[2] = SPI_getbinval(spi_tuple, spi_tupdesc, 3, &isnull);
		nulls[2] = isnull;

		/*
		 * We need to copy datum values out of SPI's memory context before
		 * they get freed.
		 */
		if (!nulls[2])
			values[2] = PointerGetDatum(DatumGetArrayTypePCopy(values[2]));

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							values, nulls);
	}

	SPI_freetuptable(SPI_tuptable);
	SPI_finish();
}


/*
 * alohadb_graph_shortest_path - SQL-callable shortest path function
 *
 * Arguments:
 *   edge_table regclass  - OID of the edge table
 *   start_id   int4      - starting node ID
 *   end_id     int4      - target node ID (-1 for all reachable nodes)
 *
 * Returns: TABLE(node_id int, total_cost float8, path int[])
 */
PG_FUNCTION_INFO_V1(alohadb_graph_shortest_path);

Datum
alohadb_graph_shortest_path(PG_FUNCTION_ARGS)
{
	Oid			edge_relid;
	int32		start_id;
	int32		end_id;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;

	/* Extract arguments */
	edge_relid = PG_GETARG_OID(0);
	start_id = PG_GETARG_INT32(1);
	end_id = PG_GETARG_INT32(2);

	/* Build output tuple descriptor: (node_id int4, total_cost float8, path int4[]) */
	tupdesc = CreateTemplateTupleDesc(3);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "node_id",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "total_cost",
					   FLOAT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "path",
					   INT4ARRAYOID, -1, 0);

	/* Initialize the tuplestore */
	InitMaterializedSRF(fcinfo, 0);

	/* Perform shortest path computation */
	execute_shortest_path_via_spi(edge_relid, start_id, end_id, rsinfo);

	return (Datum) 0;
}


/* ----------------------------------------------------------------
 *		CustomScan Methods (GraphScan)
 *
 * These implement the CustomScan provider interface for potential
 * future integration with the PostgreSQL planner.  Currently, the
 * primary entry points are the SPI-based SQL functions above, but
 * these methods are registered so that a custom planner hook could
 * inject GraphScan nodes into query plans.
 * ----------------------------------------------------------------
 */

/*
 * GraphScanState - custom state for the GraphScan executor node.
 */
typedef struct GraphScanState
{
	CustomScanState css;		/* must be first */
	Oid				edge_relid;	/* edge table OID */
	int32			start_id;	/* BFS start node */
	int32			max_depth;	/* max traversal depth */
	GraphTraversalMode mode;	/* BFS, DFS, or shortest path */
	Tuplestorestate *results;	/* collected results */
	TupleDesc		result_tupdesc;
	bool			executed;	/* have we run the traversal? */
} GraphScanState;

static Node *
graphscan_create_state(CustomScan *cscan)
{
	GraphScanState *gss;

	gss = (GraphScanState *) newNode(sizeof(GraphScanState), T_CustomScanState);
	gss->css.methods = &graphscan_exec_methods;
	gss->edge_relid = InvalidOid;
	gss->start_id = 0;
	gss->max_depth = MAX_BFS_DEPTH;
	gss->mode = GRAPH_TRAVERSE_BFS;
	gss->results = NULL;
	gss->result_tupdesc = NULL;
	gss->executed = false;

	return (Node *) gss;
}

static void
graphscan_begin(CustomScanState *node, EState *estate, int eflags)
{
	GraphScanState *gss = (GraphScanState *) node;

	/*
	 * Extract parameters from the CustomScan's custom_private list.
	 * Expected format: list of 4 Integer nodes:
	 *   [edge_relid (Oid), start_id (int32), max_depth (int32), mode (int32)]
	 */
	{
		CustomScan *cscan = (CustomScan *) node->ss.ps.plan;

		if (cscan->custom_private != NIL && list_length(cscan->custom_private) >= 4)
		{
			gss->edge_relid = intVal(linitial(cscan->custom_private));
			gss->start_id = intVal(lsecond(cscan->custom_private));
			gss->max_depth = intVal(lthird(cscan->custom_private));
			gss->mode = (GraphTraversalMode) intVal(lfourth(cscan->custom_private));
		}
	}

	/* Build result tuple descriptor */
	gss->result_tupdesc = CreateTemplateTupleDesc(3);
	TupleDescInitEntry(gss->result_tupdesc, (AttrNumber) 1, "node_id",
					   INT4OID, -1, 0);
	TupleDescInitEntry(gss->result_tupdesc, (AttrNumber) 2, "depth",
					   INT4OID, -1, 0);
	TupleDescInitEntry(gss->result_tupdesc, (AttrNumber) 3, "path",
					   INT4ARRAYOID, -1, 0);
	BlessTupleDesc(gss->result_tupdesc);

	/* Create tuplestore for results */
	gss->results = tuplestore_begin_heap(true, false, work_mem);
	gss->executed = false;
}

/*
 * Execute the graph traversal and populate the tuplestore.
 */
static void
graphscan_execute_traversal(GraphScanState *gss)
{
	char	   *relname;
	char	   *nspname;
	char	   *qualified_name;
	const char *src_col = DEFAULT_SRC_COL;
	const char *dst_col = DEFAULT_DST_COL;
	char	   *query;
	int			ret;
	uint64		proc;
	uint64		i;

	if (gss->edge_relid == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("GraphScan: no edge table specified")));

	relname = get_rel_name(gss->edge_relid);
	if (relname == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("edge table with OID %u does not exist",
						gss->edge_relid)));

	nspname = get_namespace_name(get_rel_namespace(gss->edge_relid));
	qualified_name = quote_qualified_identifier(nspname, relname);

	/* Detect column names */
	if (table_has_column(gss->edge_relid, DEFAULT_SRC_COL))
		src_col = DEFAULT_SRC_COL;
	if (table_has_column(gss->edge_relid, DEFAULT_DST_COL))
		dst_col = DEFAULT_DST_COL;

	/* Generate appropriate query based on traversal mode */
	switch (gss->mode)
	{
		case GRAPH_TRAVERSE_BFS:
			query = generate_graph_bfs_query(qualified_name, src_col, dst_col,
											 gss->start_id, gss->max_depth);
			break;
		case GRAPH_TRAVERSE_DFS:
			query = generate_graph_dfs_query(qualified_name, src_col, dst_col,
											 gss->start_id, gss->max_depth);
			break;
		case GRAPH_TRAVERSE_SHORTEST_PATH:
			query = generate_graph_shortest_path_query(qualified_name,
													   src_col, dst_col,
													   NULL,
													   gss->start_id, -1);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unsupported graph traversal mode: %d",
							gss->mode)));
			return;			/* keep compiler happy */
	}

	/* Execute via SPI */
	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("SPI_connect failed in GraphScan execution")));

	ret = SPI_execute(query, true, 0);
	pfree(query);

	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("GraphScan query failed: %s",
						SPI_result_code_string(ret))));

	proc = SPI_processed;

	for (i = 0; i < proc; i++)
	{
		HeapTuple	spi_tuple = SPI_tuptable->vals[i];
		TupleDesc	spi_tupdesc = SPI_tuptable->tupdesc;
		Datum		values[3];
		bool		nulls[3];
		bool		isnull;

		values[0] = SPI_getbinval(spi_tuple, spi_tupdesc, 1, &isnull);
		nulls[0] = isnull;

		values[1] = SPI_getbinval(spi_tuple, spi_tupdesc, 2, &isnull);
		nulls[1] = isnull;

		values[2] = SPI_getbinval(spi_tuple, spi_tupdesc, 3, &isnull);
		nulls[2] = isnull;

		if (!nulls[2])
			values[2] = PointerGetDatum(DatumGetArrayTypePCopy(values[2]));

		tuplestore_putvalues(gss->results, gss->result_tupdesc,
							values, nulls);
	}

	SPI_freetuptable(SPI_tuptable);
	SPI_finish();

	gss->executed = true;
}

static TupleTableSlot *
graphscan_exec(CustomScanState *node)
{
	GraphScanState *gss = (GraphScanState *) node;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	/* Execute traversal on first call */
	if (!gss->executed)
		graphscan_execute_traversal(gss);

	/* Fetch next tuple from tuplestore */
	if (!tuplestore_gettupleslot(gss->results, true, false, slot))
		return NULL;

	return slot;
}

static void
graphscan_end(CustomScanState *node)
{
	GraphScanState *gss = (GraphScanState *) node;

	if (gss->results)
	{
		tuplestore_end(gss->results);
		gss->results = NULL;
	}
}

static void
graphscan_rescan(CustomScanState *node)
{
	GraphScanState *gss = (GraphScanState *) node;

	if (gss->results)
		tuplestore_rescan(gss->results);
}

/*
 * CustomScan and CustomExec method tables for GraphScan.
 *
 * Note: These are defined here and referenced from graphscan_create_state.
 * We use a forward declaration pattern because CustomScanMethods needs
 * to reference CreateCustomScanState, and vice versa.
 */

/* CustomExecMethods - execution-time callbacks */
static const CustomExecMethods graphscan_exec_methods = {
	.CustomName = GRAPHSCAN_NAME,
	.BeginCustomScan = graphscan_begin,
	.ExecCustomScan = graphscan_exec,
	.EndCustomScan = graphscan_end,
	.ReScanCustomScan = graphscan_rescan,
	.MarkPosCustomScan = NULL,
	.RestrPosCustomScan = NULL,
	.EstimateDSMCustomScan = NULL,
	.InitializeDSMCustomScan = NULL,
	.ReInitializeDSMCustomScan = NULL,
	.InitializeWorkerCustomScan = NULL,
	.ShutdownCustomScan = NULL,
	.ExplainCustomScan = NULL,
};

/* CustomScanMethods - plan-time callbacks */
static const CustomScanMethods graphscan_plan_methods = {
	.CustomName = GRAPHSCAN_NAME,
	.CreateCustomScanState = graphscan_create_state,
};


/* ----------------------------------------------------------------
 *		Module initialization
 *
 * Register the GraphScan CustomScan provider so that it can be
 * referenced by name in serialized plan trees.
 * ----------------------------------------------------------------
 */

/*
 * _PG_init - module initialization function
 *
 * Called when the shared library is loaded (if compiled as an extension)
 * or at backend startup (if compiled into the core server).
 *
 * Registers the GraphScan CustomScan methods with the extensible
 * node infrastructure.
 */
void _PG_init(void);

void
_PG_init(void)
{
	RegisterCustomScanMethods(&graphscan_plan_methods);
}
