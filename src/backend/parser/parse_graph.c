/*-------------------------------------------------------------------------
 *
 * parse_graph.c
 *	  SQL/PGQ (ISO 9075-16) graph pattern matching - parser rewrite module
 *
 * This module transforms GRAPH_TABLE pattern matching specifications into
 * equivalent WITH RECURSIVE (recursive CTE) queries.  This is the
 * "syntactic sugar" approach: the graph traversal semantics are expressed
 * entirely in standard SQL, allowing the existing optimizer and executor
 * to handle them without any new plan node types.
 *
 * The generated queries use CYCLE detection via path-array tracking
 * (the standard PostgreSQL idiom for recursive CTEs), ensuring correct
 * termination even on cyclic graphs.
 *
 * Three traversal modes are supported:
 *   - BFS (Breadth-First Search): level-order traversal with depth tracking
 *   - DFS (Depth-First Search): depth-first traversal with path tracking
 *   - Shortest Path (Dijkstra-like): cost-weighted shortest path via CTE
 *
 * Copyright (c) 2025, AlohaDB Project
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/parser/parse_graph.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_graph.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

/*
 * Maximum length of generated SQL query strings.
 *
 * Graph traversal queries with recursive CTEs can grow, particularly for
 * shortest-path queries which include additional subqueries.  We use a
 * StringInfo buffer internally, so this is just a sanity check.
 */
#define MAX_GRAPH_QUERY_LEN		8192

/*
 * Default maximum depth for traversals to prevent runaway recursion.
 * This can be overridden per-call.
 */
#define DEFAULT_MAX_GRAPH_DEPTH	100


/*
 * generate_graph_bfs_query
 *
 * Generate a WITH RECURSIVE query that performs breadth-first traversal
 * of a graph stored in an edge table.
 *
 * The generated query has this structure:
 *
 *   WITH RECURSIVE graph_traversal(node_id, depth, path) AS (
 *       -- Base case: start at the given node
 *       SELECT <start_id>, 0, ARRAY[<start_id>]
 *     UNION ALL
 *       -- Recursive step: follow edges, avoiding cycles
 *       SELECT e.<dst_col>, gt.depth + 1, gt.path || e.<dst_col>
 *       FROM graph_traversal gt
 *       JOIN <edge_table> e ON e.<src_col> = gt.node_id
 *       WHERE NOT (e.<dst_col> = ANY(gt.path))
 *         AND gt.depth < <max_depth>
 *   )
 *   SELECT node_id, depth, path FROM graph_traversal
 *   ORDER BY depth, node_id
 *
 * BFS ordering is achieved by ORDER BY depth (level-order).
 */
char *
generate_graph_bfs_query(const char *edge_table,
						 const char *src_col,
						 const char *dst_col,
						 int start_id,
						 int max_depth)
{
	StringInfoData buf;
	int			effective_max_depth;

	Assert(edge_table != NULL);
	Assert(src_col != NULL);
	Assert(dst_col != NULL);

	effective_max_depth = (max_depth > 0) ? max_depth : DEFAULT_MAX_GRAPH_DEPTH;

	initStringInfo(&buf);

	appendStringInfo(&buf,
					 "WITH RECURSIVE graph_traversal(node_id, depth, path) AS ("
					 " SELECT %d, 0, ARRAY[%d]"
					 " UNION ALL"
					 " SELECT e.%s, gt.depth + 1, gt.path || e.%s"
					 " FROM graph_traversal gt"
					 " JOIN %s e ON e.%s = gt.node_id"
					 " WHERE NOT (e.%s = ANY(gt.path))"
					 " AND gt.depth < %d"
					 ")"
					 " SELECT node_id, depth, path"
					 " FROM graph_traversal"
					 " ORDER BY depth, node_id",
					 start_id,
					 start_id,
					 quote_identifier(dst_col),
					 quote_identifier(dst_col),
					 edge_table,
					 quote_identifier(src_col),
					 quote_identifier(dst_col),
					 effective_max_depth);

	return buf.data;
}


/*
 * generate_graph_dfs_query
 *
 * Generate a WITH RECURSIVE query that performs depth-first traversal
 * of a graph stored in an edge table.
 *
 * DFS ordering is achieved by using UNION ALL with a recursive query
 * that explores depth-first.  The ORDER BY uses path (lexicographic
 * ordering of the path array gives DFS order when edges are explored
 * in node_id order).
 *
 * The generated query has this structure:
 *
 *   WITH RECURSIVE graph_traversal(node_id, depth, path) AS (
 *       SELECT <start_id>, 0, ARRAY[<start_id>]
 *     UNION ALL
 *       SELECT e.<dst_col>, gt.depth + 1, gt.path || e.<dst_col>
 *       FROM graph_traversal gt
 *       JOIN <edge_table> e ON e.<src_col> = gt.node_id
 *       WHERE NOT (e.<dst_col> = ANY(gt.path))
 *         AND gt.depth < <max_depth>
 *   )
 *   SELECT node_id, depth, path FROM graph_traversal
 *   ORDER BY path
 */
char *
generate_graph_dfs_query(const char *edge_table,
						 const char *src_col,
						 const char *dst_col,
						 int start_id,
						 int max_depth)
{
	StringInfoData buf;
	int			effective_max_depth;

	Assert(edge_table != NULL);
	Assert(src_col != NULL);
	Assert(dst_col != NULL);

	effective_max_depth = (max_depth > 0) ? max_depth : DEFAULT_MAX_GRAPH_DEPTH;

	initStringInfo(&buf);

	appendStringInfo(&buf,
					 "WITH RECURSIVE graph_traversal(node_id, depth, path) AS ("
					 " SELECT %d, 0, ARRAY[%d]"
					 " UNION ALL"
					 " SELECT e.%s, gt.depth + 1, gt.path || e.%s"
					 " FROM graph_traversal gt"
					 " JOIN %s e ON e.%s = gt.node_id"
					 " WHERE NOT (e.%s = ANY(gt.path))"
					 " AND gt.depth < %d"
					 ")"
					 " SELECT node_id, depth, path"
					 " FROM graph_traversal"
					 " ORDER BY path",
					 start_id,
					 start_id,
					 quote_identifier(dst_col),
					 quote_identifier(dst_col),
					 edge_table,
					 quote_identifier(src_col),
					 quote_identifier(dst_col),
					 effective_max_depth);

	return buf.data;
}


/*
 * generate_graph_shortest_path_query
 *
 * Generate a WITH RECURSIVE query that computes shortest paths using
 * a Dijkstra-like approach via recursive CTE.
 *
 * For weighted graphs, the query accumulates edge weights along each
 * path and uses a final filtering step to select only the shortest
 * path to each node (or to the specific target node).
 *
 * The generated query structure:
 *
 *   WITH RECURSIVE graph_shortest(node_id, total_cost, path) AS (
 *       SELECT <start_id>, 0.0::float8, ARRAY[<start_id>]
 *     UNION ALL
 *       SELECT e.<dst_col>,
 *              gs.total_cost + e.<weight_col>,
 *              gs.path || e.<dst_col>
 *       FROM graph_shortest gs
 *       JOIN <edge_table> e ON e.<src_col> = gs.node_id
 *       WHERE NOT (e.<dst_col> = ANY(gs.path))
 *   ),
 *   best_costs AS (
 *       SELECT node_id,
 *              MIN(total_cost) AS total_cost
 *       FROM graph_shortest
 *       GROUP BY node_id
 *   )
 *   SELECT gs.node_id, gs.total_cost, gs.path
 *   FROM graph_shortest gs
 *   JOIN best_costs bc ON gs.node_id = bc.node_id
 *                     AND gs.total_cost = bc.total_cost
 *   [WHERE gs.node_id = <end_id>]
 *   ORDER BY gs.total_cost
 *
 * Note: This is a CTE-based approximation of Dijkstra.  It explores all
 * paths (with cycle prevention) and then selects the minimum-cost ones.
 * For true Dijkstra performance, use the nodeGraphScan CustomScan executor.
 */
char *
generate_graph_shortest_path_query(const char *edge_table,
								   const char *src_col,
								   const char *dst_col,
								   const char *weight_col,
								   int start_id,
								   int end_id)
{
	StringInfoData buf;
	const char *weight_expr;

	Assert(edge_table != NULL);
	Assert(src_col != NULL);
	Assert(dst_col != NULL);

	/* If no weight column specified, use uniform weight of 1.0 */
	if (weight_col != NULL)
		weight_expr = quote_identifier(weight_col);
	else
		weight_expr = "1.0";

	initStringInfo(&buf);

	appendStringInfo(&buf,
					 "WITH RECURSIVE graph_shortest(node_id, total_cost, path) AS ("
					 " SELECT %d, 0.0::float8, ARRAY[%d]"
					 " UNION ALL"
					 " SELECT e.%s,"
					 " gs.total_cost + e.%s,"
					 " gs.path || e.%s"
					 " FROM graph_shortest gs"
					 " JOIN %s e ON e.%s = gs.node_id"
					 " WHERE NOT (e.%s = ANY(gs.path))"
					 "),"
					 " best_costs AS ("
					 " SELECT node_id, MIN(total_cost) AS total_cost"
					 " FROM graph_shortest"
					 " GROUP BY node_id"
					 ")"
					 " SELECT gs.node_id, gs.total_cost, gs.path"
					 " FROM graph_shortest gs"
					 " JOIN best_costs bc ON gs.node_id = bc.node_id"
					 " AND gs.total_cost = bc.total_cost",
					 start_id,
					 start_id,
					 quote_identifier(dst_col),
					 weight_expr,
					 quote_identifier(dst_col),
					 edge_table,
					 quote_identifier(src_col),
					 quote_identifier(dst_col));

	/*
	 * If a specific target node is requested, filter to just that node.
	 * Otherwise, return shortest paths to all reachable nodes.
	 */
	if (end_id >= 0)
		appendStringInfo(&buf, " WHERE gs.node_id = %d", end_id);

	appendStringInfoString(&buf, " ORDER BY gs.total_cost");

	return buf.data;
}
