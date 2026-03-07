/*-------------------------------------------------------------------------
 *
 * parse_graph.h
 *	  Declarations for SQL/PGQ (ISO 9075-16) graph pattern matching
 *	  support in the parser.
 *
 * This module provides GRAPH_TABLE rewrite support, transforming graph
 * pattern matching queries into equivalent recursive CTE queries.
 *
 * Copyright (c) 2025, AlohaDB Project
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/include/parser/parse_graph.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_GRAPH_H
#define PARSE_GRAPH_H

#include "nodes/parsenodes.h"
#include "parser/parse_node.h"

/*
 * GraphTraversalMode - enumeration of supported traversal strategies
 */
typedef enum GraphTraversalMode
{
	GRAPH_TRAVERSE_BFS,			/* Breadth-first search */
	GRAPH_TRAVERSE_DFS,			/* Depth-first search */
	GRAPH_TRAVERSE_SHORTEST_PATH	/* Dijkstra shortest path */
} GraphTraversalMode;

/*
 * GraphPatternMatch - internal representation of a parsed GRAPH_TABLE
 * pattern (used as an intermediate form before rewriting to CTE).
 */
typedef struct GraphPatternMatch
{
	NodeTag		type;
	char	   *edge_table;		/* name of edge relation */
	char	   *src_alias;		/* alias for source vertex */
	char	   *edge_alias;		/* alias for edge */
	char	   *dst_alias;		/* alias for destination vertex */
	Node	   *where_clause;	/* filter on source vertex */
	List	   *columns;		/* output column list */
	GraphTraversalMode mode;	/* BFS, DFS, or shortest path */
	int			max_depth;		/* maximum traversal depth (0 = unlimited) */
} GraphPatternMatch;

/*
 * generate_graph_bfs_query - Generate a WITH RECURSIVE query string for
 * BFS graph traversal.
 *
 * Parameters:
 *   edge_table  - fully qualified name of the edge table
 *   src_col     - name of the source column in the edge table
 *   dst_col     - name of the destination column in the edge table
 *   start_id    - starting node ID
 *   max_depth   - maximum traversal depth (0 = unlimited)
 *
 * Returns a palloc'd string containing the SQL query.
 */
extern char *generate_graph_bfs_query(const char *edge_table,
									  const char *src_col,
									  const char *dst_col,
									  int start_id,
									  int max_depth);

/*
 * generate_graph_shortest_path_query - Generate a WITH RECURSIVE query
 * string for shortest-path (Dijkstra) graph traversal.
 *
 * Parameters:
 *   edge_table  - fully qualified name of the edge table
 *   src_col     - name of the source column in the edge table
 *   dst_col     - name of the destination column in the edge table
 *   weight_col  - name of the weight/cost column (NULL for unweighted)
 *   start_id    - starting node ID
 *   end_id      - target node ID
 *
 * Returns a palloc'd string containing the SQL query.
 */
extern char *generate_graph_shortest_path_query(const char *edge_table,
												const char *src_col,
												const char *dst_col,
												const char *weight_col,
												int start_id,
												int end_id);

/*
 * generate_graph_dfs_query - Generate a WITH RECURSIVE query string for
 * DFS graph traversal.
 *
 * Parameters:
 *   edge_table  - fully qualified name of the edge table
 *   src_col     - name of the source column in the edge table
 *   dst_col     - name of the destination column in the edge table
 *   start_id    - starting node ID
 *   max_depth   - maximum traversal depth (0 = unlimited)
 *
 * Returns a palloc'd string containing the SQL query.
 */
extern char *generate_graph_dfs_query(const char *edge_table,
									  const char *src_col,
									  const char *dst_col,
									  int start_id,
									  int max_depth);

#endif							/* PARSE_GRAPH_H */
