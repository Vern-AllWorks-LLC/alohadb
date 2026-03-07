/*-------------------------------------------------------------------------
 *
 * alohadb_graphql.h
 *	  Shared declarations for the alohadb_graphql extension.
 *
 *	  Provides an auto-generated GraphQL API from the database schema.
 *	  Includes a recursive-descent GraphQL parser, schema introspection
 *	  via information_schema, and a resolver that translates GraphQL
 *	  queries and mutations into SQL executed through SPI.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * contrib/alohadb_graphql/alohadb_graphql.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef ALOHADB_GRAPHQL_H
#define ALOHADB_GRAPHQL_H

#include "postgres.h"
#include "fmgr.h"

/* ----------------------------------------------------------------
 * GraphQL AST node types
 * ---------------------------------------------------------------- */

typedef enum GqlNodeType
{
	GQL_QUERY,
	GQL_MUTATION,
	GQL_FIELD,
	GQL_ARGUMENT
} GqlNodeType;

/*
 * GqlNode -- a node in the parsed GraphQL AST.
 *
 * For GQL_QUERY / GQL_MUTATION:
 *   name     = operation name (may be NULL for anonymous)
 *   children = linked list of top-level field selections
 *
 * For GQL_FIELD:
 *   name     = field (table/column) name
 *   args     = linked list of GQL_ARGUMENT nodes
 *   children = linked list of sub-field selections (nested objects)
 *   next     = next sibling field in same selection set
 *
 * For GQL_ARGUMENT:
 *   name     = argument name (e.g. "where", "limit", "offset", "order_by",
 *              "objects")
 *   value    = argument value as raw string (may be JSON-ish for objects/arrays)
 *   next     = next sibling argument
 */
typedef struct GqlNode
{
	GqlNodeType type;
	char	   *name;		/* field / table / argument name */
	struct GqlNode *args;	/* linked list of arguments */
	struct GqlNode *children;	/* linked list of child fields */
	struct GqlNode *next;	/* next sibling */
	char	   *value;		/* for arguments: string value */
} GqlNode;

/* ----------------------------------------------------------------
 * Maximum sizes
 * ---------------------------------------------------------------- */
#define GQL_MAX_QUERY_LEN		(1024 * 1024)	/* 1 MB */
#define GQL_MAX_NAME_LEN		256
#define GQL_MAX_VALUE_LEN		(64 * 1024)		/* 64 KB for arg values */
#define GQL_MAX_SQL_LEN			(256 * 1024)	/* 256 KB generated SQL */

/* ----------------------------------------------------------------
 * Parser API  (graphql_parser.c)
 * ---------------------------------------------------------------- */

/*
 * Parse a GraphQL query string into an AST.  Returns the root node
 * (GQL_QUERY or GQL_MUTATION).  Raises ereport(ERROR) on syntax error.
 *
 * Memory is allocated in CurrentMemoryContext.
 */
extern GqlNode *gql_parse(const char *query);

/*
 * Free an entire AST tree recursively.  Safe to call with NULL.
 */
extern void gql_free(GqlNode *node);

/* ----------------------------------------------------------------
 * Resolver API  (graphql_resolver.c)
 * ---------------------------------------------------------------- */

/* SQL-callable functions -- PG_FUNCTION_INFO_V1 in graphql_resolver.c */
extern Datum graphql_execute(PG_FUNCTION_ARGS);
extern Datum graphql_schema(PG_FUNCTION_ARGS);
extern Datum graphql_schema_json(PG_FUNCTION_ARGS);

#endif							/* ALOHADB_GRAPHQL_H */
