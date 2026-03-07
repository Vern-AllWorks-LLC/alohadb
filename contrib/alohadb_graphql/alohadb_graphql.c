/*-------------------------------------------------------------------------
 *
 * alohadb_graphql.c
 *	  Main entry point for the alohadb_graphql extension.
 *
 *	  This file contains only the PG_MODULE_MAGIC_EXT declaration.
 *	  All SQL-callable functions live in graphql_resolver.c alongside
 *	  their PG_FUNCTION_INFO_V1 declarations, and the parser lives
 *	  in graphql_parser.c.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_graphql/alohadb_graphql.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"

#include "alohadb_graphql.h"

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_graphql",
					.version = "1.0"
);
