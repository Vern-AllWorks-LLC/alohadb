/*-------------------------------------------------------------------------
 *
 * pg_gql.c
 *	  Main entry point for the pg_gql extension.
 *
 *	  This file contains only the PG_MODULE_MAGIC_EXT declaration.
 *	  All SQL-callable functions live in graphql_resolver.c alongside
 *	  their PG_FUNCTION_INFO_V1 declarations, and the parser lives
 *	  in graphql_parser.c.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_gql/pg_gql.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"

#include "pg_gql.h"

PG_MODULE_MAGIC_EXT(
					.name = "pg_gql",
					.version = "1.0"
);
