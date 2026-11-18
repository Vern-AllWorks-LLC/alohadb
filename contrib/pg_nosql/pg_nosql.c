/*-------------------------------------------------------------------------
 *
 * pg_nosql.c
 *	  Main entry point for the pg_nosql extension.
 *
 *	  Implements a document database API over PostgreSQL JSONB.
 *	  Collections are ordinary tables with (_id text PK, data jsonb).
 *	  All query optimization is delegated to PostgreSQL's native
 *	  planner via GIN indexes and jsonb_path_ops.
 *
 *	  This extension uses its own terminology and is NOT a
 *	  MongoDB-compatible layer.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_nosql/pg_nosql.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"

#include "pg_nosql.h"

PG_MODULE_MAGIC_EXT(
					.name = "pg_nosql",
					.version = "1.0"
);

/*
 * PG_FUNCTION_INFO_V1 declarations are in each respective .c file
 * alongside the function implementations, so that both the pg_finfo_*
 * and the function symbol are exported from the same translation unit.
 */
