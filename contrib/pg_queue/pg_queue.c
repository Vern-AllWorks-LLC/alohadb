/*-------------------------------------------------------------------------
 *
 * pg_queue.c
 *	  Main entry point for the pg_queue extension.
 *
 *	  This file contains only the module magic block.  All SQL-callable
 *	  function implementations live in queue_ops.c alongside their
 *	  PG_FUNCTION_INFO_V1 declarations.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_queue/pg_queue.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"

#include "pg_queue.h"

PG_MODULE_MAGIC_EXT(
					.name = "pg_queue",
					.version = "1.0"
);
