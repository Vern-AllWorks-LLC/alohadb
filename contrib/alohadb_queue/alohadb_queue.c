/*-------------------------------------------------------------------------
 *
 * alohadb_queue.c
 *	  Main entry point for the alohadb_queue extension.
 *
 *	  This file contains only the module magic block.  All SQL-callable
 *	  function implementations live in queue_ops.c alongside their
 *	  PG_FUNCTION_INFO_V1 declarations.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_queue/alohadb_queue.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"

#include "alohadb_queue.h"

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_queue",
					.version = "1.0"
);
