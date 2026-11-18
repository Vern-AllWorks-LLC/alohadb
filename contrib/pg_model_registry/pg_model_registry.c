/*-------------------------------------------------------------------------
 *
 * pg_model_registry.c
 *	  Module registration for the pg_model_registry extension.
 *
 *	  All business logic is implemented in pure plpgsql/SQL in
 *	  pg_model_registry--1.0.sql.  This C file exists only to
 *	  satisfy the PostgreSQL requirement for a MODULE_MAGIC declaration.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_model_registry/pg_model_registry.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"

PG_MODULE_MAGIC_EXT(
					.name = "pg_model_registry",
					.version = "1.0"
);
