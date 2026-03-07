/*-------------------------------------------------------------------------
 *
 * alohadb_schema_change.h
 *	  Header for the alohadb_schema_change extension.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_schema_change/alohadb_schema_change.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ALOHADB_SCHEMA_CHANGE_H
#define ALOHADB_SCHEMA_CHANGE_H

#include "postgres.h"
#include "fmgr.h"

#define SCHEMA_CHANGE_TABLE "alohadb_schema_changes"

extern Datum online_alter_table(PG_FUNCTION_ARGS);
extern Datum online_alter_status(PG_FUNCTION_ARGS);
extern Datum online_alter_cancel(PG_FUNCTION_ARGS);

#endif							/* ALOHADB_SCHEMA_CHANGE_H */
