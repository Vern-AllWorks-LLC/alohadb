/*-------------------------------------------------------------------------
 *
 * pg_temporal.h
 *	  Shared declarations for the pg_temporal extension.
 *
 *	  Provides SQL:2011-style system-versioned temporal tables with
 *	  automatic history tracking and time-travel queries.  Uses the
 *	  trigger+history table approach (MariaDB GPL prior art).
 *
 *	  Oracle Flashback patent expired March 2024.
 *	  SQL:2011 temporal tables are an ISO standard.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * contrib/pg_temporal/pg_temporal.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef ALOHADB_TEMPORAL_H
#define ALOHADB_TEMPORAL_H

#include "postgres.h"
#include "fmgr.h"

/*
 * Column names added to versioned tables.
 */
#define TEMPORAL_ROW_START		"row_start"
#define TEMPORAL_ROW_END		"row_end"

/*
 * Suffix appended to the base table name to form the history table name.
 */
#define TEMPORAL_HISTORY_SUFFIX	"_history"

/*
 * The sentinel value for row_end on current (non-historical) rows.
 */
#define TEMPORAL_INFINITY		"infinity"

/* SQL-callable functions */
extern Datum pg_temporal_versioning_trigger(PG_FUNCTION_ARGS);
extern Datum pg_enable_system_versioning(PG_FUNCTION_ARGS);
extern Datum pg_disable_system_versioning(PG_FUNCTION_ARGS);
extern Datum pg_as_of(PG_FUNCTION_ARGS);
extern Datum pg_versions_between(PG_FUNCTION_ARGS);
extern Datum pg_temporal_status(PG_FUNCTION_ARGS);

#endif							/* ALOHADB_TEMPORAL_H */
