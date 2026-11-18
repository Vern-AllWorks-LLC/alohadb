/*-------------------------------------------------------------------------
 *
 * pg_tsmanage.h
 *	  Shared declarations for the pg_tsmanage extension.
 *
 *	  Provides auto-partition management for time-series tables.
 *	  A background worker periodically creates future partitions
 *	  and drops expired ones based on user-defined configuration
 *	  stored in pg_tsmanage_config.
 *
 *	  Uses standard PostgreSQL declarative partitioning only.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * contrib/pg_tsmanage/pg_tsmanage.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef ALOHADB_TIMESERIES_H
#define ALOHADB_TIMESERIES_H

#include "postgres.h"
#include "fmgr.h"
#include "utils/timestamp.h"

/*
 * Default check interval in seconds (1 minute).
 */
#define DEFAULT_CHECK_INTERVAL_S	60

/*
 * Default number of future partitions to pre-create.
 */
#define DEFAULT_PREMAKE				3

/*
 * Maximum number of managed tables we track status for.
 */
#define MAX_MANAGED					256

/*
 * Per-table status, tracked in backend-local memory across iterations.
 */
typedef struct TimeseriesStatus
{
	Oid			table_oid;
	char		table_name[NAMEDATALEN];
	int			partitions_created;
	int			partitions_dropped;
	TimestampTz last_maintenance;
} TimeseriesStatus;

/* Background worker entry point (exported for postmaster) */
extern PGDLLEXPORT void pg_tsmanage_worker_main(Datum main_arg);

/* SQL-callable functions */
extern Datum pg_tsmanage_manage(PG_FUNCTION_ARGS);
extern Datum pg_tsmanage_unmanage(PG_FUNCTION_ARGS);
extern Datum pg_time_bucket_timestamptz(PG_FUNCTION_ARGS);
extern Datum pg_time_bucket_timestamp(PG_FUNCTION_ARGS);
extern Datum pg_tsmanage_status(PG_FUNCTION_ARGS);
extern Datum pg_tsmanage_maintain_now(PG_FUNCTION_ARGS);

#endif							/* ALOHADB_TIMESERIES_H */
