/*-------------------------------------------------------------------------
 *
 * alohadb_tiering.h
 *	  Shared declarations for the alohadb_tiering extension.
 *
 *	  Provides automatic movement of cold data partitions to cheaper
 *	  storage based on time-based rules.  A background worker
 *	  periodically evaluates user-defined rules and issues
 *	  ALTER TABLE ... SET TABLESPACE to relocate partitions whose
 *	  boundary falls outside the configured age threshold.
 *
 *	  Time-based only -- NO access-frequency heat maps.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * contrib/alohadb_tiering/alohadb_tiering.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef ALOHADB_TIERING_H
#define ALOHADB_TIERING_H

#include "postgres.h"
#include "fmgr.h"
#include "utils/timestamp.h"

/*
 * Name of the rules table created by the extension SQL script.
 */
#define TIERING_RULES_TABLE		"alohadb_tiering_rules"

/*
 * Default check interval in seconds (1 hour).
 */
#define TIERING_DEFAULT_INTERVAL_S	3600

/*
 * Maximum length of tablespace name stored in the rules table.
 */
#define TIERING_TABLESPACE_MAXLEN	NAMEDATALEN

/*
 * Per-rule status, tracked in backend-local memory across iterations.
 */
typedef struct TieringRuleStatus
{
	int			rule_id;
	char		parent_table[NAMEDATALEN];
	int			partitions_moved;
	TimestampTz last_check;
} TieringRuleStatus;

/*
 * Maximum number of rules we track status for.
 */
#define TIERING_MAX_RULES		256

/* Background worker entry point (exported for postmaster) */
extern PGDLLEXPORT void alohadb_tiering_worker_main(Datum main_arg);

/* SQL-callable functions */
extern Datum alohadb_tiering_status(PG_FUNCTION_ARGS);
extern Datum alohadb_tiering_check_now(PG_FUNCTION_ARGS);

#endif							/* ALOHADB_TIERING_H */
