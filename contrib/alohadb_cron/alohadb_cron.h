/*-------------------------------------------------------------------------
 *
 * alohadb_cron.h
 *	  Shared declarations for the alohadb_cron extension.
 *
 *	  Provides job scheduling with standard cron syntax.  A background
 *	  worker periodically evaluates registered cron jobs and executes
 *	  their SQL commands when the schedule matches.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * contrib/alohadb_cron/alohadb_cron.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef ALOHADB_CRON_H
#define ALOHADB_CRON_H

#include "postgres.h"
#include "fmgr.h"
#include "utils/timestamp.h"

/*
 * Default check interval in seconds (60 = 1 minute).
 */
#define CRON_DEFAULT_CHECK_INTERVAL		60

/*
 * Maximum iteration limit when computing the next matching time
 * from a cron schedule.  2 years in minutes = 2 * 366 * 24 * 60.
 */
#define CRON_MAX_ITERATIONS				(2 * 366 * 24 * 60)

/*
 * CronSchedule
 *
 * Bitmask arrays indicating which values are valid for each
 * cron field.  Using bool arrays for simplicity and clarity.
 *
 * Fields:
 *   minute[60]  - minutes 0..59
 *   hour[24]    - hours 0..23
 *   dom[31]     - days of month 1..31 (index 0 = day 1)
 *   month[12]   - months 1..12 (index 0 = month 1)
 *   dow[7]      - days of week 0..6 (0 = Sunday)
 */
typedef struct CronSchedule
{
	bool		minute[60];
	bool		hour[24];
	bool		dom[31];		/* index 0 = day 1 */
	bool		month[12];		/* index 0 = month 1 */
	bool		dow[7];			/* index 0 = Sunday */
} CronSchedule;

/* cron_parser.c */
extern bool cron_parse(const char *expr, CronSchedule *sched);
extern TimestampTz cron_next_run(const CronSchedule *sched, TimestampTz from);

/* Background worker entry point (exported for postmaster) */
extern PGDLLEXPORT void alohadb_cron_worker_main(Datum main_arg);

/* SQL-callable functions */
extern Datum cron_schedule(PG_FUNCTION_ARGS);
extern Datum cron_schedule_named(PG_FUNCTION_ARGS);
extern Datum cron_unschedule(PG_FUNCTION_ARGS);
extern Datum cron_unschedule_named(PG_FUNCTION_ARGS);
extern Datum cron_job_status(PG_FUNCTION_ARGS);

#endif							/* ALOHADB_CRON_H */
