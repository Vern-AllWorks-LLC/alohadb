/*-------------------------------------------------------------------------
 *
 * alohadb_timeseries.c
 *	  Main entry point for the alohadb_timeseries extension.
 *
 *	  Implements auto-partition management for time-series tables.
 *	  A background worker periodically scans alohadb_timeseries_config
 *	  and creates future partitions (premake) or drops expired ones
 *	  (retention) using standard PostgreSQL declarative partitioning.
 *
 *	  Uses standard PG declarative partitioning only.
 *	  pg_partman (PostgreSQL License, 2014) is prior art.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_timeseries/alohadb_timeseries.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

/* Background worker essentials */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/latch.h"

/* SPI and transaction management */
#include "access/xact.h"
#include "catalog/namespace.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#include "alohadb_timeseries.h"

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_timeseries",
					.version = "1.0"
);

/* SQL-callable function declarations */
PG_FUNCTION_INFO_V1(alohadb_timeseries_manage);
PG_FUNCTION_INFO_V1(alohadb_timeseries_unmanage);
PG_FUNCTION_INFO_V1(alohadb_time_bucket_timestamptz);
PG_FUNCTION_INFO_V1(alohadb_time_bucket_timestamp);
PG_FUNCTION_INFO_V1(alohadb_timeseries_status);
PG_FUNCTION_INFO_V1(alohadb_timeseries_maintain_now);

/* ----------------------------------------------------------------
 * GUC variables
 * ---------------------------------------------------------------- */

/* Check interval in seconds; default 60 (1 minute) */
static int	timeseries_check_interval = DEFAULT_CHECK_INTERVAL_S;

/* Database to connect to */
static char *timeseries_database = NULL;

/* ----------------------------------------------------------------
 * Backend-local status tracking
 * ---------------------------------------------------------------- */

static TimeseriesStatus managed_status[MAX_MANAGED];
static int	num_managed_status = 0;

/* Custom wait event for WaitLatch */
static uint32 timeseries_wait_event = 0;

/* ----------------------------------------------------------------
 * Forward declarations
 * ---------------------------------------------------------------- */

static void timeseries_perform_maintenance(bool manage_xacts);
static void timeseries_create_partitions(Oid parent_oid,
										 const char *parent_nspname,
										 const char *parent_relname,
										 const char *partition_column,
										 int64 interval_secs,
										 int premake_count,
										 bool manage_xacts);
static void timeseries_drop_expired_partitions(Oid parent_oid,
											   const char *parent_nspname,
											   const char *parent_relname,
											   int64 interval_secs,
											   int64 retention_secs,
											   bool manage_xacts);
static int64 interval_to_seconds(const char *interval_str);
static void format_partition_suffix(char *buf, size_t buflen,
									int64 interval_secs,
									TimestampTz ts);
static TimestampTz align_to_interval(TimestampTz ts, int64 interval_secs);
static TimeseriesStatus *timeseries_find_or_create_status(Oid table_oid,
														  const char *table_name);

/* ----------------------------------------------------------------
 * interval_to_seconds
 *
 * Convert an interval string to total seconds via SPI.
 * Must be called inside an SPI session.
 * ---------------------------------------------------------------- */
static int64
interval_to_seconds(const char *interval_str)
{
	StringInfoData buf;
	int			ret;
	bool		isnull;
	Datum		val;
	float8		secs;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT EXTRACT(EPOCH FROM %s::interval)::float8",
					 quote_literal_cstr(interval_str));

	ret = SPI_execute(buf.data, true, 1);
	pfree(buf.data);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_timeseries: failed to convert interval \"%s\" to seconds",
						interval_str)));

	val = SPI_getbinval(SPI_tuptable->vals[0],
						SPI_tuptable->tupdesc, 1, &isnull);
	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_timeseries: NULL result converting interval \"%s\"",
						interval_str)));

	secs = DatumGetFloat8(val);
	return (int64) secs;
}

/* ----------------------------------------------------------------
 * align_to_interval
 *
 * Align a timestamp down to the nearest interval boundary.
 * Uses Unix epoch as the alignment base.
 * ---------------------------------------------------------------- */
static TimestampTz
align_to_interval(TimestampTz ts, int64 interval_secs)
{
	int64		epoch_usec;
	int64		interval_usec;
	int64		aligned_usec;

	/* Convert PG timestamp to microseconds since Unix epoch */
	epoch_usec = ts + ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY);
	interval_usec = interval_secs * USECS_PER_SEC;

	if (interval_usec <= 0)
		return ts;

	/* Floor-align */
	if (epoch_usec >= 0)
		aligned_usec = (epoch_usec / interval_usec) * interval_usec;
	else
		aligned_usec = ((epoch_usec - interval_usec + 1) / interval_usec) * interval_usec;

	/* Convert back to PG timestamp */
	return aligned_usec - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY);
}

/* ----------------------------------------------------------------
 * format_partition_suffix
 *
 * Generate a partition name suffix from a timestamp.
 * For intervals >= 1 day: YYYYMMDD
 * For intervals < 1 day:  YYYYMMDD_HHMM
 * ---------------------------------------------------------------- */
static void
format_partition_suffix(char *buf, size_t buflen, int64 interval_secs,
						TimestampTz ts)
{
	struct pg_tm tm;
	fsec_t		fsec;
	int			tz;

	if (timestamp2tm(ts, &tz, &tm, &fsec, NULL, NULL) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("alohadb_timeseries: could not convert timestamp")));

	if (interval_secs >= 86400)
	{
		snprintf(buf, buflen, "%04d%02d%02d",
				 tm.tm_year, tm.tm_mon, tm.tm_mday);
	}
	else
	{
		snprintf(buf, buflen, "%04d%02d%02d_%02d%02d",
				 tm.tm_year, tm.tm_mon, tm.tm_mday,
				 tm.tm_hour, tm.tm_min);
	}
}

/* ----------------------------------------------------------------
 * timeseries_create_partitions
 *
 * For a given parent table, ensure partitions exist from the
 * current interval through premake_count intervals into the
 * future.  Also ensures the current interval partition exists.
 * ---------------------------------------------------------------- */
static void
timeseries_create_partitions(Oid parent_oid,
							 const char *parent_nspname,
							 const char *parent_relname,
							 const char *partition_column,
							 int64 interval_secs,
							 int premake_count,
							 bool manage_xacts)
{
	TimestampTz now_ts;
	TimestampTz current_boundary;
	int64		interval_usec;
	int			i;
	int			ret;
	StringInfoData query;
	char		qualified_parent[NAMEDATALEN * 2 + 2];

	/* parent_oid and partition_column reserved for future use */
	(void) parent_oid;
	(void) partition_column;

	now_ts = GetCurrentTimestamp();
	interval_usec = interval_secs * USECS_PER_SEC;

	/* Align now to the start of its interval bucket */
	current_boundary = align_to_interval(now_ts, interval_secs);

	/* Build schema-qualified parent name */
	snprintf(qualified_parent, sizeof(qualified_parent), "%s.%s",
			 quote_identifier(parent_nspname),
			 quote_identifier(parent_relname));

	initStringInfo(&query);

	/*
	 * Create partitions from 1 interval before current through
	 * premake_count intervals ahead.  This ensures the current
	 * partition always exists.
	 */
	for (i = -1; i <= premake_count; i++)
	{
		TimestampTz lower_bound;
		TimestampTz upper_bound;
		char		lower_ts_str[MAXDATELEN + 1];
		char		upper_ts_str[MAXDATELEN + 1];
		char		suffix[64];
		char		part_name[NAMEDATALEN * 2 + 64];

		lower_bound = current_boundary + (int64) i * interval_usec;
		upper_bound = lower_bound + interval_usec;

		/* Format timestamps as ISO strings for SQL */
		snprintf(lower_ts_str, sizeof(lower_ts_str), "%s",
				 timestamptz_to_str(lower_bound));
		snprintf(upper_ts_str, sizeof(upper_ts_str), "%s",
				 timestamptz_to_str(upper_bound));

		/* Generate partition suffix */
		format_partition_suffix(suffix, sizeof(suffix),
								interval_secs, lower_bound);

		/* Build partition name: <schema>.<parent>_p<suffix> */
		snprintf(part_name, sizeof(part_name), "%s.%s_p%s",
				 quote_identifier(parent_nspname),
				 quote_identifier(parent_relname),
				 suffix);

		resetStringInfo(&query);
		appendStringInfo(&query,
						 "CREATE TABLE IF NOT EXISTS %s "
						 "PARTITION OF %s "
						 "FOR VALUES FROM ('%s') TO ('%s')",
						 part_name,
						 qualified_parent,
						 lower_ts_str,
						 upper_ts_str);

		if (manage_xacts)
		{
			SetCurrentStatementStartTimestamp();
			StartTransactionCommand();
			SPI_connect();
			PushActiveSnapshot(GetTransactionSnapshot());
			pgstat_report_activity(STATE_RUNNING,
								   "alohadb_timeseries: creating partition");
		}

		PG_TRY();
		{
			ret = SPI_execute(query.data, false, 0);
			if (ret != SPI_OK_UTILITY)
				elog(WARNING, "alohadb_timeseries: failed to create partition %s: error code %d",
					 part_name, ret);

			if (manage_xacts)
			{
				PopActiveSnapshot();
				SPI_finish();
				CommitTransactionCommand();
			}
		}
		PG_CATCH();
		{
			/* Partition may already exist with different bounds; skip it */
			if (manage_xacts)
			{
				SPI_finish();
				PopActiveSnapshot();
				AbortCurrentTransaction();
			}
			FlushErrorState();
			elog(DEBUG1, "alohadb_timeseries: partition creation skipped for %s (may already exist with different bounds)",
				 part_name);
		}
		PG_END_TRY();

		if (manage_xacts)
			pgstat_report_activity(STATE_IDLE, NULL);
	}

	pfree(query.data);
}

/* ----------------------------------------------------------------
 * timeseries_drop_expired_partitions
 *
 * For a given parent table, find and drop partitions whose upper
 * boundary is older than now() - retention_interval.
 * ---------------------------------------------------------------- */
static void
timeseries_drop_expired_partitions(Oid parent_oid,
								   const char *parent_nspname,
								   const char *parent_relname,
								   int64 interval_secs,
								   int64 retention_secs,
								   bool manage_xacts)
{
	StringInfoData query;
	int			ret;
	uint64		nparts;
	uint64		j;
	char		qualified_parent[NAMEDATALEN * 2 + 2];

	/* interval_secs reserved for future use (e.g., batch retention) */
	(void) interval_secs;

	snprintf(qualified_parent, sizeof(qualified_parent), "%s.%s",
			 quote_identifier(parent_nspname),
			 quote_identifier(parent_relname));

	/*
	 * Find child partitions whose upper range bound is older than
	 * the retention threshold.
	 */
	initStringInfo(&query);
	appendStringInfo(&query,
		"SELECT c.oid::regclass::text AS partition_name "
		"FROM pg_catalog.pg_inherits i "
		"JOIN pg_catalog.pg_class c ON c.oid = i.inhrelid "
		"WHERE i.inhparent = %u "
		"  AND c.relispartition "
		"  AND pg_catalog.pg_get_expr(c.relpartbound, c.oid) LIKE 'FOR VALUES FROM%%' "
		"  AND ("
		"    SELECT (regexp_match(pg_catalog.pg_get_expr(c.relpartbound, c.oid), "
		"            'TO \\(''([^'']+)''\\)'))[1]::timestamptz"
		"  ) <= (current_timestamp - make_interval(secs => " INT64_FORMAT "))",
		parent_oid,
		retention_secs);

	if (manage_xacts)
	{
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());
		pgstat_report_activity(STATE_RUNNING,
							   "alohadb_timeseries: checking expired partitions");
	}

	ret = SPI_execute(query.data, true, 0);

	if (ret != SPI_OK_SELECT)
	{
		elog(WARNING, "alohadb_timeseries: failed to query expired partitions for %s: error code %d",
			 qualified_parent, ret);
		if (manage_xacts)
		{
			PopActiveSnapshot();
			SPI_finish();
			CommitTransactionCommand();
		}
		pfree(query.data);
		return;
	}

	nparts = SPI_processed;

	if (nparts == 0)
	{
		if (manage_xacts)
		{
			PopActiveSnapshot();
			SPI_finish();
			CommitTransactionCommand();
		}
		pfree(query.data);
		return;
	}

	/*
	 * Copy partition names out of SPI memory before we run DROP
	 * commands that will overwrite the SPI result set.
	 */
	{
		char	  **part_names;
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(CurTransactionContext);
		part_names = palloc(sizeof(char *) * nparts);
		for (j = 0; j < nparts; j++)
		{
			char   *val;

			val = SPI_getvalue(SPI_tuptable->vals[j],
							   SPI_tuptable->tupdesc, 1);
			part_names[j] = pstrdup(val ? val : "");
		}
		MemoryContextSwitchTo(oldcxt);

		if (manage_xacts)
		{
			PopActiveSnapshot();
			SPI_finish();
			CommitTransactionCommand();
		}

		/*
		 * Drop each expired partition.
		 * In bgworker mode, each DROP runs in its own transaction.
		 * In SQL mode, they all run in the caller's transaction.
		 */
		for (j = 0; j < nparts; j++)
		{
			StringInfoData drop_cmd;

			initStringInfo(&drop_cmd);
			appendStringInfo(&drop_cmd,
							 "DROP TABLE %s",
							 part_names[j]);

			if (manage_xacts)
			{
				SetCurrentStatementStartTimestamp();
				StartTransactionCommand();
				SPI_connect();
				PushActiveSnapshot(GetTransactionSnapshot());
				pgstat_report_activity(STATE_RUNNING, drop_cmd.data);
			}

			ret = SPI_execute(drop_cmd.data, false, 0);

			if (ret == SPI_OK_UTILITY)
				elog(LOG, "alohadb_timeseries: dropped expired partition %s",
					 part_names[j]);
			else
				elog(WARNING, "alohadb_timeseries: failed to drop partition %s: error code %d",
					 part_names[j], ret);

			if (manage_xacts)
			{
				PopActiveSnapshot();
				SPI_finish();
				CommitTransactionCommand();
				pgstat_report_activity(STATE_IDLE, NULL);
			}

			pfree(drop_cmd.data);
		}
	}

	pfree(query.data);
}

/* ----------------------------------------------------------------
 * timeseries_perform_maintenance
 *
 * Execute one full maintenance pass: scan all enabled entries in
 * alohadb_timeseries_config, create future partitions, and drop
 * expired ones.  Called from the bgworker main loop and also from
 * the alohadb_timeseries_maintain_now() SQL function.
 * ---------------------------------------------------------------- */
static void
timeseries_perform_maintenance(bool manage_xacts)
{
	int			ret;
	uint64		nconfigs;
	uint64		i;

	if (manage_xacts)
	{
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());
		pgstat_report_activity(STATE_RUNNING,
							   "alohadb_timeseries: scanning configuration");
	}

	/*
	 * Check whether the config table exists.  It won't be there until
	 * CREATE EXTENSION alohadb_timeseries has been run in this database.
	 */
	ret = SPI_execute(
		"SELECT 1 FROM pg_catalog.pg_class c "
		"JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
		"WHERE n.nspname = 'public' AND c.relname = 'alohadb_timeseries_config'",
		true, 1);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
	{
		if (manage_xacts)
		{
			PopActiveSnapshot();
			SPI_finish();
			CommitTransactionCommand();
			pgstat_report_activity(STATE_IDLE, NULL);
		}
		return;
	}

	/*
	 * Fetch all enabled configurations.
	 */
	ret = SPI_execute(
		"SELECT table_name::oid, partition_column, "
		"       partition_interval::text, retention_interval::text, "
		"       premake_count "
		"FROM public.alohadb_timeseries_config "
		"WHERE enabled = true "
		"ORDER BY table_name::oid",
		true, 0);

	if (ret != SPI_OK_SELECT)
	{
		elog(WARNING, "alohadb_timeseries: failed to read config table: error code %d",
			 ret);
		if (manage_xacts)
		{
			PopActiveSnapshot();
			SPI_finish();
			CommitTransactionCommand();
			pgstat_report_activity(STATE_IDLE, NULL);
		}
		return;
	}

	nconfigs = SPI_processed;

	if (nconfigs > 0)
	{
		typedef struct ConfigCopy
		{
			Oid			table_oid;
			char		partition_column[NAMEDATALEN];
			char		partition_interval[64];
			char		retention_interval[64];
			bool		has_retention;
			int			premake_count;
		} ConfigCopy;

		ConfigCopy *configs;
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(CurTransactionContext);
		configs = palloc(sizeof(ConfigCopy) * nconfigs);
		MemoryContextSwitchTo(oldcxt);

		for (i = 0; i < nconfigs; i++)
		{
			bool	isnull;
			char   *val;

			configs[i].table_oid = DatumGetObjectId(
				SPI_getbinval(SPI_tuptable->vals[i],
							  SPI_tuptable->tupdesc, 1, &isnull));

			val = SPI_getvalue(SPI_tuptable->vals[i],
							   SPI_tuptable->tupdesc, 2);
			strlcpy(configs[i].partition_column, val ? val : "", NAMEDATALEN);

			val = SPI_getvalue(SPI_tuptable->vals[i],
							   SPI_tuptable->tupdesc, 3);
			strlcpy(configs[i].partition_interval, val ? val : "", 64);

			val = SPI_getvalue(SPI_tuptable->vals[i],
							   SPI_tuptable->tupdesc, 4);
			if (val != NULL)
			{
				strlcpy(configs[i].retention_interval, val, 64);
				configs[i].has_retention = true;
			}
			else
			{
				configs[i].retention_interval[0] = '\0';
				configs[i].has_retention = false;
			}

			configs[i].premake_count = DatumGetInt32(
				SPI_getbinval(SPI_tuptable->vals[i],
							  SPI_tuptable->tupdesc, 5, &isnull));
			if (isnull)
				configs[i].premake_count = DEFAULT_PREMAKE;
		}

		if (manage_xacts)
		{
			PopActiveSnapshot();
			SPI_finish();
			CommitTransactionCommand();
		}

		/*
		 * Process each config entry.  For interval_to_seconds we need
		 * SPI; in bgworker mode we wrap each conversion in its own
		 * transaction, in SQL mode we reuse the caller's.
		 */
		for (i = 0; i < nconfigs; i++)
		{
			char	   *rel_name;
			char	   *nsp_name;
			Oid			nsp_oid;
			int64		interval_secs;
			int64		retention_secs = 0;
			TimeseriesStatus *st;

			rel_name = get_rel_name(configs[i].table_oid);
			if (rel_name == NULL)
			{
				elog(WARNING, "alohadb_timeseries: table OID %u no longer exists, skipping",
					 configs[i].table_oid);
				continue;
			}

			nsp_oid = get_rel_namespace(configs[i].table_oid);
			nsp_name = get_namespace_name(nsp_oid);
			if (nsp_name == NULL)
				nsp_name = "public";

			/* Convert interval strings to seconds */
			if (manage_xacts)
			{
				SetCurrentStatementStartTimestamp();
				StartTransactionCommand();
				SPI_connect();
				PushActiveSnapshot(GetTransactionSnapshot());
			}

			interval_secs = interval_to_seconds(configs[i].partition_interval);
			if (configs[i].has_retention)
				retention_secs = interval_to_seconds(configs[i].retention_interval);

			if (manage_xacts)
			{
				PopActiveSnapshot();
				SPI_finish();
				CommitTransactionCommand();
			}

			if (interval_secs <= 0)
			{
				elog(WARNING, "alohadb_timeseries: invalid partition_interval for %s.%s",
					 nsp_name, rel_name);
				continue;
			}

			/* Create future partitions */
			timeseries_create_partitions(configs[i].table_oid,
										 nsp_name,
										 rel_name,
										 configs[i].partition_column,
										 interval_secs,
										 configs[i].premake_count,
										 manage_xacts);

			/* Drop expired partitions if retention is configured */
			if (configs[i].has_retention && retention_secs > 0)
			{
				timeseries_drop_expired_partitions(configs[i].table_oid,
												   nsp_name,
												   rel_name,
												   interval_secs,
												   retention_secs,
												   manage_xacts);
			}

			/* Update status tracking */
			st = timeseries_find_or_create_status(configs[i].table_oid,
												  rel_name);
			st->last_maintenance = GetCurrentTimestamp();
		}
	}
	else
	{
		if (manage_xacts)
		{
			PopActiveSnapshot();
			SPI_finish();
			CommitTransactionCommand();
		}
	}

	if (manage_xacts)
		pgstat_report_activity(STATE_IDLE, NULL);
}

/* ----------------------------------------------------------------
 * timeseries_find_or_create_status
 *
 * Find or allocate a status tracking entry for a given table OID.
 * ---------------------------------------------------------------- */
static TimeseriesStatus *
timeseries_find_or_create_status(Oid table_oid, const char *table_name)
{
	int		i;

	for (i = 0; i < num_managed_status; i++)
	{
		if (managed_status[i].table_oid == table_oid)
			return &managed_status[i];
	}

	if (num_managed_status >= MAX_MANAGED)
	{
		elog(WARNING, "alohadb_timeseries: status tracking table full, reusing slot 0");
		num_managed_status = 0;
	}

	i = num_managed_status++;
	memset(&managed_status[i], 0, sizeof(TimeseriesStatus));
	managed_status[i].table_oid = table_oid;
	strlcpy(managed_status[i].table_name,
			table_name ? table_name : "",
			NAMEDATALEN);
	return &managed_status[i];
}

/* ----------------------------------------------------------------
 * Background worker entry point
 * ---------------------------------------------------------------- */
void
alohadb_timeseries_worker_main(Datum main_arg)
{
	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to the configured database */
	BackgroundWorkerInitializeConnection(timeseries_database, NULL, 0);

	elog(LOG, "alohadb_timeseries: background worker started, "
		 "check interval = %d s, database = \"%s\"",
		 timeseries_check_interval,
		 timeseries_database ? timeseries_database : "postgres");

	/*
	 * Main loop: periodically run maintenance until SIGTERM is
	 * received and processed by ProcessInterrupts (via die/CHECK_FOR_INTERRUPTS).
	 */
	for (;;)
	{
		/* Allocate or fetch the custom wait event on first use. */
		if (timeseries_wait_event == 0)
			timeseries_wait_event = WaitEventExtensionNew("AlohadbTimeseriesMain");

		/*
		 * Sleep for the configured interval, waking on latch set
		 * (SIGHUP, SIGTERM) or postmaster death.
		 */
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 timeseries_check_interval * 1000L,
						 timeseries_wait_event);
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		/* Reload configuration on SIGHUP. */
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
			elog(LOG, "alohadb_timeseries: configuration reloaded, "
				 "check interval = %d s", timeseries_check_interval);
		}

		/* Perform the actual maintenance. */
		timeseries_perform_maintenance(true);

		pgstat_report_stat(true);
	}

	/* Not reachable */
}

/* ----------------------------------------------------------------
 * _PG_init
 *
 * Module load callback.  Registers GUC variables and, if loaded
 * via shared_preload_libraries, registers the background worker.
 * ---------------------------------------------------------------- */
void
_PG_init(void)
{
	BackgroundWorker worker;

	/*
	 * Define GUC: alohadb.timeseries_check_interval
	 */
	DefineCustomIntVariable("alohadb.timeseries_check_interval",
							"Interval between partition maintenance checks (in seconds).",
							"Set to 0 to disable periodic checks; "
							"the worker will sleep indefinitely until signaled.",
							&timeseries_check_interval,
							DEFAULT_CHECK_INTERVAL_S,
							0,
							INT_MAX / 1000,
							PGC_SIGHUP,
							GUC_UNIT_S,
							NULL,
							NULL,
							NULL);

	DefineCustomStringVariable("alohadb.timeseries_database",
							   "Database the timeseries background worker connects to.",
							   NULL,
							   &timeseries_database,
							   "postgres",
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	if (!process_shared_preload_libraries_in_progress)
		return;

	MarkGUCPrefixReserved("alohadb.timeseries");

	/* Register the background worker. */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
	snprintf(worker.bgw_library_name, MAXPGPATH, "alohadb_timeseries");
	snprintf(worker.bgw_function_name, BGW_MAXLEN,
			 "alohadb_timeseries_worker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "alohadb_timeseries worker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "alohadb_timeseries");
	worker.bgw_main_arg = (Datum) 0;
	worker.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&worker);
}

/* ----------------------------------------------------------------
 * alohadb_timeseries_manage
 *
 * SQL-callable function to register a partitioned table for
 * automatic partition management.
 *
 * Args: table_name regclass, partition_column text,
 *       partition_interval interval, retention_interval interval
 * ---------------------------------------------------------------- */
Datum
alohadb_timeseries_manage(PG_FUNCTION_ARGS)
{
	Oid			table_oid = PG_GETARG_OID(0);
	text	   *partition_col_text = PG_GETARG_TEXT_PP(1);
	Interval   *partition_interval = PG_GETARG_INTERVAL_P(2);
	bool		has_retention = !PG_ARGISNULL(3);
	Interval   *retention_interval = has_retention ? PG_GETARG_INTERVAL_P(3) : NULL;
	char	   *partition_col;
	StringInfoData query;
	int			ret;

	partition_col = text_to_cstring(partition_col_text);

	/* Validate that the table is partitioned */
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	{
		StringInfoData check_query;

		initStringInfo(&check_query);
		appendStringInfo(&check_query,
						 "SELECT 1 FROM pg_catalog.pg_partitioned_table "
						 "WHERE partrelid = %u",
						 table_oid);

		ret = SPI_execute(check_query.data, true, 1);
		pfree(check_query.data);

		if (ret != SPI_OK_SELECT || SPI_processed == 0)
		{
			PopActiveSnapshot();
			SPI_finish();
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("table with OID %u is not a partitioned table",
							table_oid),
					 errhint("Use CREATE TABLE ... PARTITION BY RANGE (...) first.")));
		}
	}

	/* Insert into config table */
	initStringInfo(&query);

	if (has_retention)
	{
		char	   *part_interval_str;
		char	   *ret_interval_str;

		part_interval_str = DatumGetCString(
			DirectFunctionCall1(interval_out,
								IntervalPGetDatum(partition_interval)));
		ret_interval_str = DatumGetCString(
			DirectFunctionCall1(interval_out,
								IntervalPGetDatum(retention_interval)));

		appendStringInfo(&query,
						 "INSERT INTO public.alohadb_timeseries_config "
						 "(table_name, partition_column, partition_interval, "
						 " retention_interval) "
						 "VALUES (%u, %s, %s::interval, %s::interval) "
						 "ON CONFLICT (table_name) DO UPDATE SET "
						 "partition_column = EXCLUDED.partition_column, "
						 "partition_interval = EXCLUDED.partition_interval, "
						 "retention_interval = EXCLUDED.retention_interval, "
						 "enabled = true",
						 table_oid,
						 quote_literal_cstr(partition_col),
						 quote_literal_cstr(part_interval_str),
						 quote_literal_cstr(ret_interval_str));
	}
	else
	{
		char	   *part_interval_str;

		part_interval_str = DatumGetCString(
			DirectFunctionCall1(interval_out,
								IntervalPGetDatum(partition_interval)));

		appendStringInfo(&query,
						 "INSERT INTO public.alohadb_timeseries_config "
						 "(table_name, partition_column, partition_interval) "
						 "VALUES (%u, %s, %s::interval) "
						 "ON CONFLICT (table_name) DO UPDATE SET "
						 "partition_column = EXCLUDED.partition_column, "
						 "partition_interval = EXCLUDED.partition_interval, "
						 "retention_interval = NULL, "
						 "enabled = true",
						 table_oid,
						 quote_literal_cstr(partition_col),
						 quote_literal_cstr(part_interval_str));
	}

	ret = SPI_execute(query.data, false, 0);
	if (ret != SPI_OK_INSERT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_timeseries: failed to insert config: error code %d",
						ret)));

	/*
	 * Run immediate partition creation within the same SPI session
	 * so we don't interfere with the caller's transaction.
	 */
	{
		char	   *rel_name;
		char	   *nsp_name;
		Oid			nsp_oid;
		int64		interval_secs;
		int64		interval_usec;
		char	   *part_interval_str;
		TimestampTz now_ts;
		TimestampTz current_boundary;
		int			i;
		char		qualified_parent[NAMEDATALEN * 2 + 2];

		rel_name = get_rel_name(table_oid);
		nsp_oid = get_rel_namespace(table_oid);
		nsp_name = get_namespace_name(nsp_oid);
		if (nsp_name == NULL)
			nsp_name = "public";

		part_interval_str = DatumGetCString(
			DirectFunctionCall1(interval_out,
								IntervalPGetDatum(partition_interval)));

		interval_secs = interval_to_seconds(part_interval_str);

		if (interval_secs > 0)
		{
			now_ts = GetCurrentTimestamp();
			interval_usec = interval_secs * USECS_PER_SEC;
			current_boundary = align_to_interval(now_ts, interval_secs);

			snprintf(qualified_parent, sizeof(qualified_parent), "%s.%s",
					 quote_identifier(nsp_name),
					 quote_identifier(rel_name));

			for (i = -1; i <= DEFAULT_PREMAKE; i++)
			{
				TimestampTz		lower_bound;
				TimestampTz		upper_bound;
				char			lower_ts_str[MAXDATELEN + 1];
				char			upper_ts_str[MAXDATELEN + 1];
				char			suffix[64];
				char			part_name[NAMEDATALEN * 2 + 64];
				StringInfoData	create_cmd;

				lower_bound = current_boundary + (int64) i * interval_usec;
				upper_bound = lower_bound + interval_usec;

				snprintf(lower_ts_str, sizeof(lower_ts_str), "%s",
						 timestamptz_to_str(lower_bound));
				snprintf(upper_ts_str, sizeof(upper_ts_str), "%s",
						 timestamptz_to_str(upper_bound));

				format_partition_suffix(suffix, sizeof(suffix),
										interval_secs, lower_bound);

				snprintf(part_name, sizeof(part_name), "%s.%s_p%s",
						 quote_identifier(nsp_name),
						 quote_identifier(rel_name),
						 suffix);

				initStringInfo(&create_cmd);
				appendStringInfo(&create_cmd,
								 "CREATE TABLE IF NOT EXISTS %s "
								 "PARTITION OF %s "
								 "FOR VALUES FROM ('%s') TO ('%s')",
								 part_name,
								 qualified_parent,
								 lower_ts_str,
								 upper_ts_str);

				/*
				 * Use PG_TRY so that a partition that already exists
				 * with different bounds does not abort the whole
				 * registration.
				 */
				PG_TRY();
				{
					ret = SPI_execute(create_cmd.data, false, 0);
					if (ret != SPI_OK_UTILITY)
						elog(WARNING, "alohadb_timeseries: failed to create partition %s: error code %d",
							 part_name, ret);
				}
				PG_CATCH();
				{
					FlushErrorState();
					elog(DEBUG1, "alohadb_timeseries: partition creation skipped for %s (may already exist with different bounds)",
						 part_name);
				}
				PG_END_TRY();

				pfree(create_cmd.data);
			}
		}
	}

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * alohadb_timeseries_unmanage
 *
 * SQL-callable function to remove a table from automatic
 * partition management.
 *
 * Args: table_name regclass
 * ---------------------------------------------------------------- */
Datum
alohadb_timeseries_unmanage(PG_FUNCTION_ARGS)
{
	Oid			table_oid = PG_GETARG_OID(0);
	StringInfoData query;
	int			ret;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	initStringInfo(&query);
	appendStringInfo(&query,
					 "DELETE FROM public.alohadb_timeseries_config "
					 "WHERE table_name = %u",
					 table_oid);

	ret = SPI_execute(query.data, false, 0);
	if (ret != SPI_OK_DELETE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_timeseries: failed to delete config: error code %d",
						ret)));

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * alohadb_time_bucket_timestamptz
 *
 * Pure C computation: truncate a timestamptz to the start of its
 * time bucket.  No SPI.
 *
 * Args: bucket_width interval, ts timestamptz
 * Returns: timestamptz
 * ---------------------------------------------------------------- */
Datum
alohadb_time_bucket_timestamptz(PG_FUNCTION_ARGS)
{
	Interval   *bucket_width = PG_GETARG_INTERVAL_P(0);
	TimestampTz ts = PG_GETARG_TIMESTAMPTZ(1);
	int64		interval_usec;
	int64		epoch_usec;
	int64		bucket_usec;

	/*
	 * Convert interval to microseconds.  For simple intervals
	 * (days/seconds only), this is straightforward.  Months are
	 * approximated at 30 days.
	 */
	interval_usec = bucket_width->month * (int64) 30 * USECS_PER_DAY
				  + bucket_width->day * USECS_PER_DAY
				  + bucket_width->time;

	if (interval_usec <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("bucket width must be a positive interval")));

	/* Convert PG timestamptz to microseconds since Unix epoch */
	epoch_usec = ts + ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY);

	/* Floor-align to bucket boundary */
	if (epoch_usec >= 0)
		bucket_usec = (epoch_usec / interval_usec) * interval_usec;
	else
		bucket_usec = ((epoch_usec - interval_usec + 1) / interval_usec) * interval_usec;

	/* Convert back to PG timestamptz */
	PG_RETURN_TIMESTAMPTZ(bucket_usec - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY));
}

/* ----------------------------------------------------------------
 * alohadb_time_bucket_timestamp
 *
 * Overload for timestamp without time zone.  Same logic as the
 * timestamptz variant.
 *
 * Args: bucket_width interval, ts timestamp
 * Returns: timestamp
 * ---------------------------------------------------------------- */
Datum
alohadb_time_bucket_timestamp(PG_FUNCTION_ARGS)
{
	Interval   *bucket_width = PG_GETARG_INTERVAL_P(0);
	Timestamp	ts = PG_GETARG_TIMESTAMP(1);
	int64		interval_usec;
	int64		epoch_usec;
	int64		bucket_usec;

	/*
	 * Convert interval to microseconds.  Months approximated at 30 days.
	 */
	interval_usec = bucket_width->month * (int64) 30 * USECS_PER_DAY
				  + bucket_width->day * USECS_PER_DAY
				  + bucket_width->time;

	if (interval_usec <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("bucket width must be a positive interval")));

	/* Convert PG timestamp to microseconds since Unix epoch */
	epoch_usec = ts + ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY);

	/* Floor-align to bucket boundary */
	if (epoch_usec >= 0)
		bucket_usec = (epoch_usec / interval_usec) * interval_usec;
	else
		bucket_usec = ((epoch_usec - interval_usec + 1) / interval_usec) * interval_usec;

	/* Convert back to PG timestamp */
	PG_RETURN_TIMESTAMP(bucket_usec - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY));
}

/* ----------------------------------------------------------------
 * alohadb_timeseries_status
 *
 * SQL-callable function returning one row per managed table with
 * partition metadata.  Uses InitMaterializedSRF.
 * ---------------------------------------------------------------- */
Datum
alohadb_timeseries_status(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			ret;
	uint64		i;

	InitMaterializedSRF(fcinfo, 0);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(
		"SELECT c.table_name::regclass::text, "
		"       c.partition_column, "
		"       c.partition_interval, "
		"       c.retention_interval, "
		"       (SELECT count(*) "
		"        FROM pg_catalog.pg_inherits i "
		"        WHERE i.inhparent = c.table_name)::int AS partition_count, "
		"       c.enabled "
		"FROM public.alohadb_timeseries_config c "
		"ORDER BY c.table_name::regclass::text",
		true, 0);

	if (ret != SPI_OK_SELECT)
	{
		PopActiveSnapshot();
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_timeseries: failed to query status: error code %d",
						ret)));
	}

	for (i = 0; i < SPI_processed; i++)
	{
		Datum		values[6];
		bool		nulls[6];
		int			j;

		for (j = 0; j < 6; j++)
		{
			Form_pg_attribute attr = TupleDescAttr(SPI_tuptable->tupdesc, j);

			values[j] = SPI_getbinval(SPI_tuptable->vals[i],
									   SPI_tuptable->tupdesc,
									   j + 1, &nulls[j]);

			/* Copy datums out of SPI context */
			if (!nulls[j])
				values[j] = SPI_datumTransfer(values[j],
											   attr->attbyval,
											   attr->attlen);
		}

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	PopActiveSnapshot();
	SPI_finish();

	return (Datum) 0;
}

/* ----------------------------------------------------------------
 * alohadb_timeseries_maintain_now
 *
 * SQL-callable function to trigger an immediate maintenance pass
 * from the current session (not the background worker).
 * ---------------------------------------------------------------- */
Datum
alohadb_timeseries_maintain_now(PG_FUNCTION_ARGS)
{
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	timeseries_perform_maintenance(false);

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_VOID();
}
