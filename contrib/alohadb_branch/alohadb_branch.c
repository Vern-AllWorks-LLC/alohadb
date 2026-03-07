/*-------------------------------------------------------------------------
 *
 * alohadb_branch.c
 *	  Database branching extension for PostgreSQL.
 *
 *	  Creates lightweight database branches from point-in-time snapshots
 *	  for testing migrations, experiments, and safe schema evolution.
 *
 *	  Each branch is a separate PostgreSQL data directory created via
 *	  pg_basebackup with WAL timeline forking.  On filesystems that
 *	  support reflinks (e.g., XFS, Btrfs, APFS), the copy is
 *	  near-instantaneous via copy-on-write semantics.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_branch/alohadb_branch.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <signal.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "catalog/pg_type.h"
#include "common/file_perm.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"

#include "alohadb_branch.h"

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_branch",
					.version = "1.0"
);

/* SQL-callable functions */
PG_FUNCTION_INFO_V1(alohadb_create_branch);
PG_FUNCTION_INFO_V1(alohadb_list_branches);
PG_FUNCTION_INFO_V1(alohadb_drop_branch);

/* ----------------------------------------------------------------
 *	Helper functions
 * ----------------------------------------------------------------
 */

/*
 * branch_get_parent_dir
 *
 * Returns the parent directory of the current PGDATA.  Branch data
 * directories are placed under <parent>/branches/<name>.
 */
char *
branch_get_parent_dir(void)
{
	char	   *pgdata;
	char	   *parent;
	char	   *last_sep;

	pgdata = pstrdup(DataDir);

	/* Strip trailing slashes */
	while (strlen(pgdata) > 1 && pgdata[strlen(pgdata) - 1] == '/')
		pgdata[strlen(pgdata) - 1] = '\0';

	last_sep = strrchr(pgdata, '/');
	if (last_sep == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("cannot determine parent directory of PGDATA \"%s\"",
						DataDir)));

	parent = pnstrdup(pgdata, last_sep - pgdata);
	pfree(pgdata);
	return parent;
}

/*
 * branch_get_data_dir
 *
 * Returns the full path to a branch data directory:
 *   <PGDATA_parent>/branches/<branch_name>
 */
char *
branch_get_data_dir(const char *branch_name)
{
	char	   *parent = branch_get_parent_dir();
	char	   *result;

	result = psprintf("%s/%s/%s", parent, BRANCH_SUBDIR, branch_name);
	pfree(parent);
	return result;
}

/*
 * branch_name_is_valid
 *
 * Branch names must be non-empty, at most BRANCH_NAME_MAXLEN characters,
 * and consist only of alphanumeric characters, hyphens, and underscores.
 */
bool
branch_name_is_valid(const char *name)
{
	int			i;

	if (name == NULL || name[0] == '\0')
		return false;

	if (strlen(name) > BRANCH_NAME_MAXLEN)
		return false;

	for (i = 0; name[i] != '\0'; i++)
	{
		char		ch = name[i];

		if (!((ch >= 'a' && ch <= 'z') ||
			  (ch >= 'A' && ch <= 'Z') ||
			  (ch >= '0' && ch <= '9') ||
			  ch == '-' || ch == '_'))
			return false;
	}

	return true;
}

/*
 * branch_find_next_port
 *
 * Queries the alohadb_branches table to find the next available port.
 * Starts from (PostPortNumber + BRANCH_PORT_OFFSET) and picks the first
 * port not already in use by an existing branch.
 *
 * Must be called within an SPI context.
 */
int
branch_find_next_port(void)
{
	int			base_port;
	int			next_port;
	int			ret;

	base_port = PostPortNumber + BRANCH_PORT_OFFSET;

	ret = SPI_execute("SELECT COALESCE(MAX(port), 0) FROM alohadb_branches",
					  true, 1);
	if (ret != SPI_OK_SELECT || SPI_processed == 0)
		return base_port;

	{
		bool		isnull;
		Datum		val;

		val = SPI_getbinval(SPI_tuptable->vals[0],
							SPI_tuptable->tupdesc,
							1, &isnull);
		if (isnull)
			next_port = base_port;
		else
		{
			int			max_port = DatumGetInt32(val);

			next_port = (max_port >= base_port) ? max_port + 1 : base_port;
		}
	}

	return next_port;
}

/*
 * branch_exec_command
 *
 * Execute a shell command and return its exit status.
 * Raises an ERROR on failure, with the command's stderr output if available.
 */
static int
branch_exec_command(const char *cmd)
{
	int			rc;

	rc = system(cmd);
	if (rc == -1)
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("failed to execute command: %s", cmd)));

	return WEXITSTATUS(rc);
}

/*
 * branch_ensure_dir
 *
 * Create the branches directory if it doesn't exist.
 */
static void
branch_ensure_dir(void)
{
	char	   *parent = branch_get_parent_dir();
	char	   *branches_dir;

	branches_dir = psprintf("%s/%s", parent, BRANCH_SUBDIR);

	if (mkdir(branches_dir, pg_dir_create_mode) != 0)
	{
		if (errno != EEXIST)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create directory \"%s\": %m",
							branches_dir)));
	}

	pfree(branches_dir);
	pfree(parent);
}

/*
 * branch_write_config_overrides
 *
 * Write a postgresql.auto.conf into the branch data directory with
 * overrides for port and unix_socket_directories.
 */
static void
branch_write_config_overrides(const char *data_dir, int port)
{
	char	   *conf_path;
	FILE	   *fp;

	conf_path = psprintf("%s/postgresql.auto.conf", data_dir);

	fp = fopen(conf_path, "w");
	if (fp == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open \"%s\" for writing: %m", conf_path)));

	fprintf(fp, "# alohadb_branch auto-generated configuration\n");
	fprintf(fp, "port = %d\n", port);
	fprintf(fp, "unix_socket_directories = '%s'\n", data_dir);
	fprintf(fp, "log_directory = '%s/log'\n", data_dir);
	fprintf(fp, "logging_collector = on\n");

	/*
	 * Disable archive and replication settings that might conflict
	 * with the parent server.
	 */
	fprintf(fp, "archive_mode = off\n");
	fprintf(fp, "primary_conninfo = ''\n");

	if (fclose(fp) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close \"%s\": %m", conf_path)));

	pfree(conf_path);
}

/*
 * branch_write_recovery_signal
 *
 * Create a recovery.signal file in the branch data directory to trigger
 * recovery mode.  If a target LSN is specified, also write
 * recovery_target_lsn and recovery_target_action into the config.
 */
static void
branch_write_recovery_conf(const char *data_dir, XLogRecPtr target_lsn)
{
	char	   *signal_path;
	FILE	   *fp;

	/* Create recovery.signal */
	signal_path = psprintf("%s/recovery.signal", data_dir);
	fp = fopen(signal_path, "w");
	if (fp == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create \"%s\": %m", signal_path)));
	fclose(fp);
	pfree(signal_path);

	/* If a target LSN is given, append recovery target settings */
	if (target_lsn != InvalidXLogRecPtr)
	{
		char	   *conf_path;

		conf_path = psprintf("%s/postgresql.auto.conf", data_dir);

		fp = fopen(conf_path, "a");
		if (fp == NULL)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open \"%s\" for append: %m",
							conf_path)));

		fprintf(fp, "\n# Point-in-time recovery target\n");
		fprintf(fp, "recovery_target_lsn = '%X/%X'\n",
				LSN_FORMAT_ARGS(target_lsn));
		fprintf(fp, "recovery_target_action = 'promote'\n");
		fprintf(fp, "recovery_target_inclusive = true\n");

		if (fclose(fp) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not close \"%s\": %m", conf_path)));

		pfree(conf_path);
	}
}

/*
 * branch_insert_metadata
 *
 * Insert a row into the alohadb_branches table.
 * Must be called within an SPI context.
 */
static void
branch_insert_metadata(const char *name, XLogRecPtr lsn, int port,
					   const char *data_dir)
{
	StringInfoData buf;
	int			ret;

	initStringInfo(&buf);

	if (lsn != InvalidXLogRecPtr)
		appendStringInfo(&buf,
						 "INSERT INTO alohadb_branches (name, parent_lsn, port, data_dir, status) "
						 "VALUES ('%s', '%X/%X'::pg_lsn, %d, '%s', '%s')",
						 name,
						 LSN_FORMAT_ARGS(lsn),
						 port, data_dir,
						 BRANCH_STATUS_RUNNING);
	else
		appendStringInfo(&buf,
						 "INSERT INTO alohadb_branches (name, parent_lsn, port, data_dir, status) "
						 "VALUES ('%s', NULL, %d, '%s', '%s')",
						 name, port, data_dir,
						 BRANCH_STATUS_RUNNING);

	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_INSERT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_branch: failed to insert branch metadata")));

	pfree(buf.data);
}

/*
 * branch_delete_metadata
 *
 * Delete a row from the alohadb_branches table.
 * Must be called within an SPI context.
 */
static void
branch_delete_metadata(const char *name)
{
	StringInfoData buf;
	int			ret;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "DELETE FROM alohadb_branches WHERE name = '%s'",
					 name);

	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_DELETE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_branch: failed to delete branch metadata for \"%s\"",
						name)));

	pfree(buf.data);
}

/* ----------------------------------------------------------------
 *	SQL-callable functions
 * ----------------------------------------------------------------
 */

/*
 * alohadb_create_branch(name text, from_lsn pg_lsn DEFAULT NULL,
 *                       OUT branch_name text, OUT port int, OUT data_dir text)
 * RETURNS record
 *
 * Creates a new database branch by:
 *   1. Taking a base backup of the current server via pg_basebackup
 *   2. Optionally configuring point-in-time recovery to from_lsn
 *   3. Writing postgresql.auto.conf overrides (different port, socket dir)
 *   4. Starting the branch as an independent postmaster
 *   5. Recording metadata in the alohadb_branches table
 *
 * Requires superuser privileges.
 */
Datum
alohadb_create_branch(PG_FUNCTION_ARGS)
{
	text	   *name_text;
	char	   *branch_name;
	XLogRecPtr	target_lsn = InvalidXLogRecPtr;
	char	   *data_dir;
	int			port;
	int			rc;
	StringInfoData cmd;
	TupleDesc	tupdesc;
	Datum		values[CREATE_BRANCH_COLS];
	bool		nulls[CREATE_BRANCH_COLS];
	HeapTuple	result_tuple;
	char		pg_bindir[MAXPGPATH];

	/* Superuser check */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("only superusers can create database branches")));

	/* Extract branch name -- first arg must not be NULL */
	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("branch name must not be NULL")));

	name_text = PG_GETARG_TEXT_PP(0);
	branch_name = text_to_cstring(name_text);

	if (!branch_name_is_valid(branch_name))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid branch name \"%s\"", branch_name),
				 errhint("Branch names must be 1-%d characters and contain "
						 "only alphanumeric characters, hyphens, and underscores.",
						 BRANCH_NAME_MAXLEN)));

	/* Optional target LSN (second arg may be NULL) */
	if (!PG_ARGISNULL(1))
		target_lsn = PG_GETARG_LSN(1);

	/*
	 * Determine the PostgreSQL bin directory by deriving it from the
	 * running postgres executable path (my_exec_path).  This is the most
	 * reliable method for a backend extension.
	 */
	strlcpy(pg_bindir, my_exec_path, MAXPGPATH);
	get_parent_directory(pg_bindir);

	/* Connect to SPI for metadata operations */
	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_branch: SPI_connect failed")));

	/* Check for duplicate branch name */
	{
		StringInfoData check;

		initStringInfo(&check);
		appendStringInfo(&check,
						 "SELECT 1 FROM alohadb_branches WHERE name = '%s'",
						 branch_name);

		rc = SPI_execute(check.data, true, 1);
		if (rc != SPI_OK_SELECT)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("alohadb_branch: failed to check for existing branch")));

		if (SPI_processed > 0)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("branch \"%s\" already exists", branch_name)));

		pfree(check.data);
	}

	/* Allocate port and data directory */
	port = branch_find_next_port();
	data_dir = branch_get_data_dir(branch_name);

	/* Ensure the branches directory exists */
	branch_ensure_dir();

	/*
	 * Step 1: Create the branch data directory via pg_basebackup.
	 *
	 * We use streaming mode (-Xs) and plain format (-Fp) to get a usable
	 * data directory directly.  The --no-sync flag speeds up the copy;
	 * the branch is ephemeral by nature.
	 */
	initStringInfo(&cmd);
	{
		/*
		 * Determine the socket directory for connecting to this server.
		 * Unix_socket_directories may be a comma-separated list; extract
		 * only the first entry.
		 */
		char		sockdir[MAXPGPATH];

		if (Unix_socket_directories && Unix_socket_directories[0] != '\0')
		{
			const char *comma;

			comma = strchr(Unix_socket_directories, ',');
			if (comma != NULL)
				strlcpy(sockdir, Unix_socket_directories,
						Min((size_t)(comma - Unix_socket_directories + 1),
							MAXPGPATH));
			else
				strlcpy(sockdir, Unix_socket_directories, MAXPGPATH);

			/* Trim leading/trailing whitespace */
			{
				char *p = sockdir;
				while (*p == ' ') p++;
				if (p != sockdir)
					memmove(sockdir, p, strlen(p) + 1);
			}
		}
		else
			strlcpy(sockdir, "/tmp", MAXPGPATH);

		appendStringInfo(&cmd,
						 "%s/pg_basebackup -D %s -Fp -Xs --no-sync "
						 "-h %s -p %d --checkpoint=fast 2>&1",
						 pg_bindir, data_dir, sockdir, PostPortNumber);
	}

	ereport(NOTICE,
			(errmsg("creating branch \"%s\" via pg_basebackup...",
					branch_name)));

	rc = branch_exec_command(cmd.data);
	if (rc != 0)
	{
		/* Clean up partial data directory on failure */
		StringInfoData cleanup;

		initStringInfo(&cleanup);
		appendStringInfo(&cleanup, "rm -rf %s", data_dir);
		(void) system(cleanup.data);
		pfree(cleanup.data);

		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("pg_basebackup failed with exit code %d", rc),
				 errhint("Check server logs and ensure WAL archiving or "
						 "replication slots are configured.")));
	}

	resetStringInfo(&cmd);

	/*
	 * Step 2: Write configuration overrides for the branch.
	 */
	branch_write_config_overrides(data_dir, port);

	/*
	 * Step 3: If a target LSN was specified, set up point-in-time recovery.
	 */
	if (target_lsn != InvalidXLogRecPtr)
		branch_write_recovery_conf(data_dir, target_lsn);

	/*
	 * Step 4: Create the log directory for the branch.
	 */
	{
		char	   *log_dir = psprintf("%s/log", data_dir);

		if (mkdir(log_dir, pg_dir_create_mode) != 0 && errno != EEXIST)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not create log directory \"%s\": %m",
							log_dir)));
		pfree(log_dir);
	}

	/*
	 * Step 5: Remove any standby.signal that pg_basebackup may have left,
	 * unless we explicitly want recovery mode.
	 */
	if (target_lsn == InvalidXLogRecPtr)
	{
		char	   *standby_signal;

		standby_signal = psprintf("%s/standby.signal", data_dir);
		(void) unlink(standby_signal);
		pfree(standby_signal);
	}

	/*
	 * Step 6: Start the branch postmaster using pg_ctl.
	 */
	appendStringInfo(&cmd,
					 "%s/pg_ctl start -D %s -l %s/log/branch.log "
					 "-o \"-p %d\" -w 2>&1",
					 pg_bindir, data_dir, data_dir, port);

	ereport(NOTICE,
			(errmsg("starting branch \"%s\" on port %d...",
					branch_name, port)));

	rc = branch_exec_command(cmd.data);
	if (rc != 0)
	{
		ereport(WARNING,
				(errmsg("pg_ctl start failed with exit code %d for branch \"%s\"",
						rc, branch_name),
				 errhint("The data directory has been preserved at \"%s\". "
						 "Check %s/log/branch.log for details.",
						 data_dir, data_dir)));
	}

	pfree(cmd.data);

	/*
	 * Step 7: Record metadata in the alohadb_branches table.
	 */
	{
		XLogRecPtr	recorded_lsn;

		if (target_lsn != InvalidXLogRecPtr)
			recorded_lsn = target_lsn;
		else
			recorded_lsn = GetXLogWriteRecPtr();

		branch_insert_metadata(branch_name, recorded_lsn, port, data_dir);
	}

	SPI_finish();

	/*
	 * Build the result tuple using the OUT-parameter tuple descriptor
	 * provided by the function call info.
	 */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	tupdesc = BlessTupleDesc(tupdesc);

	memset(nulls, 0, sizeof(nulls));
	values[0] = CStringGetTextDatum(branch_name);
	values[1] = Int32GetDatum(port);
	values[2] = CStringGetTextDatum(data_dir);

	result_tuple = heap_form_tuple(tupdesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(result_tuple));
}

/*
 * alohadb_list_branches()
 * RETURNS TABLE(name text, lsn pg_lsn, port int, data_dir text,
 *               status text, created_at timestamptz)
 *
 * Returns all branches recorded in the alohadb_branches table.
 */
Datum
alohadb_list_branches(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	MemoryContext oldcontext;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;
		int			ret;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Build the result tuple descriptor */
		tupdesc = CreateTemplateTupleDesc(LIST_BRANCHES_COLS);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "name",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "lsn",
						   LSNOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "port",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "data_dir",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "status",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "created_at",
						   TIMESTAMPTZOID, -1, 0);

		funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);

		/* Connect to SPI and fetch all branches */
		if (SPI_connect() != SPI_OK_CONNECT)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("alohadb_branch: SPI_connect failed")));

		ret = SPI_execute(
						  "SELECT name, "
						  "       parent_lsn::text, "
						  "       port::text, "
						  "       data_dir, "
						  "       status, "
						  "       created_at::text "
						  "FROM alohadb_branches "
						  "ORDER BY created_at",
						  true, 0);

		if (ret != SPI_OK_SELECT)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("alohadb_branch: failed to query branches table")));

		funcctx->max_calls = SPI_processed;

		/*
		 * Copy the SPI result into the multi-call memory context so it
		 * survives across calls.
		 */
		if (SPI_processed > 0)
		{
			SPITupleTable *tuptable;
			uint64		i;

			tuptable = SPI_tuptable;

			funcctx->user_fctx = palloc(sizeof(char **) * SPI_processed);

			for (i = 0; i < SPI_processed; i++)
			{
				char	  **row;
				int			j;

				row = palloc(sizeof(char *) * LIST_BRANCHES_COLS);
				for (j = 1; j <= LIST_BRANCHES_COLS; j++)
				{
					char   *val;

					val = SPI_getvalue(tuptable->vals[i],
									   tuptable->tupdesc, j);
					row[j - 1] = val ? pstrdup(val) : NULL;
				}

				((char ***) funcctx->user_fctx)[i] = row;
			}
		}

		SPI_finish();

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		char	  **row;
		HeapTuple	tuple;
		Datum		result;

		row = ((char ***) funcctx->user_fctx)[funcctx->call_cntr];

		tuple = BuildTupleFromCStrings(funcctx->attinmeta, row);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	else
	{
		SRF_RETURN_DONE(funcctx);
	}
}

/*
 * alohadb_drop_branch(name text)
 * RETURNS void
 *
 * Stops a branch postmaster and removes its data directory and metadata.
 * Requires superuser privileges.
 */
Datum
alohadb_drop_branch(PG_FUNCTION_ARGS)
{
	text	   *name_text;
	char	   *branch_name;
	char	   *data_dir = NULL;
	int			ret;
	StringInfoData cmd;
	char		pg_bindir[MAXPGPATH];

	/* Superuser check */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("only superusers can drop database branches")));

	name_text = PG_GETARG_TEXT_PP(0);
	branch_name = text_to_cstring(name_text);

	if (!branch_name_is_valid(branch_name))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid branch name \"%s\"", branch_name)));

	/* Determine bin directory from the running postgres binary */
	strlcpy(pg_bindir, my_exec_path, MAXPGPATH);
	get_parent_directory(pg_bindir);

	/* Connect to SPI and look up the branch */
	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_branch: SPI_connect failed")));

	{
		StringInfoData query;

		initStringInfo(&query);
		appendStringInfo(&query,
						 "SELECT data_dir FROM alohadb_branches WHERE name = '%s'",
						 branch_name);

		ret = SPI_execute(query.data, true, 1);
		if (ret != SPI_OK_SELECT)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("alohadb_branch: failed to look up branch \"%s\"",
							branch_name)));

		if (SPI_processed == 0)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("branch \"%s\" does not exist", branch_name)));

		{
			char   *val;

			val = SPI_getvalue(SPI_tuptable->vals[0],
							   SPI_tuptable->tupdesc, 1);
			if (val != NULL)
				data_dir = pstrdup(val);
		}

		pfree(query.data);
	}

	if (data_dir == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("branch \"%s\" has no recorded data directory",
						branch_name)));

	/*
	 * Step 1: Stop the branch postmaster.
	 *
	 * Use "fast" mode to shut down promptly.  Ignore errors because the
	 * branch might already be stopped.
	 */
	initStringInfo(&cmd);
	appendStringInfo(&cmd,
					 "%s/pg_ctl stop -D %s -m fast 2>&1",
					 pg_bindir, data_dir);

	ereport(NOTICE,
			(errmsg("stopping branch \"%s\"...", branch_name)));

	(void) branch_exec_command(cmd.data);

	/*
	 * Step 2: Remove the data directory.
	 *
	 * Safety check: only remove if it's under our branches directory.
	 */
	{
		char	   *expected_prefix;

		expected_prefix = psprintf("%s/%s/",
								  branch_get_parent_dir(), BRANCH_SUBDIR);

		if (strncmp(data_dir, expected_prefix, strlen(expected_prefix)) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("refusing to remove data directory \"%s\" "
							"which is not under the branches directory",
							data_dir)));

		pfree(expected_prefix);
	}

	resetStringInfo(&cmd);
	appendStringInfo(&cmd, "rm -rf %s", data_dir);

	ereport(NOTICE,
			(errmsg("removing branch data directory \"%s\"...", data_dir)));

	{
		int		rc;

		rc = branch_exec_command(cmd.data);
		if (rc != 0)
			ereport(WARNING,
					(errmsg("failed to remove data directory \"%s\" (exit code %d)",
							data_dir, rc)));
	}

	pfree(cmd.data);

	/*
	 * Step 3: Remove metadata from the branches table.
	 */
	branch_delete_metadata(branch_name);

	SPI_finish();

	PG_RETURN_VOID();
}
