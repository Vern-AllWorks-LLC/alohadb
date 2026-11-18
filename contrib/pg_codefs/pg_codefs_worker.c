/*-------------------------------------------------------------------------
 *
 * pg_codefs_worker.c
 *	  Background workers for AlohaDB CodeFS.
 *
 *	  Worker 1 (sync): Bidirectional sync between DB files and disk.
 *	  Its running state serves as a marker for agents to know if
 *	  codefs is active. Only runs if the extension is installed.
 *
 *	  Worker 2 (policy): Enforces codefs_policies settings --
 *	  archives old versions to compressed files on disk, offloads
 *	  large binaries, enforces retention, cleans expired locks.
 *
 * Copyright (c) 2026, OpenCan.ai / AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_codefs/pg_codefs_worker.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "pg_codefs.h"

#include "fmgr.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "executor/spi.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/wait_classes.h"
#include "lib/stringinfo.h"

#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>
#include <zlib.h>

/* ----------------------------------------------------------------
 * Helper: check if extension is installed in the database
 * ---------------------------------------------------------------- */
static bool
codefs_extension_installed(void)
{
	int		ret;

	ret = SPI_execute(
		"SELECT 1 FROM pg_extension WHERE extname = 'pg_codefs'",
		true, 1);

	return (ret == SPI_OK_SELECT && SPI_processed > 0);
}

/* ----------------------------------------------------------------
 * Helper: compress a string with zlib and write to file
 * ---------------------------------------------------------------- */
static bool
compress_to_file(const char *data, int len, const char *filepath)
{
	gzFile	gz;

	gz = gzopen(filepath, "wb9");
	if (!gz)
		return false;

	if (gzwrite(gz, data, len) != len)
	{
		gzclose(gz);
		return false;
	}

	gzclose(gz);
	return true;
}

/* ================================================================
 * SYNC WORKER
 *
 * Syncs files between codefs_files (main branch) and disk.
 * Its PID in shared memory tells agents codefs is active.
 * ================================================================ */
void
codefs_sync_worker_main(Datum main_arg)
{
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection(
		codefs_database_guc ? codefs_database_guc : "postgres",
		NULL, 0);

	elog(LOG, "pg_codefs: sync worker started (interval=%ds)",
		 codefs_sync_interval_guc);

	/* Mark active in shared memory */
	if (codefs_shared)
	{
		LWLockAcquire(codefs_shared->lock, LW_EXCLUSIVE);
		codefs_shared->sync_worker_active = true;
		codefs_shared->sync_worker_pid = MyProcPid;
		LWLockRelease(codefs_shared->lock);
	}

	for (;;)
	{
		int		rc;
		int		synced = 0;

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   (long) codefs_sync_interval_guc * 1000L,
					   PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
			elog(LOG, "pg_codefs: sync worker config reloaded");
		}

		/* Start transaction for SPI */
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());

		/* Check if extension is installed */
		if (!codefs_extension_installed())
		{
			PopActiveSnapshot();
			SPI_finish();
			CommitTransactionCommand();

			/* Mark inactive */
			if (codefs_shared)
			{
				LWLockAcquire(codefs_shared->lock, LW_EXCLUSIVE);
				codefs_shared->sync_worker_active = false;
				LWLockRelease(codefs_shared->lock);
			}
			continue;
		}

		/* Mark active */
		if (codefs_shared)
		{
			LWLockAcquire(codefs_shared->lock, LW_EXCLUSIVE);
			codefs_shared->sync_worker_active = true;
			LWLockRelease(codefs_shared->lock);
		}

		/*
		 * Read sync_base_path from policy.
		 */
		{
			int		ret;
			char   *sync_path = NULL;

			ret = SPI_execute(
				"SELECT sync_base_path FROM codefs_policies "
				"WHERE policy_name = 'default' LIMIT 1",
				true, 1);

			if (ret == SPI_OK_SELECT && SPI_processed > 0)
			{
				sync_path = SPI_getvalue(SPI_tuptable->vals[0],
										  SPI_tuptable->tupdesc, 1);
			}

			if (sync_path && sync_path[0] != '\0')
			{
				StringInfoData sql;

				/* Ensure sync directory exists */
				pg_mkdir_p(sync_path, 0755);

				/*
				 * DB-to-disk: write files updated since last sync.
				 * Only sync main branch to disk.
				 */
				initStringInfo(&sql);
				appendStringInfo(&sql,
					"SELECT path, content, checksum "
					"FROM codefs_files "
					"WHERE branch = 'main' AND is_deleted = false "
					"AND content IS NOT NULL");

				ret = SPI_execute(sql.data, true, 0);
				if (ret == SPI_OK_SELECT)
				{
					uint64	i;

					for (i = 0; i < SPI_processed; i++)
					{
						char   *fpath = SPI_getvalue(
							SPI_tuptable->vals[i],
							SPI_tuptable->tupdesc, 1);
						char   *content = SPI_getvalue(
							SPI_tuptable->vals[i],
							SPI_tuptable->tupdesc, 2);
						char   *db_checksum = SPI_getvalue(
							SPI_tuptable->vals[i],
							SPI_tuptable->tupdesc, 3);

						if (fpath && content)
						{
							char	full_path[CODEFS_MAX_PATH_LEN * 2];
							char	dir_buf[CODEFS_MAX_PATH_LEN * 2];
							char   *dir_end;
							FILE   *fp;
							bool	needs_write = true;

							snprintf(full_path, sizeof(full_path),
									 "%s/%s", sync_path, fpath);

							/* Check if disk file matches */
							fp = fopen(full_path, "r");
							if (fp)
							{
								/* Read and compute checksum */
								StringInfoData disk_content;
								char	buf[8192];
								size_t	nread;
								char	disk_cksum[CODEFS_SHA256_LEN];

								initStringInfo(&disk_content);
								while ((nread = fread(buf, 1,
													  sizeof(buf), fp)) > 0)
									appendBinaryStringInfo(&disk_content,
														   buf, nread);
								fclose(fp);

								codefs_compute_sha256(disk_content.data,
													  disk_content.len,
													  disk_cksum);

								if (db_checksum &&
									strcmp(disk_cksum, db_checksum) == 0)
									needs_write = false;

								pfree(disk_content.data);
							}

							if (needs_write)
							{
								/* Create parent dirs */
								strncpy(dir_buf, full_path,
										sizeof(dir_buf) - 1);
								dir_buf[sizeof(dir_buf) - 1] = '\0';
								dir_end = strrchr(dir_buf, '/');
								if (dir_end)
								{
									*dir_end = '\0';
									pg_mkdir_p(dir_buf, 0755);
								}

								fp = fopen(full_path, "w");
								if (fp)
								{
									fputs(content, fp);
									fclose(fp);
									synced++;
								}
							}
						}
					}
				}

				pfree(sql.data);
			}
		}

		/* Update shared memory */
		if (codefs_shared)
		{
			LWLockAcquire(codefs_shared->lock, LW_EXCLUSIVE);
			codefs_shared->last_sync_time = GetCurrentTimestamp();
			codefs_shared->files_synced += synced;
			LWLockRelease(codefs_shared->lock);
		}

		PopActiveSnapshot();
		SPI_finish();
		CommitTransactionCommand();
	}
}

/* ================================================================
 * POLICY ENFORCEMENT WORKER
 *
 * Enforces codefs_policies:
 * - Archives old versions (compress to disk)
 * - Offloads large binaries
 * - Enforces retention (delete old archives)
 * - Cleans expired locks
 * ================================================================ */
void
codefs_policy_worker_main(Datum main_arg)
{
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection(
		codefs_database_guc ? codefs_database_guc : "postgres",
		NULL, 0);

	elog(LOG, "pg_codefs: policy worker started (interval=%ds)",
		 codefs_policy_interval_guc);

	if (codefs_shared)
	{
		LWLockAcquire(codefs_shared->lock, LW_EXCLUSIVE);
		codefs_shared->policy_worker_active = true;
		codefs_shared->policy_worker_pid = MyProcPid;
		LWLockRelease(codefs_shared->lock);
	}

	for (;;)
	{
		int		rc;
		int		archived = 0;

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   (long) codefs_policy_interval_guc * 1000L,
					   PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());

		if (!codefs_extension_installed())
		{
			PopActiveSnapshot();
			SPI_finish();
			CommitTransactionCommand();

			if (codefs_shared)
			{
				LWLockAcquire(codefs_shared->lock, LW_EXCLUSIVE);
				codefs_shared->policy_worker_active = false;
				LWLockRelease(codefs_shared->lock);
			}
			continue;
		}

		if (codefs_shared)
		{
			LWLockAcquire(codefs_shared->lock, LW_EXCLUSIVE);
			codefs_shared->policy_worker_active = true;
			LWLockRelease(codefs_shared->lock);
		}

		/* Read policy */
		{
			int		ret;
			int		max_versions = 50;
			char   *archive_path = NULL;
			char   *binary_path = NULL;
			bool	store_binaries = false;
			int		retention_days = 365;
			bool	do_compress = true;
			int64	max_file_size = 10 * 1024 * 1024;

			ret = SPI_execute(
				"SELECT max_versions, archive_storage_path, "
				"  binary_storage_path, store_binaries_in_db, "
				"  retention_days, compress_archived, "
				"  max_file_size_bytes "
				"FROM codefs_policies "
				"WHERE policy_name = 'default' LIMIT 1",
				true, 1);

			if (ret == SPI_OK_SELECT && SPI_processed > 0)
			{
				bool	isnull;
				Datum	d;

				d = SPI_getbinval(SPI_tuptable->vals[0],
								  SPI_tuptable->tupdesc, 1, &isnull);
				if (!isnull) max_versions = DatumGetInt32(d);

				archive_path = SPI_getvalue(SPI_tuptable->vals[0],
											 SPI_tuptable->tupdesc, 2);

				binary_path = SPI_getvalue(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc, 3);

				d = SPI_getbinval(SPI_tuptable->vals[0],
								  SPI_tuptable->tupdesc, 4, &isnull);
				if (!isnull) store_binaries = DatumGetBool(d);

				d = SPI_getbinval(SPI_tuptable->vals[0],
								  SPI_tuptable->tupdesc, 5, &isnull);
				if (!isnull) retention_days = DatumGetInt32(d);

				d = SPI_getbinval(SPI_tuptable->vals[0],
								  SPI_tuptable->tupdesc, 6, &isnull);
				if (!isnull) do_compress = DatumGetBool(d);

				d = SPI_getbinval(SPI_tuptable->vals[0],
								  SPI_tuptable->tupdesc, 7, &isnull);
				if (!isnull) max_file_size = DatumGetInt64(d);
			}

			/*
			 * Task 1: Archive old versions beyond max_versions.
			 * Find files with more versions than allowed,
			 * compress and move oldest to disk.
			 */
			if (archive_path && archive_path[0] != '\0')
			{
				StringInfoData sql;

				pg_mkdir_p(archive_path, 0755);

				initStringInfo(&sql);
				appendStringInfo(&sql,
					"SELECT v.version_id, v.file_id, v.version_num, "
					"  v.content, v.checksum, f.path "
					"FROM codefs_versions v "
					"JOIN codefs_files f ON v.file_id = f.file_id "
					"WHERE v.is_archived = false AND v.content IS NOT NULL "
					"AND v.version_num <= ("
					"  SELECT MAX(v2.version_num) - %d "
					"  FROM codefs_versions v2 "
					"  WHERE v2.file_id = v.file_id"
					") "
					"ORDER BY v.file_id, v.version_num "
					"LIMIT 100",
					max_versions);

				ret = SPI_execute(sql.data, true, 0);

				if (ret == SPI_OK_SELECT && SPI_processed > 0)
				{
					uint64	i;

					for (i = 0; i < SPI_processed; i++)
					{
						bool	isnull;
						int64	vid;
						char   *content;
						char   *fpath;
						int		vnum;
						char	arch_file[CODEFS_MAX_PATH_LEN * 2];

						vid = DatumGetInt64(
							SPI_getbinval(SPI_tuptable->vals[i],
										  SPI_tuptable->tupdesc, 1, &isnull));

						vnum = DatumGetInt32(
							SPI_getbinval(SPI_tuptable->vals[i],
										  SPI_tuptable->tupdesc, 3, &isnull));

						content = SPI_getvalue(SPI_tuptable->vals[i],
												SPI_tuptable->tupdesc, 4);

						fpath = SPI_getvalue(SPI_tuptable->vals[i],
											  SPI_tuptable->tupdesc, 6);

						if (!content || !fpath)
							continue;

						/* Write compressed archive */
						snprintf(arch_file, sizeof(arch_file),
								 "%s/%s.v%d%s",
								 archive_path, fpath, vnum,
								 do_compress ? ".gz" : "");

						/* Create parent dirs for archive */
						{
							char	dir_buf[CODEFS_MAX_PATH_LEN * 2];
							char   *dir_end;

							strncpy(dir_buf, arch_file,
									sizeof(dir_buf) - 1);
							dir_buf[sizeof(dir_buf) - 1] = '\0';
							dir_end = strrchr(dir_buf, '/');
							if (dir_end)
							{
								*dir_end = '\0';
								pg_mkdir_p(dir_buf, 0755);
							}
						}

						if (do_compress)
						{
							if (compress_to_file(content, strlen(content),
												 arch_file))
							{
								/* Mark as archived, clear content */
								StringInfoData upd;

								initStringInfo(&upd);
								appendStringInfo(&upd,
									"UPDATE codefs_versions SET "
									"  is_archived = true, "
									"  archive_path = %s, "
									"  content = NULL "
									"WHERE version_id = %lld",
									quote_literal_cstr(arch_file),
									(long long) vid);

								SPI_execute(upd.data, false, 0);
								pfree(upd.data);
								archived++;
							}
						}
						else
						{
							FILE *fp = fopen(arch_file, "w");
							if (fp)
							{
								StringInfoData upd;

								fputs(content, fp);
								fclose(fp);

								initStringInfo(&upd);
								appendStringInfo(&upd,
									"UPDATE codefs_versions SET "
									"  is_archived = true, "
									"  archive_path = %s, "
									"  content = NULL "
									"WHERE version_id = %lld",
									quote_literal_cstr(arch_file),
									(long long) vid);

								SPI_execute(upd.data, false, 0);
								pfree(upd.data);
								archived++;
							}
						}
					}
				}

				pfree(sql.data);
			}

			/*
			 * Task 2: Offload large binaries to disk if policy says so.
			 */
			if (!store_binaries && binary_path && binary_path[0] != '\0')
			{
				StringInfoData sql;

				pg_mkdir_p(binary_path, 0755);

				initStringInfo(&sql);
				appendStringInfo(&sql,
					"SELECT file_id, path, content, size_bytes "
					"FROM codefs_files "
					"WHERE is_binary = true AND content IS NOT NULL "
					"AND size_bytes > %lld "
					"LIMIT 50",
					(long long) max_file_size);

				ret = SPI_execute(sql.data, true, 0);

				if (ret == SPI_OK_SELECT && SPI_processed > 0)
				{
					uint64	i;

					for (i = 0; i < SPI_processed; i++)
					{
						bool	isnull;
						int64	fid;
						char   *content;
						char   *fpath;
						char	disk_file[CODEFS_MAX_PATH_LEN * 2];
						char	dir_buf[CODEFS_MAX_PATH_LEN * 2];
						char   *dir_end;
						FILE   *fp;

						fid = DatumGetInt64(
							SPI_getbinval(SPI_tuptable->vals[i],
										  SPI_tuptable->tupdesc, 1, &isnull));

						fpath = SPI_getvalue(SPI_tuptable->vals[i],
											  SPI_tuptable->tupdesc, 2);
						content = SPI_getvalue(SPI_tuptable->vals[i],
												SPI_tuptable->tupdesc, 3);

						if (!fpath || !content)
							continue;

						snprintf(disk_file, sizeof(disk_file),
								 "%s/%s", binary_path, fpath);

						strncpy(dir_buf, disk_file, sizeof(dir_buf) - 1);
						dir_buf[sizeof(dir_buf) - 1] = '\0';
						dir_end = strrchr(dir_buf, '/');
						if (dir_end)
						{
							*dir_end = '\0';
							pg_mkdir_p(dir_buf, 0755);
						}

						fp = fopen(disk_file, "wb");
						if (fp)
						{
							StringInfoData upd;

							fputs(content, fp);
							fclose(fp);

							initStringInfo(&upd);
							appendStringInfo(&upd,
								"UPDATE codefs_files SET "
								"  content = NULL, "
								"  metadata = metadata || "
								"    jsonb_build_object("
								"      'binary_disk_path', %s) "
								"WHERE file_id = %lld",
								quote_literal_cstr(disk_file),
								(long long) fid);

							SPI_execute(upd.data, false, 0);
							pfree(upd.data);
						}
					}
				}

				pfree(sql.data);
			}

			/*
			 * Task 3: Retention cleanup -- delete archived versions
			 * older than retention_days.
			 */
			{
				StringInfoData sql;

				initStringInfo(&sql);
				appendStringInfo(&sql,
					"DELETE FROM codefs_versions "
					"WHERE is_archived = true "
					"AND committed_at < now() - interval '%d days'",
					retention_days);

				SPI_execute(sql.data, false, 0);
				pfree(sql.data);
			}

			/*
			 * Task 4: Clean expired locks.
			 */
			SPI_execute(
				"DELETE FROM codefs_locks "
				"WHERE locked_at + lock_timeout < now()",
				false, 0);
		}

		/* Update shared memory */
		if (codefs_shared)
		{
			LWLockAcquire(codefs_shared->lock, LW_EXCLUSIVE);
			codefs_shared->last_policy_run = GetCurrentTimestamp();
			codefs_shared->versions_archived += archived;
			LWLockRelease(codefs_shared->lock);
		}

		PopActiveSnapshot();
		SPI_finish();
		CommitTransactionCommand();
	}
}
