/*-------------------------------------------------------------------------
 *
 * pg_codefs.h
 *	  AlohaDB CodeFS - virtual filesystem with git-like versioning.
 *
 * Copyright (c) 2026, AlohaDB / OpenCan.ai
 *
 *-------------------------------------------------------------------------
 */
#ifndef ALOHADB_CODEFS_H
#define ALOHADB_CODEFS_H

#include "postgres.h"
#include "fmgr.h"
#include "storage/lwlock.h"
#include "utils/timestamp.h"

#define CODEFS_MAX_PATH_LEN		4096
#define CODEFS_LOCK_TIMEOUT_MS	5000
#define CODEFS_SHA256_LEN		65		/* hex string + NUL */

/* ----------------------------------------------------------------
 * Shared memory state -- visible to SQL functions and workers
 * ---------------------------------------------------------------- */
typedef struct CodefsSharedState
{
	LWLock	   *lock;
	bool		sync_worker_active;
	bool		policy_worker_active;
	TimestampTz last_sync_time;
	TimestampTz last_policy_run;
	int			files_synced;
	int			versions_archived;
	pid_t		sync_worker_pid;
	pid_t		policy_worker_pid;
} CodefsSharedState;

/* Worker entry points */
extern PGDLLEXPORT void codefs_sync_worker_main(Datum main_arg);
extern PGDLLEXPORT void codefs_policy_worker_main(Datum main_arg);

/* Shared state pointer (set during shmem_startup) */
extern CodefsSharedState *codefs_shared;

/* GUC variables */
extern char *codefs_database_guc;
extern int	 codefs_sync_interval_guc;
extern int	 codefs_policy_interval_guc;

/* Internal helpers */
extern bool codefs_check_scope(const char *path, const char *scope);
extern void codefs_validate_path(const char *path);
extern void codefs_compute_sha256(const char *data, int len, char *out);

#endif /* ALOHADB_CODEFS_H */
