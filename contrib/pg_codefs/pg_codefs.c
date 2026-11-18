/*-------------------------------------------------------------------------
 *
 * pg_codefs.c
 *	  AlohaDB CodeFS - virtual filesystem with git-like versioning.
 *
 *	  Stores source files as rows in PostgreSQL tables with full version
 *	  history, branching, merging, locking, and bidirectional disk sync.
 *	  Designed for AI-native development where agents read/write code
 *	  via SQL instead of filesystem calls.
 *
 *	  Prior art: Pgfs (Linux Journal, 1996), Oracle DBFS (2009),
 *	  AgentFS/Turso (2024), Unison lang (2013), Dolt (2019),
 *	  PGFs (2023), pgfs FUSE (2023), WinFS (2003), libsqlfs.
 *
 * Copyright (c) 2026, OpenCan.ai / AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_codefs/pg_codefs.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "pg_codefs.h"

#include "fmgr.h"
#include "funcapi.h"
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

#include <openssl/evp.h>
#include <string.h>
#ifndef WIN32
#include <fnmatch.h>
#else
/*
 * Windows has no fnmatch(); this matches fnmatch(pattern, string, 0):
 * '*' and '?' match any characters (including '/'), "[...]" sets with
 * ranges and leading '!' or '^' negation, and '\' escapes.
 */
static int
fnmatch(const char *p, const char *s, int flags)
{
	(void) flags;
	for (; *p; p++, s++)
	{
		switch (*p)
		{
			case '?':
				if (*s == '\0')
					return 1;
				break;
			case '*':
				while (p[1] == '*')
					p++;
				if (p[1] == '\0')
					return 0;
				for (; *s; s++)
					if (fnmatch(p + 1, s, 0) == 0)
						return 0;
				return fnmatch(p + 1, s, 0);
			case '[':
				{
					const char *q = p + 1;
					bool		neg = (*q == '!' || *q == '^');
					bool		hit = false;

					if (*s == '\0')
						return 1;
					if (neg)
						q++;
					do
					{
						if (q[1] == '-' && q[2] && q[2] != ']')
						{
							if ((unsigned char) *s >= (unsigned char) q[0] &&
								(unsigned char) *s <= (unsigned char) q[2])
								hit = true;
							q += 3;
						}
						else
						{
							if (*q == *s)
								hit = true;
							q++;
						}
					} while (*q && *q != ']');
					if (*q != ']')
					{
						/* no closing bracket: treat '[' literally */
						if (*s != '[')
							return 1;
						break;
					}
					if (hit == neg)
						return 1;
					p = q;
				}
				break;
			case '\\':
				if (p[1])
					p++;
				/* FALLTHROUGH */
			default:
				if (*p != *s)
					return 1;
		}
	}
	return *s != '\0';
}
#endif

PG_MODULE_MAGIC_EXT(
					.name = "pg_codefs",
					.version = "1.0"
);

/* SQL-callable function declarations */
PG_FUNCTION_INFO_V1(codefs_write);
PG_FUNCTION_INFO_V1(codefs_read);
PG_FUNCTION_INFO_V1(codefs_delete);
PG_FUNCTION_INFO_V1(codefs_rename);
PG_FUNCTION_INFO_V1(codefs_list);
PG_FUNCTION_INFO_V1(codefs_commit);
PG_FUNCTION_INFO_V1(codefs_history);
PG_FUNCTION_INFO_V1(codefs_diff);
PG_FUNCTION_INFO_V1(codefs_branch_create);
PG_FUNCTION_INFO_V1(codefs_branch_list);
PG_FUNCTION_INFO_V1(codefs_branch_merge);
PG_FUNCTION_INFO_V1(codefs_branch_delete);
PG_FUNCTION_INFO_V1(codefs_lock_file);
PG_FUNCTION_INFO_V1(codefs_unlock_file);
PG_FUNCTION_INFO_V1(codefs_search);
PG_FUNCTION_INFO_V1(codefs_status);
PG_FUNCTION_INFO_V1(codefs_export_to_disk);
PG_FUNCTION_INFO_V1(codefs_import_from_disk);
PG_FUNCTION_INFO_V1(codefs_revert);

/* ----------------------------------------------------------------
 * Shared memory
 * ---------------------------------------------------------------- */
CodefsSharedState *codefs_shared = NULL;

static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* ----------------------------------------------------------------
 * GUC variables
 * ---------------------------------------------------------------- */
char *codefs_database_guc = NULL;
int   codefs_sync_interval_guc = 30;
int   codefs_policy_interval_guc = 300;

/* ----------------------------------------------------------------
 * Shared memory size / request / startup
 * ---------------------------------------------------------------- */
static Size
codefs_shmem_size(void)
{
	return MAXALIGN(sizeof(CodefsSharedState));
}

static void
codefs_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
	RequestAddinShmemSpace(codefs_shmem_size());
	RequestNamedLWLockTranche("pg_codefs", 1);
}

static void
codefs_shmem_startup(void)
{
	bool	found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	codefs_shared = ShmemInitStruct("pg_codefs",
									codefs_shmem_size(),
									&found);
	if (!found)
	{
		memset(codefs_shared, 0, sizeof(CodefsSharedState));
		codefs_shared->lock =
			&(GetNamedLWLockTranche("pg_codefs"))->lock;
		codefs_shared->sync_worker_active = false;
		codefs_shared->policy_worker_active = false;
		codefs_shared->last_sync_time = 0;
		codefs_shared->last_policy_run = 0;
		codefs_shared->files_synced = 0;
		codefs_shared->versions_archived = 0;
		codefs_shared->sync_worker_pid = 0;
		codefs_shared->policy_worker_pid = 0;
	}

	LWLockRelease(AddinShmemInitLock);
}

/* ----------------------------------------------------------------
 * _PG_init -- register GUCs and background workers
 * ---------------------------------------------------------------- */
void
_PG_init(void)
{
	BackgroundWorker worker;

	if (!process_shared_preload_libraries_in_progress)
		return;

	/* GUC: pg_codefs.database */
	DefineCustomStringVariable("pg_codefs.database",
							   "Database for codefs background workers.",
							   NULL,
							   &codefs_database_guc,
							   "postgres",
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	/* GUC: pg_codefs.sync_interval */
	DefineCustomIntVariable("pg_codefs.sync_interval",
							"Seconds between disk sync cycles.",
							NULL,
							&codefs_sync_interval_guc,
							30, 5, 3600,
							PGC_SIGHUP,
							GUC_UNIT_S,
							NULL, NULL, NULL);

	/* GUC: pg_codefs.policy_interval */
	DefineCustomIntVariable("pg_codefs.policy_interval",
							"Seconds between policy enforcement cycles.",
							NULL,
							&codefs_policy_interval_guc,
							300, 30, 86400,
							PGC_SIGHUP,
							GUC_UNIT_S,
							NULL, NULL, NULL);

	MarkGUCPrefixReserved("pg_codefs");

	/* Shared memory hooks */
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = codefs_shmem_request;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = codefs_shmem_startup;

	/* Worker 1: sync worker (DB <-> disk) */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
	snprintf(worker.bgw_library_name, MAXPGPATH, "pg_codefs");
	snprintf(worker.bgw_function_name, BGW_MAXLEN,
			 "codefs_sync_worker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_codefs sync");
	snprintf(worker.bgw_type, BGW_MAXLEN, "pg_codefs");
	worker.bgw_main_arg = (Datum) 0;
	worker.bgw_notify_pid = 0;
	if (!IsBinaryUpgrade)	/* no background work during pg_upgrade */
		RegisterBackgroundWorker(&worker);

	/* Worker 2: policy enforcement */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
	snprintf(worker.bgw_library_name, MAXPGPATH, "pg_codefs");
	snprintf(worker.bgw_function_name, BGW_MAXLEN,
			 "codefs_policy_worker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_codefs policy");
	snprintf(worker.bgw_type, BGW_MAXLEN, "pg_codefs");
	worker.bgw_main_arg = (Datum) 1;
	worker.bgw_notify_pid = 0;
	if (!IsBinaryUpgrade)	/* no background work during pg_upgrade */
		RegisterBackgroundWorker(&worker);
}

/* ================================================================
 * INTERNAL HELPERS
 * ================================================================ */

/*
 * codefs_validate_path -- reject dangerous paths
 */
void
codefs_validate_path(const char *path)
{
	if (!path || path[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("codefs: path cannot be empty")));

	if (strlen(path) > CODEFS_MAX_PATH_LEN)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("codefs: path exceeds %d characters",
						CODEFS_MAX_PATH_LEN)));

	if (path[0] == '/')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("codefs: absolute paths not allowed")));

	if (strstr(path, "..") != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("codefs: path traversal (..) not allowed")));

	if (memchr(path, '\0', strlen(path)) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("codefs: NUL bytes in path not allowed")));
}

/*
 * codefs_compute_sha256 -- SHA-256 of content, hex output
 */
void
codefs_compute_sha256(const char *data, int len, char *out)
{
	EVP_MD_CTX *ctx;
	unsigned char hash[EVP_MAX_MD_SIZE];
	unsigned int  hash_len;
	int			  i;

	ctx = EVP_MD_CTX_new();
	EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
	EVP_DigestUpdate(ctx, data, len);
	EVP_DigestFinal_ex(ctx, hash, &hash_len);
	EVP_MD_CTX_free(ctx);

	for (i = 0; i < (int) hash_len; i++)
		snprintf(out + i * 2, 3, "%02x", hash[i]);
	out[hash_len * 2] = '\0';
}

/*
 * codefs_check_scope -- match path against comma-separated globs
 *
 * scope = "*" means allow all.
 * scope = "src/app1/ *, src/app2/ *" means only those paths.
 */
bool
codefs_check_scope(const char *path, const char *scope)
{
	char   *copy;
	char   *token;
	char   *saveptr;
	bool	matched = false;

	if (!scope || scope[0] == '\0' || strcmp(scope, "*") == 0)
		return true;

	copy = pstrdup(scope);
	for (token = strtok_r(copy, ",", &saveptr);
		 token != NULL;
		 token = strtok_r(NULL, ",", &saveptr))
	{
		/* trim leading spaces */
		while (*token == ' ')
			token++;

		if (fnmatch(token, path, 0) == 0)
		{
			matched = true;
			break;
		}
	}

	pfree(copy);
	return matched;
}

/*
 * check_agent_scope_policy -- read policy and enforce agent_write_scope
 *
 * Must be called inside an SPI context.
 */
static void
check_agent_scope_policy(const char *path)
{
	int		ret;

	ret = SPI_execute(
		"SELECT agent_write_scope FROM codefs_policies "
		"WHERE policy_name = 'default' LIMIT 1",
		true, 1);

	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		char   *scope;

		scope = SPI_getvalue(SPI_tuptable->vals[0],
							 SPI_tuptable->tupdesc, 1);
		if (scope)
		{
			if (!codefs_check_scope(path, scope))
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("codefs: path '%s' outside agent_write_scope '%s'",
								path, scope)));
		}
	}
}

/*
 * check_branch_policy -- enforce auto_branch_on_agent and branch-per-agent
 *
 * Must be called inside an SPI context.
 * Returns the branch name to use (may create an agent branch).
 */
static char *
check_branch_policy(const char *requested_branch)
{
	int		ret;

	/* If writing to main, check if agent branching is enforced */
	if (strcmp(requested_branch, "main") == 0)
	{
		ret = SPI_execute(
			"SELECT auto_branch_on_agent FROM codefs_policies "
			"WHERE policy_name = 'default' LIMIT 1",
			true, 1);

		if (ret == SPI_OK_SELECT && SPI_processed > 0)
		{
			bool	isnull;
			Datum	d = SPI_getbinval(SPI_tuptable->vals[0],
									  SPI_tuptable->tupdesc, 1, &isnull);
			if (!isnull && DatumGetBool(d))
			{
				/*
				 * Check if current user has codefs_agent role.
				 * If so, auto-create a branch for them.
				 */
				ret = SPI_execute(
					"SELECT EXISTS("
					"  SELECT 1 FROM pg_auth_members m "
					"  JOIN pg_roles r ON r.oid = m.roleid "
					"  WHERE r.rolname = 'codefs_agent' "
					"  AND m.member = (SELECT oid FROM pg_roles "
					"    WHERE rolname = current_user))",
					true, 1);

				if (ret == SPI_OK_SELECT && SPI_processed > 0)
				{
					Datum	d2 = SPI_getbinval(SPI_tuptable->vals[0],
											   SPI_tuptable->tupdesc, 1,
											   &isnull);
					if (!isnull && DatumGetBool(d2))
					{
						/* Agent trying to write to main -- create branch */
						StringInfoData sql;
						char	   *branch_name;

						branch_name = psprintf("agent/%s",
											   GetUserNameFromId(GetUserId(),
																 false));

						initStringInfo(&sql);
						appendStringInfo(&sql,
							"INSERT INTO codefs_branches "
							"(name, parent_branch, is_agent, agent_name, status) "
							"VALUES (%s, 'main', true, %s, 'active') "
							"ON CONFLICT (name) DO NOTHING",
							quote_literal_cstr(branch_name),
							quote_literal_cstr(
								GetUserNameFromId(GetUserId(), false)));

						SPI_execute(sql.data, false, 0);
						pfree(sql.data);

						ereport(NOTICE,
								(errmsg("codefs: agent auto-branched to '%s'",
										branch_name)));
						return branch_name;
					}
				}
			}
		}
	}

	return pstrdup(requested_branch);
}

/*
 * get_advisory_lock_key -- deterministic int64 from path + branch
 */
static int64
get_advisory_lock_key(const char *path, const char *branch)
{
	unsigned int h = 0;
	const char  *p;

	for (p = path; *p; p++)
		h = h * 31 + (unsigned char) *p;
	for (p = branch; *p; p++)
		h = h * 31 + (unsigned char) *p;

	return (int64) h;
}

/*
 * is_binary_content -- simple heuristic: contains NUL bytes
 */
static bool
is_binary_content(const char *data, int len)
{
	int		i;

	for (i = 0; i < len && i < 8192; i++)
	{
		if (data[i] == '\0')
			return true;
	}
	return false;
}

/*
 * guess_mime_type -- basic extension-based detection
 */
static const char *
guess_mime_type(const char *path)
{
	const char *dot = strrchr(path, '.');

	if (!dot)
		return "text/plain";

	if (strcmp(dot, ".py") == 0)	 return "text/x-python";
	if (strcmp(dot, ".js") == 0)	 return "text/javascript";
	if (strcmp(dot, ".ts") == 0)	 return "text/typescript";
	if (strcmp(dot, ".c") == 0)	 return "text/x-c";
	if (strcmp(dot, ".h") == 0)	 return "text/x-c";
	if (strcmp(dot, ".sql") == 0) return "text/x-sql";
	if (strcmp(dot, ".java") == 0) return "text/x-java";
	if (strcmp(dot, ".go") == 0)	 return "text/x-go";
	if (strcmp(dot, ".rs") == 0)	 return "text/x-rust";
	if (strcmp(dot, ".rb") == 0)	 return "text/x-ruby";
	if (strcmp(dot, ".sh") == 0)	 return "text/x-shellscript";
	if (strcmp(dot, ".md") == 0)	 return "text/markdown";
	if (strcmp(dot, ".json") == 0) return "application/json";
	if (strcmp(dot, ".yaml") == 0 || strcmp(dot, ".yml") == 0)
		return "text/yaml";
	if (strcmp(dot, ".xml") == 0) return "text/xml";
	if (strcmp(dot, ".html") == 0) return "text/html";
	if (strcmp(dot, ".css") == 0) return "text/css";
	if (strcmp(dot, ".csv") == 0) return "text/csv";
	if (strcmp(dot, ".txt") == 0) return "text/plain";
	if (strcmp(dot, ".png") == 0) return "image/png";
	if (strcmp(dot, ".jpg") == 0 || strcmp(dot, ".jpeg") == 0)
		return "image/jpeg";
	if (strcmp(dot, ".gif") == 0) return "image/gif";
	if (strcmp(dot, ".pdf") == 0) return "application/pdf";
	if (strcmp(dot, ".zip") == 0) return "application/zip";
	if (strcmp(dot, ".tar") == 0) return "application/x-tar";

	return "application/octet-stream";
}

/* ================================================================
 * SQL-CALLABLE FUNCTIONS
 * ================================================================ */

/*
 * codefs_write(path, content, branch DEFAULT 'main', commit_msg DEFAULT NULL)
 * -> bigint (file_id)
 *
 * Content is ONLY stored as data. Never executed as SQL.
 */
Datum
codefs_write(PG_FUNCTION_ARGS)
{
	text	   *path_text;
	text	   *content_text;
	char	   *path;
	char	   *content;
	char	   *branch;
	char	   *commit_msg = NULL;
	int			content_len;
	bool		binary;
	char		checksum[CODEFS_SHA256_LEN];
	const char *mime;
	StringInfoData sql;
	int			ret;
	int64		file_id = 0;
	int64		lock_key;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("codefs: path and content cannot be NULL")));

	path_text = PG_GETARG_TEXT_PP(0);
	content_text = PG_GETARG_TEXT_PP(1);
	path = text_to_cstring(path_text);
	content = text_to_cstring(content_text);
	content_len = strlen(content);

	branch = PG_ARGISNULL(2) ? "main" : text_to_cstring(PG_GETARG_TEXT_PP(2));
	if (!PG_ARGISNULL(3))
		commit_msg = text_to_cstring(PG_GETARG_TEXT_PP(3));

	codefs_validate_path(path);

	binary = is_binary_content(content, content_len);
	mime = guess_mime_type(path);
	codefs_compute_sha256(content, content_len, checksum);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* Enforce agent_write_scope */
	check_agent_scope_policy(path);

	/* Enforce branch-per-agent policy */
	branch = check_branch_policy(branch);

	/* Advisory lock on path+branch */
	lock_key = get_advisory_lock_key(path, branch);
	initStringInfo(&sql);
	appendStringInfo(&sql,
		"SELECT pg_try_advisory_xact_lock(%lld)",
		(long long) lock_key);
	ret = SPI_execute(sql.data, true, 1);
	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		bool	isnull;
		Datum	d = SPI_getbinval(SPI_tuptable->vals[0],
								  SPI_tuptable->tupdesc, 1, &isnull);
		if (isnull || !DatumGetBool(d))
			ereport(ERROR,
					(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					 errmsg("codefs: could not acquire lock on '%s' branch '%s'",
							path, branch)));
	}

	/* Upsert into codefs_files */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
		"INSERT INTO codefs_files "
		"(path, content, size_bytes, mime_type, is_binary, checksum, "
		" branch, created_by, updated_at) "
		"VALUES (%s, %s, %d, %s, %s, %s, %s, current_user, now()) "
		"ON CONFLICT (path, branch) DO UPDATE SET "
		"  content = EXCLUDED.content, "
		"  size_bytes = EXCLUDED.size_bytes, "
		"  mime_type = EXCLUDED.mime_type, "
		"  is_binary = EXCLUDED.is_binary, "
		"  checksum = EXCLUDED.checksum, "
		"  updated_at = now(), "
		"  is_deleted = false "
		"RETURNING file_id",
		quote_literal_cstr(path),
		quote_literal_cstr(content),
		content_len,
		quote_literal_cstr(mime),
		binary ? "true" : "false",
		quote_literal_cstr(checksum),
		quote_literal_cstr(branch));

	ret = SPI_execute(sql.data, false, 1);
	if (ret == SPI_OK_INSERT_RETURNING && SPI_processed > 0)
	{
		bool	isnull;

		file_id = DatumGetInt64(
			SPI_getbinval(SPI_tuptable->vals[0],
						  SPI_tuptable->tupdesc, 1, &isnull));
	}

	/* Create version entry */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
		"INSERT INTO codefs_versions "
		"(file_id, version_num, content, checksum, size_bytes, "
		" commit_msg, committed_by) "
		"SELECT %lld, COALESCE(MAX(version_num), 0) + 1, "
		"  %s, %s, %d, %s, current_user "
		"FROM codefs_versions WHERE file_id = %lld",
		(long long) file_id,
		quote_literal_cstr(content),
		quote_literal_cstr(checksum),
		content_len,
		commit_msg ? quote_literal_cstr(commit_msg) : "NULL",
		(long long) file_id);

	SPI_execute(sql.data, false, 0);

	pfree(sql.data);
	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_INT64(file_id);
}

/*
 * codefs_read(path, branch DEFAULT 'main', version DEFAULT NULL)
 * -> text
 */
Datum
codefs_read(PG_FUNCTION_ARGS)
{
	char	   *path;
	char	   *branch;
	StringInfoData sql;
	int			ret;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("codefs: path cannot be NULL")));

	path = text_to_cstring(PG_GETARG_TEXT_PP(0));
	branch = PG_ARGISNULL(1) ? "main" : text_to_cstring(PG_GETARG_TEXT_PP(1));

	codefs_validate_path(path);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	initStringInfo(&sql);

	if (!PG_ARGISNULL(2))
	{
		/* Read specific version */
		int		version = PG_GETARG_INT32(2);

		appendStringInfo(&sql,
			"SELECT v.content FROM codefs_versions v "
			"JOIN codefs_files f ON v.file_id = f.file_id "
			"WHERE f.path = %s AND f.branch = %s "
			"AND v.version_num = %d AND v.is_archived = false",
			quote_literal_cstr(path),
			quote_literal_cstr(branch),
			version);
	}
	else
	{
		/* Read current version */
		appendStringInfo(&sql,
			"SELECT content FROM codefs_files "
			"WHERE path = %s AND branch = %s AND is_deleted = false",
			quote_literal_cstr(path),
			quote_literal_cstr(branch));
	}

	ret = SPI_execute(sql.data, true, 1);

	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		char   *val = SPI_getvalue(SPI_tuptable->vals[0],
								   SPI_tuptable->tupdesc, 1);
		text   *result;

		if (val)
		{
			result = cstring_to_text(val);
			pfree(sql.data);
			PopActiveSnapshot();
			SPI_finish();
			PG_RETURN_TEXT_P(result);
		}
	}

	pfree(sql.data);
	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_NULL();
}

/*
 * codefs_delete(path, branch DEFAULT 'main', commit_msg DEFAULT NULL)
 * -> boolean
 */
Datum
codefs_delete(PG_FUNCTION_ARGS)
{
	char	   *path;
	char	   *branch;
	char	   *commit_msg = NULL;
	StringInfoData sql;
	int			ret;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("codefs: path cannot be NULL")));

	path = text_to_cstring(PG_GETARG_TEXT_PP(0));
	branch = PG_ARGISNULL(1) ? "main" : text_to_cstring(PG_GETARG_TEXT_PP(1));
	if (!PG_ARGISNULL(2))
		commit_msg = text_to_cstring(PG_GETARG_TEXT_PP(2));

	codefs_validate_path(path);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	check_agent_scope_policy(path);
	branch = check_branch_policy(branch);

	/* Soft delete */
	initStringInfo(&sql);
	appendStringInfo(&sql,
		"UPDATE codefs_files SET is_deleted = true, updated_at = now() "
		"WHERE path = %s AND branch = %s AND is_deleted = false "
		"RETURNING file_id",
		quote_literal_cstr(path),
		quote_literal_cstr(branch));

	ret = SPI_execute(sql.data, false, 1);

	if (ret == SPI_OK_UPDATE_RETURNING && SPI_processed > 0)
	{
		bool	isnull;
		int64	fid = DatumGetInt64(
			SPI_getbinval(SPI_tuptable->vals[0],
						  SPI_tuptable->tupdesc, 1, &isnull));

		/* Record deletion in version history */
		resetStringInfo(&sql);
		appendStringInfo(&sql,
			"INSERT INTO codefs_versions "
			"(file_id, version_num, content, checksum, size_bytes, "
			" commit_msg, committed_by) "
			"SELECT %lld, COALESCE(MAX(version_num), 0) + 1, "
			"  NULL, NULL, 0, %s, current_user "
			"FROM codefs_versions WHERE file_id = %lld",
			(long long) fid,
			commit_msg ? quote_literal_cstr(commit_msg)
					   : "'file deleted'",
			(long long) fid);

		SPI_execute(sql.data, false, 0);

		pfree(sql.data);
		PopActiveSnapshot();
		SPI_finish();
		PG_RETURN_BOOL(true);
	}

	pfree(sql.data);
	PopActiveSnapshot();
	SPI_finish();
	PG_RETURN_BOOL(false);
}

/*
 * codefs_rename(old_path, new_path, branch DEFAULT 'main')
 * -> boolean
 */
Datum
codefs_rename(PG_FUNCTION_ARGS)
{
	char	   *old_path;
	char	   *new_path;
	char	   *branch;
	StringInfoData sql;
	int			ret;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("codefs: paths cannot be NULL")));

	old_path = text_to_cstring(PG_GETARG_TEXT_PP(0));
	new_path = text_to_cstring(PG_GETARG_TEXT_PP(1));
	branch = PG_ARGISNULL(2) ? "main" : text_to_cstring(PG_GETARG_TEXT_PP(2));

	codefs_validate_path(old_path);
	codefs_validate_path(new_path);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	check_agent_scope_policy(old_path);
	check_agent_scope_policy(new_path);
	branch = check_branch_policy(branch);

	initStringInfo(&sql);
	appendStringInfo(&sql,
		"UPDATE codefs_files SET path = %s, "
		"  mime_type = %s, updated_at = now() "
		"WHERE path = %s AND branch = %s AND is_deleted = false",
		quote_literal_cstr(new_path),
		quote_literal_cstr(guess_mime_type(new_path)),
		quote_literal_cstr(old_path),
		quote_literal_cstr(branch));

	ret = SPI_execute(sql.data, false, 0);

	pfree(sql.data);
	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_BOOL(ret == SPI_OK_UPDATE && SPI_processed > 0);
}

/*
 * codefs_list(branch DEFAULT 'main', pattern DEFAULT '*')
 * -> TABLE(path, size_bytes, mime_type, updated_at, created_by)
 */
Datum
codefs_list(PG_FUNCTION_ARGS)
{
	ReturnSetInfo  *rsinfo;
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	char	   *branch;
	char	   *pattern;
	StringInfoData sql;
	int			ret;
	uint64		i;

	branch = PG_ARGISNULL(0) ? "main" : text_to_cstring(PG_GETARG_TEXT_PP(0));
	pattern = PG_ARGISNULL(1) ? "*" : text_to_cstring(PG_GETARG_TEXT_PP(1));

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
	rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	tupstore = rsinfo->setResult;
	tupdesc = rsinfo->setDesc;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* Convert glob pattern to SQL LIKE */
	initStringInfo(&sql);
	if (strcmp(pattern, "*") == 0)
	{
		appendStringInfo(&sql,
			"SELECT path, size_bytes, mime_type, updated_at, created_by "
			"FROM codefs_files WHERE branch = %s AND is_deleted = false "
			"ORDER BY path",
			quote_literal_cstr(branch));
	}
	else
	{
		/* Convert * to %% and ? to _ for LIKE */
		StringInfoData like_pat;
		const char *p;

		initStringInfo(&like_pat);
		for (p = pattern; *p; p++)
		{
			if (*p == '*')
				appendStringInfoChar(&like_pat, '%');
			else if (*p == '?')
				appendStringInfoChar(&like_pat, '_');
			else if (*p == '%' || *p == '_')
			{
				appendStringInfoChar(&like_pat, '\\');
				appendStringInfoChar(&like_pat, *p);
			}
			else
				appendStringInfoChar(&like_pat, *p);
		}

		appendStringInfo(&sql,
			"SELECT path, size_bytes, mime_type, updated_at, created_by "
			"FROM codefs_files WHERE branch = %s AND is_deleted = false "
			"AND path LIKE %s ORDER BY path",
			quote_literal_cstr(branch),
			quote_literal_cstr(like_pat.data));
		pfree(like_pat.data);
	}

	ret = SPI_execute(sql.data, true, 0);

	if (ret == SPI_OK_SELECT)
	{
		for (i = 0; i < SPI_processed; i++)
		{
			Datum	values[5];
			bool	nulls[5] = {false, false, false, false, false};
			bool	isnull;

			values[0] = SPI_getbinval(SPI_tuptable->vals[i],
									  SPI_tuptable->tupdesc, 1, &isnull);
			values[1] = SPI_getbinval(SPI_tuptable->vals[i],
									  SPI_tuptable->tupdesc, 2, &isnull);
			if (isnull) { nulls[1] = true; values[1] = (Datum) 0; }
			values[2] = SPI_getbinval(SPI_tuptable->vals[i],
									  SPI_tuptable->tupdesc, 3, &isnull);
			if (isnull) { nulls[2] = true; values[2] = (Datum) 0; }
			values[3] = SPI_getbinval(SPI_tuptable->vals[i],
									  SPI_tuptable->tupdesc, 4, &isnull);
			if (isnull) { nulls[3] = true; values[3] = (Datum) 0; }
			values[4] = SPI_getbinval(SPI_tuptable->vals[i],
									  SPI_tuptable->tupdesc, 5, &isnull);
			if (isnull) { nulls[4] = true; values[4] = (Datum) 0; }

			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}
	}

	pfree(sql.data);
	PopActiveSnapshot();
	SPI_finish();

	return (Datum) 0;
}

/*
 * codefs_commit(branch, message, paths DEFAULT NULL)
 * -> bigint (number of versions stamped)
 */
Datum
codefs_commit(PG_FUNCTION_ARGS)
{
	char	   *branch;
	char	   *message;
	StringInfoData sql;
	int			ret;
	int64		count = 0;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("codefs: branch and message cannot be NULL")));

	branch = text_to_cstring(PG_GETARG_TEXT_PP(0));
	message = text_to_cstring(PG_GETARG_TEXT_PP(1));

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	initStringInfo(&sql);

	if (!PG_ARGISNULL(2))
	{
		/* Commit specific paths */
		appendStringInfo(&sql,
			"UPDATE codefs_versions v SET commit_msg = %s "
			"FROM codefs_files f "
			"WHERE v.file_id = f.file_id AND f.branch = %s "
			"AND v.commit_msg IS NULL "
			"AND f.path = ANY(%s)",
			quote_literal_cstr(message),
			quote_literal_cstr(branch),
			quote_literal_cstr(
				text_to_cstring(PG_GETARG_TEXT_PP(2))));
	}
	else
	{
		/* Commit all uncommitted versions on branch */
		appendStringInfo(&sql,
			"UPDATE codefs_versions v SET commit_msg = %s "
			"FROM codefs_files f "
			"WHERE v.file_id = f.file_id AND f.branch = %s "
			"AND v.commit_msg IS NULL",
			quote_literal_cstr(message),
			quote_literal_cstr(branch));
	}

	ret = SPI_execute(sql.data, false, 0);
	if (ret == SPI_OK_UPDATE)
		count = SPI_processed;

	pfree(sql.data);
	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_INT64(count);
}

/*
 * codefs_history(path, branch DEFAULT 'main', max_entries DEFAULT 20)
 * -> TABLE(version_num, commit_msg, committed_by, committed_at, size_bytes)
 */
Datum
codefs_history(PG_FUNCTION_ARGS)
{
	ReturnSetInfo  *rsinfo;
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	char	   *path;
	char	   *branch;
	int			max_entries;
	StringInfoData sql;
	int			ret;
	uint64		i;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("codefs: path cannot be NULL")));

	path = text_to_cstring(PG_GETARG_TEXT_PP(0));
	branch = PG_ARGISNULL(1) ? "main" : text_to_cstring(PG_GETARG_TEXT_PP(1));
	max_entries = PG_ARGISNULL(2) ? 20 : PG_GETARG_INT32(2);

	codefs_validate_path(path);

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
	rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	tupstore = rsinfo->setResult;
	tupdesc = rsinfo->setDesc;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	initStringInfo(&sql);
	appendStringInfo(&sql,
		"SELECT v.version_num, v.commit_msg, v.committed_by, "
		"  v.committed_at, v.size_bytes "
		"FROM codefs_versions v "
		"JOIN codefs_files f ON v.file_id = f.file_id "
		"WHERE f.path = %s AND f.branch = %s "
		"ORDER BY v.version_num DESC LIMIT %d",
		quote_literal_cstr(path),
		quote_literal_cstr(branch),
		max_entries);

	ret = SPI_execute(sql.data, true, 0);

	if (ret == SPI_OK_SELECT)
	{
		for (i = 0; i < SPI_processed; i++)
		{
			Datum	values[5];
			bool	nulls[5];
			int		col;

			for (col = 0; col < 5; col++)
			{
				values[col] = SPI_getbinval(SPI_tuptable->vals[i],
											SPI_tuptable->tupdesc,
											col + 1, &nulls[col]);
				if (nulls[col])
					values[col] = (Datum) 0;
			}

			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}
	}

	pfree(sql.data);
	PopActiveSnapshot();
	SPI_finish();

	return (Datum) 0;
}

/*
 * codefs_diff(path, version_a, version_b, branch DEFAULT 'main')
 * -> text (unified diff)
 */
Datum
codefs_diff(PG_FUNCTION_ARGS)
{
	char	   *path;
	int			va, vb;
	char	   *branch;
	StringInfoData sql;
	int			ret;
	char	   *content_a = NULL;
	char	   *content_b = NULL;
	StringInfoData result;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("codefs: path and versions cannot be NULL")));

	path = text_to_cstring(PG_GETARG_TEXT_PP(0));
	va = PG_GETARG_INT32(1);
	vb = PG_GETARG_INT32(2);
	branch = PG_ARGISNULL(3) ? "main" : text_to_cstring(PG_GETARG_TEXT_PP(3));

	codefs_validate_path(path);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* Fetch version A */
	initStringInfo(&sql);
	appendStringInfo(&sql,
		"SELECT v.content FROM codefs_versions v "
		"JOIN codefs_files f ON v.file_id = f.file_id "
		"WHERE f.path = %s AND f.branch = %s AND v.version_num = %d",
		quote_literal_cstr(path),
		quote_literal_cstr(branch), va);

	ret = SPI_execute(sql.data, true, 1);
	if (ret == SPI_OK_SELECT && SPI_processed > 0)
		content_a = SPI_getvalue(SPI_tuptable->vals[0],
								 SPI_tuptable->tupdesc, 1);
	if (!content_a)
		content_a = "";

	/* Fetch version B */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
		"SELECT v.content FROM codefs_versions v "
		"JOIN codefs_files f ON v.file_id = f.file_id "
		"WHERE f.path = %s AND f.branch = %s AND v.version_num = %d",
		quote_literal_cstr(path),
		quote_literal_cstr(branch), vb);

	ret = SPI_execute(sql.data, true, 1);
	if (ret == SPI_OK_SELECT && SPI_processed > 0)
		content_b = SPI_getvalue(SPI_tuptable->vals[0],
								 SPI_tuptable->tupdesc, 1);
	if (!content_b)
		content_b = "";

	PopActiveSnapshot();
	SPI_finish();

	/* Simple line-by-line diff */
	initStringInfo(&result);
	appendStringInfo(&result, "--- %s (v%d)\n+++ %s (v%d)\n",
					 path, va, path, vb);

	{
		char   *a_copy = pstrdup(content_a);
		char   *b_copy = pstrdup(content_b);
		char   *line_a, *line_b;
		char   *save_a, *save_b;
		int		lineno = 0;

		line_a = strtok_r(a_copy, "\n", &save_a);
		line_b = strtok_r(b_copy, "\n", &save_b);

		while (line_a || line_b)
		{
			lineno++;

			if (line_a && line_b)
			{
				if (strcmp(line_a, line_b) != 0)
				{
					appendStringInfo(&result, "@@ line %d @@\n", lineno);
					appendStringInfo(&result, "-%s\n", line_a);
					appendStringInfo(&result, "+%s\n", line_b);
				}
			}
			else if (line_a && !line_b)
			{
				appendStringInfo(&result, "-%s\n", line_a);
			}
			else
			{
				appendStringInfo(&result, "+%s\n", line_b);
			}

			if (line_a) line_a = strtok_r(NULL, "\n", &save_a);
			if (line_b) line_b = strtok_r(NULL, "\n", &save_b);
		}

		pfree(a_copy);
		pfree(b_copy);
	}

	pfree(sql.data);
	PG_RETURN_TEXT_P(cstring_to_text(result.data));
}

/*
 * codefs_revert(path, to_version, branch DEFAULT 'main')
 * -> boolean
 */
Datum
codefs_revert(PG_FUNCTION_ARGS)
{
	char	   *path;
	int			to_version;
	char	   *branch;
	StringInfoData sql;
	int			ret;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("codefs: path and version cannot be NULL")));

	path = text_to_cstring(PG_GETARG_TEXT_PP(0));
	to_version = PG_GETARG_INT32(1);
	branch = PG_ARGISNULL(2) ? "main" : text_to_cstring(PG_GETARG_TEXT_PP(2));

	codefs_validate_path(path);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	check_agent_scope_policy(path);
	branch = check_branch_policy(branch);

	/* Get content from target version and write as new current */
	initStringInfo(&sql);
	appendStringInfo(&sql,
		"UPDATE codefs_files f SET "
		"  content = v.content, "
		"  size_bytes = v.size_bytes, "
		"  checksum = v.checksum, "
		"  updated_at = now() "
		"FROM codefs_versions v "
		"WHERE v.file_id = f.file_id AND f.path = %s "
		"AND f.branch = %s AND v.version_num = %d",
		quote_literal_cstr(path),
		quote_literal_cstr(branch),
		to_version);

	ret = SPI_execute(sql.data, false, 0);

	if (ret == SPI_OK_UPDATE && SPI_processed > 0)
	{
		/* Record revert as a new version */
		resetStringInfo(&sql);
		appendStringInfo(&sql,
			"INSERT INTO codefs_versions "
			"(file_id, version_num, content, checksum, size_bytes, "
			" commit_msg, committed_by) "
			"SELECT f.file_id, COALESCE(MAX(v2.version_num), 0) + 1, "
			"  f.content, f.checksum, f.size_bytes, "
			"  'reverted to version %d', current_user "
			"FROM codefs_files f "
			"LEFT JOIN codefs_versions v2 ON v2.file_id = f.file_id "
			"WHERE f.path = %s AND f.branch = %s "
			"GROUP BY f.file_id, f.content, f.checksum, f.size_bytes",
			to_version,
			quote_literal_cstr(path),
			quote_literal_cstr(branch));

		SPI_execute(sql.data, false, 0);

		pfree(sql.data);
		PopActiveSnapshot();
		SPI_finish();
		PG_RETURN_BOOL(true);
	}

	pfree(sql.data);
	PopActiveSnapshot();
	SPI_finish();
	PG_RETURN_BOOL(false);
}

/* ================================================================
 * BRANCHING
 * ================================================================ */

/*
 * codefs_branch_create(name, from_branch DEFAULT 'main',
 *                      agent_name DEFAULT NULL)
 * -> text (branch name)
 */
Datum
codefs_branch_create(PG_FUNCTION_ARGS)
{
	char	   *name;
	char	   *from_branch;
	char	   *agent_name = NULL;
	bool		is_agent = false;
	StringInfoData sql;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("codefs: branch name cannot be NULL")));

	name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	from_branch = PG_ARGISNULL(1) ? "main"
		: text_to_cstring(PG_GETARG_TEXT_PP(1));

	if (!PG_ARGISNULL(2))
	{
		agent_name = text_to_cstring(PG_GETARG_TEXT_PP(2));
		is_agent = true;
	}

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* Create branch record */
	initStringInfo(&sql);
	appendStringInfo(&sql,
		"INSERT INTO codefs_branches "
		"(name, parent_branch, is_agent, agent_name, status) "
		"VALUES (%s, %s, %s, %s, 'active')",
		quote_literal_cstr(name),
		quote_literal_cstr(from_branch),
		is_agent ? "true" : "false",
		agent_name ? quote_literal_cstr(agent_name) : "NULL");

	SPI_execute(sql.data, false, 0);

	/* Copy files from parent branch (copy-on-write: copies references) */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
		"INSERT INTO codefs_files "
		"(path, content, size_bytes, mime_type, is_binary, checksum, "
		" branch, created_by, metadata) "
		"SELECT path, content, size_bytes, mime_type, is_binary, checksum, "
		"  %s, created_by, metadata "
		"FROM codefs_files "
		"WHERE branch = %s AND is_deleted = false",
		quote_literal_cstr(name),
		quote_literal_cstr(from_branch));

	SPI_execute(sql.data, false, 0);

	pfree(sql.data);
	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_TEXT_P(cstring_to_text(name));
}

/*
 * codefs_branch_list()
 * -> TABLE(name, parent_branch, created_by, created_at, is_agent, status)
 */
Datum
codefs_branch_list(PG_FUNCTION_ARGS)
{
	ReturnSetInfo  *rsinfo;
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	int			ret;
	uint64		i;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
	rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	tupstore = rsinfo->setResult;
	tupdesc = rsinfo->setDesc;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(
		"SELECT name, parent_branch, created_by, created_at, "
		"  is_agent, status "
		"FROM codefs_branches ORDER BY created_at",
		true, 0);

	if (ret == SPI_OK_SELECT)
	{
		for (i = 0; i < SPI_processed; i++)
		{
			Datum	values[6];
			bool	nulls[6];
			int		col;

			for (col = 0; col < 6; col++)
			{
				values[col] = SPI_getbinval(SPI_tuptable->vals[i],
											SPI_tuptable->tupdesc,
											col + 1, &nulls[col]);
				if (nulls[col])
					values[col] = (Datum) 0;
			}

			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}
	}

	PopActiveSnapshot();
	SPI_finish();

	return (Datum) 0;
}

/*
 * codefs_branch_merge(source_branch, target_branch DEFAULT 'main')
 * -> TABLE(path, action, status)
 */
Datum
codefs_branch_merge(PG_FUNCTION_ARGS)
{
	ReturnSetInfo  *rsinfo;
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	char	   *source;
	char	   *target;
	StringInfoData sql;
	int			ret;
	uint64		i;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("codefs: source branch cannot be NULL")));

	source = text_to_cstring(PG_GETARG_TEXT_PP(0));
	target = PG_ARGISNULL(1) ? "main" : text_to_cstring(PG_GETARG_TEXT_PP(1));

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
	rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	tupstore = rsinfo->setResult;
	tupdesc = rsinfo->setDesc;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* Check require_human_merge policy */
	ret = SPI_execute(
		"SELECT require_human_merge FROM codefs_policies "
		"WHERE policy_name = 'default' LIMIT 1",
		true, 1);

	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		bool	isnull;
		Datum	d = SPI_getbinval(SPI_tuptable->vals[0],
								  SPI_tuptable->tupdesc, 1, &isnull);
		if (!isnull && DatumGetBool(d))
		{
			/*
			 * Check if current user is explicitly a member of
			 * codefs_agent (not via superuser bypass).
			 */
			ret = SPI_execute(
				"SELECT EXISTS("
				"  SELECT 1 FROM pg_auth_members m "
				"  JOIN pg_roles r ON r.oid = m.roleid "
				"  WHERE r.rolname = 'codefs_agent' "
				"  AND m.member = (SELECT oid FROM pg_roles "
				"    WHERE rolname = current_user))",
				true, 1);
			if (ret == SPI_OK_SELECT && SPI_processed > 0)
			{
				Datum	d2 = SPI_getbinval(SPI_tuptable->vals[0],
										   SPI_tuptable->tupdesc, 1, &isnull);
				if (!isnull && DatumGetBool(d2))
				{
					PopActiveSnapshot();
					SPI_finish();
					ereport(ERROR,
							(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
							 errmsg("codefs: agents cannot merge; "
									"require_human_merge is enabled")));
				}
			}
		}
	}

	/*
	 * Find files that differ between source and target.
	 * Files in source not in target = 'add'
	 * Files in both with different checksum = 'modify' or 'conflict'
	 * Files deleted in source = 'delete'
	 */
	initStringInfo(&sql);
	appendStringInfo(&sql,
		"SELECT s.path, "
		"  CASE "
		"    WHEN t.file_id IS NULL THEN 'add' "
		"    WHEN s.is_deleted THEN 'delete' "
		"    WHEN s.checksum != t.checksum AND t.updated_at > s.created_at "
		"      THEN 'conflict' "
		"    WHEN s.checksum != t.checksum THEN 'modify' "
		"    ELSE 'unchanged' "
		"  END AS action "
		"FROM codefs_files s "
		"LEFT JOIN codefs_files t ON s.path = t.path AND t.branch = %s "
		"  AND t.is_deleted = false "
		"WHERE s.branch = %s "
		"ORDER BY s.path",
		quote_literal_cstr(target),
		quote_literal_cstr(source));

	ret = SPI_execute(sql.data, true, 0);

	if (ret == SPI_OK_SELECT)
	{
		for (i = 0; i < SPI_processed; i++)
		{
			char   *fpath = SPI_getvalue(SPI_tuptable->vals[i],
										  SPI_tuptable->tupdesc, 1);
			char   *action = SPI_getvalue(SPI_tuptable->vals[i],
										   SPI_tuptable->tupdesc, 2);
			Datum	values[3];
			bool	nulls[3] = {false, false, false};
			char   *status_str = "ok";

			if (strcmp(action, "unchanged") == 0)
				continue;

			if (strcmp(action, "conflict") == 0)
			{
				status_str = "skipped: conflict";
			}
			else if (strcmp(action, "add") == 0 ||
					 strcmp(action, "modify") == 0)
			{
				/* Copy content from source to target */
				StringInfoData merge_sql;

				initStringInfo(&merge_sql);
				appendStringInfo(&merge_sql,
					"INSERT INTO codefs_files "
					"(path, content, size_bytes, mime_type, is_binary, "
					" checksum, branch, created_by, metadata) "
					"SELECT path, content, size_bytes, mime_type, is_binary, "
					"  checksum, %s, current_user, metadata "
					"FROM codefs_files "
					"WHERE path = %s AND branch = %s "
					"ON CONFLICT (path, branch) DO UPDATE SET "
					"  content = EXCLUDED.content, "
					"  size_bytes = EXCLUDED.size_bytes, "
					"  checksum = EXCLUDED.checksum, "
					"  updated_at = now()",
					quote_literal_cstr(target),
					quote_literal_cstr(fpath),
					quote_literal_cstr(source));

				SPI_execute(merge_sql.data, false, 0);
				pfree(merge_sql.data);
				status_str = "merged";
			}
			else if (strcmp(action, "delete") == 0)
			{
				StringInfoData del_sql;

				initStringInfo(&del_sql);
				appendStringInfo(&del_sql,
					"UPDATE codefs_files SET is_deleted = true, "
					"  updated_at = now() "
					"WHERE path = %s AND branch = %s",
					quote_literal_cstr(fpath),
					quote_literal_cstr(target));

				SPI_execute(del_sql.data, false, 0);
				pfree(del_sql.data);
				status_str = "deleted";
			}

			values[0] = CStringGetTextDatum(fpath);
			values[1] = CStringGetTextDatum(action);
			values[2] = CStringGetTextDatum(status_str);
			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}
	}

	/* Mark source branch as merged */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
		"UPDATE codefs_branches SET status = 'merged', "
		"  merged_at = now(), merged_by = current_user "
		"WHERE name = %s",
		quote_literal_cstr(source));
	SPI_execute(sql.data, false, 0);

	pfree(sql.data);
	PopActiveSnapshot();
	SPI_finish();

	return (Datum) 0;
}

/*
 * codefs_branch_delete(name) -> boolean
 */
Datum
codefs_branch_delete(PG_FUNCTION_ARGS)
{
	char	   *name;
	StringInfoData sql;
	int			ret;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("codefs: branch name cannot be NULL")));

	name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (strcmp(name, "main") == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("codefs: cannot delete 'main' branch")));

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	initStringInfo(&sql);
	appendStringInfo(&sql,
		"UPDATE codefs_branches SET status = 'abandoned' "
		"WHERE name = %s AND status = 'active'",
		quote_literal_cstr(name));

	ret = SPI_execute(sql.data, false, 0);

	/* Also soft-delete files on this branch */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
		"UPDATE codefs_files SET is_deleted = true "
		"WHERE branch = %s",
		quote_literal_cstr(name));
	SPI_execute(sql.data, false, 0);

	pfree(sql.data);
	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_BOOL(ret == SPI_OK_UPDATE && SPI_processed > 0);
}

/* ================================================================
 * LOCKING
 * ================================================================ */

/*
 * codefs_lock_file(path, branch DEFAULT 'main', timeout_secs DEFAULT 5)
 * -> boolean
 */
Datum
codefs_lock_file(PG_FUNCTION_ARGS)
{
	char	   *path;
	char	   *branch;
	int			timeout_secs;
	int64		key;
	StringInfoData sql;
	int			ret;
	bool		acquired = false;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("codefs: path cannot be NULL")));

	path = text_to_cstring(PG_GETARG_TEXT_PP(0));
	branch = PG_ARGISNULL(1) ? "main" : text_to_cstring(PG_GETARG_TEXT_PP(1));
	timeout_secs = PG_ARGISNULL(2) ? 5 : PG_GETARG_INT32(2);

	codefs_validate_path(path);
	key = get_advisory_lock_key(path, branch);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* Try advisory lock */
	initStringInfo(&sql);
	appendStringInfo(&sql,
		"SELECT pg_try_advisory_lock(%lld)", (long long) key);

	ret = SPI_execute(sql.data, true, 1);
	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		bool	isnull;
		Datum	d = SPI_getbinval(SPI_tuptable->vals[0],
								  SPI_tuptable->tupdesc, 1, &isnull);
		acquired = !isnull && DatumGetBool(d);
	}

	if (acquired)
	{
		/* Record in codefs_locks for visibility */
		resetStringInfo(&sql);
		appendStringInfo(&sql,
			"INSERT INTO codefs_locks (file_id, branch, locked_by, lock_timeout) "
			"SELECT file_id, %s, current_user, interval '%d seconds' "
			"FROM codefs_files WHERE path = %s AND branch = %s "
			"AND is_deleted = false "
			"ON CONFLICT (file_id, branch) DO UPDATE SET "
			"  locked_by = current_user, locked_at = now()",
			quote_literal_cstr(branch),
			timeout_secs,
			quote_literal_cstr(path),
			quote_literal_cstr(branch));

		SPI_execute(sql.data, false, 0);
	}

	pfree(sql.data);
	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_BOOL(acquired);
}

/*
 * codefs_unlock_file(path, branch DEFAULT 'main') -> boolean
 */
Datum
codefs_unlock_file(PG_FUNCTION_ARGS)
{
	char	   *path;
	char	   *branch;
	int64		key;
	StringInfoData sql;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("codefs: path cannot be NULL")));

	path = text_to_cstring(PG_GETARG_TEXT_PP(0));
	branch = PG_ARGISNULL(1) ? "main" : text_to_cstring(PG_GETARG_TEXT_PP(1));

	codefs_validate_path(path);
	key = get_advisory_lock_key(path, branch);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* Release advisory lock */
	initStringInfo(&sql);
	appendStringInfo(&sql,
		"SELECT pg_advisory_unlock(%lld)", (long long) key);
	SPI_execute(sql.data, true, 0);

	/* Remove from tracking table */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
		"DELETE FROM codefs_locks "
		"WHERE file_id IN (SELECT file_id FROM codefs_files "
		"  WHERE path = %s AND branch = %s) "
		"AND branch = %s AND locked_by = current_user",
		quote_literal_cstr(path),
		quote_literal_cstr(branch),
		quote_literal_cstr(branch));

	SPI_execute(sql.data, false, 0);

	pfree(sql.data);
	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_BOOL(true);
}

/* ================================================================
 * SEARCH
 * ================================================================ */

/*
 * codefs_search(pattern, branch DEFAULT 'main', max_results DEFAULT 100)
 * -> TABLE(path, line_num, line_text)
 */
Datum
codefs_search(PG_FUNCTION_ARGS)
{
	ReturnSetInfo  *rsinfo;
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	char	   *pattern;
	char	   *branch;
	int			max_results;
	StringInfoData sql;
	int			ret;
	uint64		i;
	int			total_matches = 0;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("codefs: search pattern cannot be NULL")));

	pattern = text_to_cstring(PG_GETARG_TEXT_PP(0));
	branch = PG_ARGISNULL(1) ? "main" : text_to_cstring(PG_GETARG_TEXT_PP(1));
	max_results = PG_ARGISNULL(2) ? 100 : PG_GETARG_INT32(2);

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
	rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	tupstore = rsinfo->setResult;
	tupdesc = rsinfo->setDesc;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* Get files containing pattern (content is data, never executed) */
	initStringInfo(&sql);
	appendStringInfo(&sql,
		"SELECT path, content FROM codefs_files "
		"WHERE branch = %s AND is_deleted = false "
		"AND content IS NOT NULL "
		"AND content ~ %s",
		quote_literal_cstr(branch),
		quote_literal_cstr(pattern));

	ret = SPI_execute(sql.data, true, 0);

	if (ret == SPI_OK_SELECT)
	{
		for (i = 0; i < SPI_processed && total_matches < max_results; i++)
		{
			char   *fpath = SPI_getvalue(SPI_tuptable->vals[i],
										  SPI_tuptable->tupdesc, 1);
			char   *content = SPI_getvalue(SPI_tuptable->vals[i],
											SPI_tuptable->tupdesc, 2);

			if (fpath && content)
			{
				/* Scan lines for matches */
				char   *copy = pstrdup(content);
				char   *line;
				char   *saveptr;
				int		lineno = 0;

				for (line = strtok_r(copy, "\n", &saveptr);
					 line && total_matches < max_results;
					 line = strtok_r(NULL, "\n", &saveptr))
				{
					lineno++;

					if (strstr(line, pattern) != NULL)
					{
						Datum	values[3];
						bool	nulls[3] = {false, false, false};

						values[0] = CStringGetTextDatum(fpath);
						values[1] = Int32GetDatum(lineno);
						values[2] = CStringGetTextDatum(line);
						tuplestore_putvalues(tupstore, tupdesc,
											 values, nulls);
						total_matches++;
					}
				}

				pfree(copy);
			}
		}
	}

	pfree(sql.data);
	PopActiveSnapshot();
	SPI_finish();

	return (Datum) 0;
}

/* ================================================================
 * STATUS
 * ================================================================ */

/*
 * codefs_status() -> TABLE(setting_name, setting_value)
 */
Datum
codefs_status(PG_FUNCTION_ARGS)
{
	ReturnSetInfo  *rsinfo;
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
	rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	tupstore = rsinfo->setResult;
	tupdesc = rsinfo->setDesc;

	if (!codefs_shared)
	{
		Datum	values[2];
		bool	nulls[2] = {false, false};

		values[0] = CStringGetTextDatum("status");
		values[1] = CStringGetTextDatum(
			"not loaded via shared_preload_libraries");
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		return (Datum) 0;
	}

	LWLockAcquire(codefs_shared->lock, LW_SHARED);

	{
		Datum	values[2];
		bool	nulls[2] = {false, false};
		char	buf[128];

		/* sync_worker_active */
		values[0] = CStringGetTextDatum("sync_worker_active");
		values[1] = CStringGetTextDatum(
			codefs_shared->sync_worker_active ? "true" : "false");
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);

		/* policy_worker_active */
		values[0] = CStringGetTextDatum("policy_worker_active");
		values[1] = CStringGetTextDatum(
			codefs_shared->policy_worker_active ? "true" : "false");
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);

		/* last_sync_time */
		values[0] = CStringGetTextDatum("last_sync_time");
		if (codefs_shared->last_sync_time != 0)
		{
			char *ts = DatumGetCString(
				DirectFunctionCall1(timestamptz_out,
					TimestampTzGetDatum(codefs_shared->last_sync_time)));
			values[1] = CStringGetTextDatum(ts);
		}
		else
			values[1] = CStringGetTextDatum("never");
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);

		/* last_policy_run */
		values[0] = CStringGetTextDatum("last_policy_run");
		if (codefs_shared->last_policy_run != 0)
		{
			char *ts = DatumGetCString(
				DirectFunctionCall1(timestamptz_out,
					TimestampTzGetDatum(codefs_shared->last_policy_run)));
			values[1] = CStringGetTextDatum(ts);
		}
		else
			values[1] = CStringGetTextDatum("never");
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);

		/* files_synced */
		values[0] = CStringGetTextDatum("files_synced");
		snprintf(buf, sizeof(buf), "%d", codefs_shared->files_synced);
		values[1] = CStringGetTextDatum(buf);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);

		/* versions_archived */
		values[0] = CStringGetTextDatum("versions_archived");
		snprintf(buf, sizeof(buf), "%d", codefs_shared->versions_archived);
		values[1] = CStringGetTextDatum(buf);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);

		/* sync_worker_pid */
		values[0] = CStringGetTextDatum("sync_worker_pid");
		snprintf(buf, sizeof(buf), "%d", codefs_shared->sync_worker_pid);
		values[1] = CStringGetTextDatum(buf);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);

		/* policy_worker_pid */
		values[0] = CStringGetTextDatum("policy_worker_pid");
		snprintf(buf, sizeof(buf), "%d", codefs_shared->policy_worker_pid);
		values[1] = CStringGetTextDatum(buf);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	LWLockRelease(codefs_shared->lock);

	return (Datum) 0;
}

/* ================================================================
 * EXPORT / IMPORT
 * ================================================================ */

/*
 * codefs_export_to_disk(branch DEFAULT 'main', target_dir DEFAULT NULL)
 * -> TABLE(path, status)
 */
Datum
codefs_export_to_disk(PG_FUNCTION_ARGS)
{
	ReturnSetInfo  *rsinfo;
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	char	   *branch;
	char	   *target_dir;
	StringInfoData sql;
	int			ret;
	uint64		i;

	branch = PG_ARGISNULL(0) ? "main" : text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (PG_ARGISNULL(1))
	{
		/* Read from policy */
		target_dir = "/var/lib/alohadb/codefs/export";
	}
	else
		target_dir = text_to_cstring(PG_GETARG_TEXT_PP(1));

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
	rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	tupstore = rsinfo->setResult;
	tupdesc = rsinfo->setDesc;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	initStringInfo(&sql);
	appendStringInfo(&sql,
		"SELECT path, content FROM codefs_files "
		"WHERE branch = %s AND is_deleted = false "
		"AND content IS NOT NULL ORDER BY path",
		quote_literal_cstr(branch));

	ret = SPI_execute(sql.data, true, 0);

	if (ret == SPI_OK_SELECT)
	{
		for (i = 0; i < SPI_processed; i++)
		{
			char   *fpath = SPI_getvalue(SPI_tuptable->vals[i],
										  SPI_tuptable->tupdesc, 1);
			char   *content = SPI_getvalue(SPI_tuptable->vals[i],
											SPI_tuptable->tupdesc, 2);
			Datum	values[2];
			bool	nulls[2] = {false, false};
			char	full_path[CODEFS_MAX_PATH_LEN * 2];
			FILE   *fp;
			char   *dir_end;
			char	dir_buf[CODEFS_MAX_PATH_LEN * 2];

			if (!fpath || !content)
				continue;

			snprintf(full_path, sizeof(full_path), "%s/%s",
					 target_dir, fpath);

			/* Create parent directories */
			strncpy(dir_buf, full_path, sizeof(dir_buf) - 1);
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
				values[0] = CStringGetTextDatum(fpath);
				values[1] = CStringGetTextDatum("exported");
			}
			else
			{
				values[0] = CStringGetTextDatum(fpath);
				values[1] = CStringGetTextDatum("error: cannot write");
			}

			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}
	}

	pfree(sql.data);
	PopActiveSnapshot();
	SPI_finish();

	return (Datum) 0;
}

/*
 * codefs_import_from_disk(source_dir, branch DEFAULT 'main')
 * -> TABLE(path, status)
 *
 * Uses a helper query to list files (we can't easily walk dirs in C
 * without platform-specific code, so we shell out via pg_ls_dir-like approach).
 * For safety, we use a recursive directory walk in C.
 */

#include <dirent.h>
#include <sys/stat.h>

/* Forward declaration for recursive import */
static void
import_dir_recursive(const char *base_dir, const char *rel_prefix,
					 const char *branch, Tuplestorestate *tupstore,
					 TupleDesc tupdesc, int *count);

static void
import_dir_recursive(const char *base_dir, const char *rel_prefix,
					 const char *branch, Tuplestorestate *tupstore,
					 TupleDesc tupdesc, int *count)
{
	DIR			   *dir;
	struct dirent  *entry;
	char			full_path[CODEFS_MAX_PATH_LEN * 2];
	char			rel_path[CODEFS_MAX_PATH_LEN];

	dir = opendir(base_dir);
	if (!dir)
		return;

	while ((entry = readdir(dir)) != NULL)
	{
		struct stat st;

		if (strcmp(entry->d_name, ".") == 0 ||
			strcmp(entry->d_name, "..") == 0)
			continue;

		/* Skip hidden files/dirs */
		if (entry->d_name[0] == '.')
			continue;

		snprintf(full_path, sizeof(full_path), "%s/%s",
				 base_dir, entry->d_name);

		if (rel_prefix[0] == '\0')
			snprintf(rel_path, sizeof(rel_path), "%s", entry->d_name);
		else
			snprintf(rel_path, sizeof(rel_path), "%s/%s",
					 rel_prefix, entry->d_name);

		if (stat(full_path, &st) != 0)
			continue;

		if (S_ISDIR(st.st_mode))
		{
			import_dir_recursive(full_path, rel_path, branch,
								 tupstore, tupdesc, count);
		}
		else if (S_ISREG(st.st_mode))
		{
			/* Read file content */
			FILE   *fp = fopen(full_path, "r");
			if (fp)
			{
				StringInfoData content;
				char			buf[8192];
				size_t			nread;
				char			checksum[CODEFS_SHA256_LEN];
				const char	   *mime;
				bool			binary;
				StringInfoData	isql;
				Datum			values[2];
				bool			nulls[2] = {false, false};

				initStringInfo(&content);
				while ((nread = fread(buf, 1, sizeof(buf), fp)) > 0)
					appendBinaryStringInfo(&content, buf, nread);
				fclose(fp);

				binary = is_binary_content(content.data, content.len);
				mime = guess_mime_type(rel_path);
				codefs_compute_sha256(content.data, content.len, checksum);

				initStringInfo(&isql);
				appendStringInfo(&isql,
					"INSERT INTO codefs_files "
					"(path, content, size_bytes, mime_type, is_binary, "
					" checksum, branch, created_by) "
					"VALUES (%s, %s, %d, %s, %s, %s, %s, current_user) "
					"ON CONFLICT (path, branch) DO UPDATE SET "
					"  content = EXCLUDED.content, "
					"  size_bytes = EXCLUDED.size_bytes, "
					"  checksum = EXCLUDED.checksum, "
					"  updated_at = now()",
					quote_literal_cstr(rel_path),
					quote_literal_cstr(content.data),
					content.len,
					quote_literal_cstr(mime),
					binary ? "true" : "false",
					quote_literal_cstr(checksum),
					quote_literal_cstr(branch));

				SPI_execute(isql.data, false, 0);
				pfree(isql.data);
				pfree(content.data);

				values[0] = CStringGetTextDatum(rel_path);
				values[1] = CStringGetTextDatum("imported");
				tuplestore_putvalues(tupstore, tupdesc, values, nulls);
				(*count)++;
			}
		}
	}

	closedir(dir);
}

Datum
codefs_import_from_disk(PG_FUNCTION_ARGS)
{
	ReturnSetInfo  *rsinfo;
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	char	   *source_dir;
	char	   *branch;
	int			count = 0;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("codefs: source directory cannot be NULL")));

	source_dir = text_to_cstring(PG_GETARG_TEXT_PP(0));
	branch = PG_ARGISNULL(1) ? "main" : text_to_cstring(PG_GETARG_TEXT_PP(1));

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
	rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	tupstore = rsinfo->setResult;
	tupdesc = rsinfo->setDesc;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	import_dir_recursive(source_dir, "", branch, tupstore, tupdesc, &count);

	PopActiveSnapshot();
	SPI_finish();

	ereport(NOTICE,
			(errmsg("codefs: imported %d files from '%s' to branch '%s'",
					count, source_dir, branch)));

	return (Datum) 0;
}
