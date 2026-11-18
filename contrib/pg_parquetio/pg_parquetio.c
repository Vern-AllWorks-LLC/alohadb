/*-------------------------------------------------------------------------
 *
 * pg_parquetio.c
 *	  Main entry point for the pg_parquetio extension.
 *
 *	  Provides GUC configuration and path validation for the
 *	  Parquet, CSV, and JSON file reader functions and the Parquet /
 *	  Arrow IPC writer functions (write_parquet, write_arrow,
 *	  arrow_ipc_stream; version 1.2).
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/pg_parquetio/pg_parquetio.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "utils/guc.h"

#include "pg_parquetio.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

PG_MODULE_MAGIC_EXT(
					.name = "pg_parquetio",
					.version = "1.2"
);

/* GUC variable */
char *pg_parquetio_allowed_paths = "/tmp";

void
_PG_init(void)
{
	DefineCustomStringVariable("pg_parquetio.allowed_paths",
							   "Comma-separated list of directories that pg_parquetio may read from and write to",
							   NULL,
							   &pg_parquetio_allowed_paths,
							   "/tmp",
							   PGC_SUSET,
							   0,
							   NULL, NULL, NULL);
}

/*
 * Validate that a file path is within allowed directories.
 * Returns true if path is allowed, false otherwise.
 */
bool
parquet_validate_path(const char *filepath)
{
	char	   *paths_copy;
	char	   *token;
	char	   *saveptr;
	char		resolved[PARQUET_MAX_PATH_LEN];

	if (filepath == NULL || filepath[0] == '\0')
		return false;

	/* Resolve the real path */
#ifdef WIN32
	if (_fullpath(resolved, filepath, PARQUET_MAX_PATH_LEN) == NULL)
#else
	if (realpath(filepath, resolved) == NULL)
#endif
	{
		/* File might not exist yet, use the path as-is for prefix check */
		strlcpy(resolved, filepath, PARQUET_MAX_PATH_LEN);
	}

	if (pg_parquetio_allowed_paths == NULL ||
		pg_parquetio_allowed_paths[0] == '\0')
		return false;

	paths_copy = pstrdup(pg_parquetio_allowed_paths);

	for (token = strtok_r(paths_copy, ",", &saveptr);
		 token != NULL;
		 token = strtok_r(NULL, ",", &saveptr))
	{
		char   *end;

		/* Trim leading whitespace */
		while (*token == ' ')
			token++;

		/* Trim trailing whitespace */
		end = token + strlen(token) - 1;
		while (end > token && *end == ' ')
		{
			*end = '\0';
			end--;
		}

		if (strncmp(resolved, token, strlen(token)) == 0)
		{
			pfree(paths_copy);
			return true;
		}
	}

	pfree(paths_copy);
	return false;
}
