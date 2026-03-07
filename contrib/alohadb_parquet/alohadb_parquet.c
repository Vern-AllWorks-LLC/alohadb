/*-------------------------------------------------------------------------
 *
 * alohadb_parquet.c
 *	  Main entry point for the alohadb_parquet extension.
 *
 *	  Provides GUC configuration and path validation for the
 *	  Parquet, CSV, and JSON file reader functions.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_parquet/alohadb_parquet.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "utils/guc.h"

#include "alohadb_parquet.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_parquet",
					.version = "1.0"
);

/* GUC variable */
char *alohadb_parquet_allowed_paths = "/tmp";

void
_PG_init(void)
{
	DefineCustomStringVariable("alohadb.parquet_allowed_paths",
							   "Comma-separated list of allowed directory paths for file reading",
							   NULL,
							   &alohadb_parquet_allowed_paths,
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
	if (realpath(filepath, resolved) == NULL)
	{
		/* File might not exist yet, use the path as-is for prefix check */
		strlcpy(resolved, filepath, PARQUET_MAX_PATH_LEN);
	}

	if (alohadb_parquet_allowed_paths == NULL ||
		alohadb_parquet_allowed_paths[0] == '\0')
		return false;

	paths_copy = pstrdup(alohadb_parquet_allowed_paths);

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
