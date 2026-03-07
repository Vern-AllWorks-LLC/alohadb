/*-------------------------------------------------------------------------
 *
 * alohadb_parquet.h
 *	  Header for the alohadb_parquet extension.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_parquet/alohadb_parquet.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ALOHADB_PARQUET_H
#define ALOHADB_PARQUET_H

#include "postgres.h"
#include "fmgr.h"

/* Safety: maximum file path length */
#define PARQUET_MAX_PATH_LEN 4096

/* GUC for allowed paths */
extern char *alohadb_parquet_allowed_paths;

/* Path validation */
extern bool parquet_validate_path(const char *filepath);

/* Functions */
extern Datum read_csv(PG_FUNCTION_ARGS);
extern Datum read_json_file(PG_FUNCTION_ARGS);
extern Datum read_parquet(PG_FUNCTION_ARGS);

#endif /* ALOHADB_PARQUET_H */
