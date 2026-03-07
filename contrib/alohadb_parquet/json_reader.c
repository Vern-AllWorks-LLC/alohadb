/*-------------------------------------------------------------------------
 *
 * json_reader.c
 *	  JSON Lines file reader for the alohadb_parquet extension.
 *
 *	  Reads JSON Lines files (one JSON object per line) and returns
 *	  each line as a jsonb value.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_parquet/json_reader.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"

#include "alohadb_parquet.h"

#include <stdio.h>

PG_FUNCTION_INFO_V1(read_json_file);

typedef struct JsonReaderState
{
	FILE	   *fp;
} JsonReaderState;

/*
 * read_json_file(file_path text)
 * RETURNS SETOF jsonb
 *
 * Reads a JSON Lines file (one JSON object per line).
 */
Datum
read_json_file(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	JsonReaderState *state;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcxt;
		char	   *filepath;
		FILE	   *fp;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcxt = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		filepath = text_to_cstring(PG_GETARG_TEXT_PP(0));

		if (!parquet_validate_path(filepath))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("file path \"%s\" is not in allowed directories",
							filepath),
					 errhint("Set alohadb.parquet_allowed_paths to allow this directory.")));

		fp = fopen(filepath, "r");
		if (fp == NULL)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", filepath)));

		state = palloc0(sizeof(JsonReaderState));
		state->fp = fp;

		funcctx->user_fctx = state;
		MemoryContextSwitchTo(oldcxt);
	}

	funcctx = SRF_PERCALL_SETUP();
	state = (JsonReaderState *) funcctx->user_fctx;

	{
		char		line[1048576];	/* 1MB max per line */

		while (fgets(line, sizeof(line), state->fp) != NULL)
		{
			int			len = strlen(line);
			Datum		result;

			/* Strip trailing whitespace */
			while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r' ||
							   line[len - 1] == ' ' || line[len - 1] == '\t'))
				line[--len] = '\0';

			/* Skip empty lines */
			if (len == 0)
				continue;

			/* Parse as jsonb */
			result = DirectFunctionCall1(jsonb_in, CStringGetDatum(line));
			SRF_RETURN_NEXT(funcctx, result);
		}

		fclose(state->fp);
		SRF_RETURN_DONE(funcctx);
	}
}
