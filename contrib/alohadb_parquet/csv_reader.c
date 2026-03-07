/*-------------------------------------------------------------------------
 *
 * csv_reader.c
 *	  CSV file reader for the alohadb_parquet extension.
 *
 *	  Reads CSV files and returns each row as a jsonb object.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_parquet/csv_reader.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"

#include "alohadb_parquet.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

PG_FUNCTION_INFO_V1(read_csv);

/* State for CSV reading SRF */
typedef struct CsvReaderState
{
	FILE	   *fp;
	char	  **headers;
	int			ncols;
	char		delimiter;
	bool		has_header;
} CsvReaderState;

/* Parse a single CSV line into fields */
static int
parse_csv_line(char *line, char delimiter, char ***fields_out)
{
	int			capacity = 16;
	int			count = 0;
	char	  **fields = palloc(sizeof(char *) * capacity);
	char	   *p = line;
	StringInfoData buf;
	bool		in_quotes = false;
	int			len;

	/* Remove trailing newline */
	len = strlen(line);
	while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r'))
		line[--len] = '\0';

	initStringInfo(&buf);

	while (*p != '\0')
	{
		if (*p == '"' && !in_quotes)
		{
			in_quotes = true;
			p++;
		}
		else if (*p == '"' && in_quotes)
		{
			if (*(p + 1) == '"')
			{
				appendStringInfoChar(&buf, '"');
				p += 2;
			}
			else
			{
				in_quotes = false;
				p++;
			}
		}
		else if (*p == delimiter && !in_quotes)
		{
			if (count >= capacity)
			{
				capacity *= 2;
				fields = repalloc(fields, sizeof(char *) * capacity);
			}
			fields[count++] = pstrdup(buf.data);
			resetStringInfo(&buf);
			p++;
		}
		else
		{
			appendStringInfoChar(&buf, *p);
			p++;
		}
	}

	/* Last field */
	if (count >= capacity)
	{
		capacity++;
		fields = repalloc(fields, sizeof(char *) * capacity);
	}
	fields[count++] = pstrdup(buf.data);
	pfree(buf.data);

	*fields_out = fields;
	return count;
}

/*
 * read_csv(file_path text, delimiter text DEFAULT ',', has_header bool DEFAULT true)
 * RETURNS SETOF jsonb
 *
 * Each row is returned as a jsonb object with header names as keys.
 * If no header, uses col1, col2, etc.
 */
Datum
read_csv(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	CsvReaderState *state;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcxt;
		char	   *filepath;
		char	   *delim_str;
		bool		has_header;
		FILE	   *fp;
		char		line[65536];

		funcctx = SRF_FIRSTCALL_INIT();
		oldcxt = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		filepath = text_to_cstring(PG_GETARG_TEXT_PP(0));
		delim_str = PG_ARGISNULL(1) ? "," : text_to_cstring(PG_GETARG_TEXT_PP(1));
		has_header = PG_ARGISNULL(2) ? true : PG_GETARG_BOOL(2);

		/* Validate path */
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

		state = palloc0(sizeof(CsvReaderState));
		state->fp = fp;
		state->delimiter = delim_str[0];
		state->has_header = has_header;
		state->headers = NULL;
		state->ncols = 0;

		/* Read header line if present */
		if (has_header && fgets(line, sizeof(line), fp) != NULL)
		{
			state->ncols = parse_csv_line(line, state->delimiter,
										  &state->headers);
		}

		funcctx->user_fctx = state;
		MemoryContextSwitchTo(oldcxt);
	}

	funcctx = SRF_PERCALL_SETUP();
	state = (CsvReaderState *) funcctx->user_fctx;

	{
		char		line[65536];

		if (fgets(line, sizeof(line), state->fp) != NULL)
		{
			char	  **fields;
			int			nfields;
			StringInfoData json_buf;
			Datum		result;
			int			i;

			nfields = parse_csv_line(line, state->delimiter, &fields);

			/* If we didn't have headers, create them now */
			if (state->headers == NULL)
			{
				state->ncols = nfields;
				state->headers = MemoryContextAlloc(
					funcctx->multi_call_memory_ctx,
					sizeof(char *) * nfields);
				for (i = 0; i < nfields; i++)
				{
					char		colname[32];

					snprintf(colname, sizeof(colname), "col%d", i + 1);
					state->headers[i] = MemoryContextStrdup(
						funcctx->multi_call_memory_ctx, colname);
				}
			}

			initStringInfo(&json_buf);
			appendStringInfoChar(&json_buf, '{');

			for (i = 0; i < nfields && i < state->ncols; i++)
			{
				if (i > 0)
					appendStringInfoString(&json_buf, ", ");

				/* Escape the key */
				appendStringInfo(&json_buf, "\"%s\": ", state->headers[i]);

				/* Try to detect type: number, bool, null, or string */
				if (fields[i][0] == '\0')
				{
					appendStringInfoString(&json_buf, "null");
				}
				else
				{
					char	   *endptr;

					strtod(fields[i], &endptr);
					if (*endptr == '\0' && fields[i][0] != '\0')
					{
						/* number */
						appendStringInfoString(&json_buf, fields[i]);
					}
					else if (strcmp(fields[i], "true") == 0 ||
							 strcmp(fields[i], "false") == 0)
					{
						/* bool */
						appendStringInfoString(&json_buf, fields[i]);
					}
					else
					{
						char   *s;

						/* String - escape special chars */
						appendStringInfoChar(&json_buf, '"');
						for (s = fields[i]; *s; s++)
						{
							if (*s == '"')
								appendStringInfoString(&json_buf, "\\\"");
							else if (*s == '\\')
								appendStringInfoString(&json_buf, "\\\\");
							else if (*s == '\n')
								appendStringInfoString(&json_buf, "\\n");
							else if (*s == '\t')
								appendStringInfoString(&json_buf, "\\t");
							else
								appendStringInfoChar(&json_buf, *s);
						}
						appendStringInfoChar(&json_buf, '"');
					}
				}
				pfree(fields[i]);
			}

			appendStringInfoChar(&json_buf, '}');
			pfree(fields);

			result = DirectFunctionCall1(jsonb_in,
										 CStringGetDatum(json_buf.data));
			pfree(json_buf.data);

			SRF_RETURN_NEXT(funcctx, result);
		}
		else
		{
			/* EOF */
			fclose(state->fp);
			SRF_RETURN_DONE(funcctx);
		}
	}
}
