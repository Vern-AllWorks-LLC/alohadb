/*-------------------------------------------------------------------------
 *
 * parquet_reader.c
 *	  Minimal Parquet file reader for the alohadb_parquet extension.
 *
 *	  Validates Parquet file format and returns file metadata as jsonb.
 *	  Full Parquet reading requires linking against libarrow/libparquet.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_parquet/parquet_reader.c
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
#include <string.h>
#include <arpa/inet.h>

PG_FUNCTION_INFO_V1(read_parquet);

/*
 * read_parquet(file_path text)
 * RETURNS SETOF jsonb
 *
 * Minimal Parquet reader. Validates the file magic and returns
 * file metadata as a single jsonb row. Full Parquet parsing requires
 * Apache Thrift deserialization; for production use, link against
 * libarrow/libparquet.
 */
Datum
read_parquet(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcxt;
		char	   *filepath;
		FILE	   *fp;
		char		magic[4];
		unsigned char footer_buf[4];
		int32		footer_len;
		long		file_size;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcxt = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		filepath = text_to_cstring(PG_GETARG_TEXT_PP(0));

		if (!parquet_validate_path(filepath))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("file path \"%s\" is not in allowed directories",
							filepath),
					 errhint("Set alohadb.parquet_allowed_paths to allow this directory.")));

		fp = fopen(filepath, "rb");
		if (fp == NULL)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", filepath)));

		/* Check magic number: PAR1 */
		if (fread(magic, 1, 4, fp) != 4 || memcmp(magic, "PAR1", 4) != 0)
		{
			fclose(fp);
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("file \"%s\" is not a valid Parquet file (missing PAR1 magic)",
							filepath)));
		}

		/* Get file size */
		fseek(fp, 0, SEEK_END);
		file_size = ftell(fp);

		/* Read footer length (4 bytes before trailing PAR1) */
		fseek(fp, file_size - 8, SEEK_SET);
		if (fread(footer_buf, 1, 4, fp) != 4)
		{
			fclose(fp);
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("could not read Parquet footer length")));
		}

		/* Footer length is little-endian 32-bit int */
		footer_len = (int32) footer_buf[0] |
			((int32) footer_buf[1] << 8) |
			((int32) footer_buf[2] << 16) |
			((int32) footer_buf[3] << 24);

		/* Check trailing magic */
		{
			char		tail_magic[4];

			fseek(fp, file_size - 4, SEEK_SET);
			if (fread(tail_magic, 1, 4, fp) != 4 ||
				memcmp(tail_magic, "PAR1", 4) != 0)
			{
				fclose(fp);
				ereport(ERROR,
						(errcode(ERRCODE_DATA_EXCEPTION),
						 errmsg("file \"%s\" has corrupted Parquet footer",
								filepath)));
			}
		}

		fclose(fp);

		/*
		 * For a complete Parquet reader, we would:
		 * 1. Read the Thrift-encoded FileMetaData from the footer
		 * 2. Parse schema, row groups, column chunks
		 * 3. Read and decode data pages (PLAIN, DICTIONARY, RLE encoding)
		 * 4. Handle compression (snappy, gzip, zstd)
		 *
		 * Since full Thrift deserialization is very complex (~3000 LOC),
		 * we return file metadata as a single jsonb row for now.
		 * For production: link against libparquet/libarrow.
		 */
		{
			StringInfoData json;
			Datum	   *result_datums;

			initStringInfo(&json);
			appendStringInfo(&json,
							 "{\"format\": \"parquet\", \"file_size\": %ld, "
							 "\"footer_length\": %d, \"status\": \"valid\", "
							 "\"note\": \"Full Parquet reading requires libarrow. "
							 "Use read_csv() or read_json() for text-based file formats.\"}",
							 file_size, footer_len);

			result_datums = palloc(sizeof(Datum));
			result_datums[0] = DirectFunctionCall1(jsonb_in,
												   CStringGetDatum(json.data));
			pfree(json.data);

			funcctx->user_fctx = result_datums;
			funcctx->max_calls = 1;
		}

		MemoryContextSwitchTo(oldcxt);
	}

	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		Datum	   *result_datums = (Datum *) funcctx->user_fctx;

		SRF_RETURN_NEXT(funcctx, result_datums[funcctx->call_cntr]);
	}
	else
		SRF_RETURN_DONE(funcctx);
}
