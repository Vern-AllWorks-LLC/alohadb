/*-------------------------------------------------------------------------
 *
 * search_suggest.c
 *	  Autocomplete, synonym expansion, and text analysis functions.
 *
 *	  search_autocomplete: Uses SPI to find matching rows via prefix
 *	    search with to_tsquery and ts_rank.
 *
 *	  search_expand_synonyms: Looks up synonyms from the
 *	    alohadb_search_synonyms table and builds an expanded tsquery.
 *
 *	  search_analyze: Wraps ts_debug() via SPI to return token analysis.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_search/search_suggest.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <string.h>

#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/snapmgr.h"

#include "alohadb_search.h"

PG_FUNCTION_INFO_V1(search_autocomplete);
PG_FUNCTION_INFO_V1(search_expand_synonyms);
PG_FUNCTION_INFO_V1(search_analyze);

/* ----------------------------------------------------------------
 * search_autocomplete(prefix text, source_table text,
 *                     source_column text, lim int DEFAULT 10)
 * RETURNS TABLE(suggestion text, score float8)
 *
 * Finds rows in source_table whose source_column matches the prefix
 * using a prefix tsquery.  Returns up to lim rows ordered by ts_rank.
 * ---------------------------------------------------------------- */
Datum
search_autocomplete(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	text	   *prefix_text = PG_GETARG_TEXT_PP(0);
	text	   *table_text = PG_GETARG_TEXT_PP(1);
	text	   *column_text = PG_GETARG_TEXT_PP(2);
	int			lim = PG_GETARG_INT32(3);

	char	   *prefix = text_to_cstring(prefix_text);
	char	   *source_table = text_to_cstring(table_text);
	char	   *source_column = text_to_cstring(column_text);

	StringInfoData query;
	int			ret;
	uint64		proc;
	uint64		i;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	Datum	   *values_copy = NULL;
	bool	   *nulls_copy = NULL;
	MemoryContext per_query_ctx;
	MemoryContext oldcxt;

	InitMaterializedSRF(fcinfo, 0);

	rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	tupdesc = rsinfo->setDesc;
	tupstore = rsinfo->setResult;

	/*
	 * Build query:
	 *   SELECT <column>::text AS suggestion,
	 *          ts_rank(to_tsvector(<column>::text),
	 *                  to_tsquery(<prefix> || ':*'))::float8 AS score
	 *   FROM <table>
	 *   WHERE to_tsvector(<column>::text) @@ to_tsquery(<prefix> || ':*')
	 *   ORDER BY score DESC
	 *   LIMIT <lim>
	 */
	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT %s::text AS suggestion, "
					 "ts_rank(to_tsvector(%s::text), to_tsquery(%s || ':*'))::float8 AS score "
					 "FROM %s "
					 "WHERE to_tsvector(%s::text) @@ to_tsquery(%s || ':*') "
					 "ORDER BY score DESC LIMIT %d",
					 quote_identifier(source_column),
					 quote_identifier(source_column),
					 quote_literal_cstr(prefix),
					 source_table,
					 quote_identifier(source_column),
					 quote_literal_cstr(prefix),
					 lim);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(query.data, true, 0);

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "search_autocomplete: SPI_execute failed: error code %d", ret);

	/* Save processed count before finishing SPI */
	proc = SPI_processed;

	/*
	 * Copy results out of SPI memory context before SPI_finish.
	 */
	if (proc > 0)
	{
		per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
		oldcxt = MemoryContextSwitchTo(per_query_ctx);

		values_copy = palloc(sizeof(Datum) * proc * 2);
		nulls_copy = palloc(sizeof(bool) * proc * 2);

		for (i = 0; i < proc; i++)
		{
			bool	isnull;

			values_copy[i * 2] = SPI_getbinval(SPI_tuptable->vals[i],
											   SPI_tuptable->tupdesc,
											   1, &isnull);
			nulls_copy[i * 2] = isnull;
			if (!isnull)
				values_copy[i * 2] = datumCopy(values_copy[i * 2], false, -1);

			values_copy[i * 2 + 1] = SPI_getbinval(SPI_tuptable->vals[i],
												   SPI_tuptable->tupdesc,
												   2, &isnull);
			nulls_copy[i * 2 + 1] = isnull;
			/* float8 is pass-by-value on most platforms, but copy to be safe */
			if (!isnull)
				values_copy[i * 2 + 1] = datumCopy(values_copy[i * 2 + 1], true, sizeof(float8));
		}

		MemoryContextSwitchTo(oldcxt);
	}

	PopActiveSnapshot();
	SPI_finish();

	/* Now populate the tuplestore from our copied data */
	for (i = 0; i < proc; i++)
	{
		Datum		vals[2];
		bool		nuls[2];

		vals[0] = values_copy[i * 2];
		nuls[0] = nulls_copy[i * 2];
		vals[1] = values_copy[i * 2 + 1];
		nuls[1] = nulls_copy[i * 2 + 1];

		tuplestore_putvalues(tupstore, tupdesc, vals, nuls);
	}

	/* Do NOT pfree values_copy/nulls_copy - they may contain Datums pointing
	 * to memory in per_query_ctx that the tuplestore references */

	pfree(query.data);
	pfree(prefix);
	pfree(source_table);
	pfree(source_column);

	return (Datum) 0;
}

/* ----------------------------------------------------------------
 * search_expand_synonyms(query text, config text DEFAULT 'english')
 * RETURNS tsquery
 *
 * Looks up each word of 'query' in the alohadb_search_synonyms table,
 * builds an expanded tsquery with OR'd synonyms for each term.
 *
 * Uses a single SPI session: first looks up synonyms for each word,
 * then converts the assembled expression to a tsquery via to_tsquery().
 * ---------------------------------------------------------------- */
Datum
search_expand_synonyms(PG_FUNCTION_ARGS)
{
	text	   *query_text = PG_GETARG_TEXT_PP(0);
	text	   *config_text = PG_GETARG_TEXT_PP(1);
	char	   *query_str = text_to_cstring(query_text);
	char	   *config = text_to_cstring(config_text);
	StringInfoData result;
	StringInfoData spi_buf;
	int			ret;
	bool		first_term = true;
	char	   *word;
	char	   *saveptr = NULL;
	char	   *query_copy;
	Datum		tsq_datum;
	MemoryContext caller_ctx;

	/*
	 * Allocate working buffers in the caller's memory context so they
	 * survive SPI_finish().
	 */
	caller_ctx = CurrentMemoryContext;
	initStringInfo(&result);
	query_copy = pstrdup(query_str);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/*
	 * Phase 1: Look up synonyms for each word and assemble a tsquery
	 * expression string.
	 */
	for (word = strtok_r(query_copy, " \t\n\r", &saveptr);
		 word != NULL;
		 word = strtok_r(NULL, " \t\n\r", &saveptr))
	{
		int			wlen;

		wlen = strlen(word);
		if (wlen == 0)
			continue;

		if (!first_term)
			appendStringInfoString(&result, " & ");
		first_term = false;

		/*
		 * Look up synonyms for this word.
		 */
		initStringInfo(&spi_buf);
		appendStringInfo(&spi_buf,
						 "SELECT unnest(synonyms) AS syn "
						 "FROM %s "
						 "WHERE term = %s AND config = %s",
						 SEARCH_SYNONYMS_TABLE,
						 quote_literal_cstr(word),
						 quote_literal_cstr(config));

		ret = SPI_execute(spi_buf.data, true, 0);

		if (ret == SPI_OK_SELECT && SPI_processed > 0)
		{
			uint64		nsyns = SPI_processed;
			uint64		j;

			/* Build: (word | syn1 | syn2 | ...) */
			appendStringInfoChar(&result, '(');
			appendStringInfoString(&result, word);

			for (j = 0; j < nsyns; j++)
			{
				char   *syn;

				syn = SPI_getvalue(SPI_tuptable->vals[j],
								   SPI_tuptable->tupdesc, 1);
				if (syn != NULL)
				{
					appendStringInfoString(&result, " | ");
					appendStringInfoString(&result, syn);
				}
			}
			appendStringInfoChar(&result, ')');
		}
		else
		{
			/* No synonyms found; use the word as-is */
			appendStringInfoString(&result, word);
		}

		pfree(spi_buf.data);
	}

	/*
	 * Phase 2: Convert the assembled expression to a tsquery.
	 * We are still inside the same SPI session.
	 */
	if (result.len == 0)
	{
		PopActiveSnapshot();
		SPI_finish();
		PG_RETURN_NULL();
	}

	initStringInfo(&spi_buf);
	appendStringInfo(&spi_buf,
					 "SELECT to_tsquery(%s, %s)",
					 quote_literal_cstr(config),
					 quote_literal_cstr(result.data));

	ret = SPI_execute(spi_buf.data, true, 1);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
	{
		PopActiveSnapshot();
		SPI_finish();
		elog(ERROR, "search_expand_synonyms: failed to build tsquery");
	}

	{
		bool	isnull;
		MemoryContext oldcxt;

		tsq_datum = SPI_getbinval(SPI_tuptable->vals[0],
								  SPI_tuptable->tupdesc, 1, &isnull);
		if (isnull)
		{
			PopActiveSnapshot();
			SPI_finish();
			PG_RETURN_NULL();
		}

		/*
		 * Copy the tsquery datum into the caller's memory context
		 * so it survives SPI_finish().
		 */
		oldcxt = MemoryContextSwitchTo(caller_ctx);
		tsq_datum = datumCopy(tsq_datum, false, -1);
		MemoryContextSwitchTo(oldcxt);
	}

	PopActiveSnapshot();
	SPI_finish();

	/* Do NOT pfree result.data or query_copy -- they are small and
	 * will be freed when the function's memory context is reset. */

	PG_RETURN_DATUM(tsq_datum);
}

/* ----------------------------------------------------------------
 * search_analyze(input text, config text DEFAULT 'english')
 * RETURNS TABLE(token text, type text, position int)
 *
 * Wraps ts_debug() to return token analysis for the input string.
 * ---------------------------------------------------------------- */
Datum
search_analyze(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	text	   *input_text = PG_GETARG_TEXT_PP(0);
	text	   *config_text = PG_GETARG_TEXT_PP(1);
	char	   *input = text_to_cstring(input_text);
	char	   *config = text_to_cstring(config_text);

	StringInfoData query;
	int			ret;
	uint64		proc;
	uint64		i;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	Datum	   *tok_values = NULL;
	Datum	   *type_values = NULL;
	bool	   *tok_nulls = NULL;
	bool	   *type_nulls = NULL;
	MemoryContext per_query_ctx;
	MemoryContext oldcxt;

	InitMaterializedSRF(fcinfo, 0);

	rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	tupdesc = rsinfo->setDesc;
	tupstore = rsinfo->setResult;

	/*
	 * Use ts_debug() to analyze the input.  ts_debug returns columns:
	 *   alias, description, token, dictionaries, dictionary, lexemes
	 * We extract alias (type), token, and generate a position number.
	 */
	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT token, alias AS type "
					 "FROM ts_debug(%s, %s)",
					 quote_literal_cstr(config),
					 quote_literal_cstr(input));

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(query.data, true, 0);

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "search_analyze: SPI_execute failed: error code %d", ret);

	proc = SPI_processed;

	/*
	 * Copy results out of SPI context.
	 */
	if (proc > 0)
	{
		per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
		oldcxt = MemoryContextSwitchTo(per_query_ctx);

		tok_values = palloc(sizeof(Datum) * proc);
		type_values = palloc(sizeof(Datum) * proc);
		tok_nulls = palloc(sizeof(bool) * proc);
		type_nulls = palloc(sizeof(bool) * proc);

		for (i = 0; i < proc; i++)
		{
			bool	isnull;

			/* token (column 1) */
			tok_values[i] = SPI_getbinval(SPI_tuptable->vals[i],
										  SPI_tuptable->tupdesc,
										  1, &isnull);
			tok_nulls[i] = isnull;
			if (!isnull)
				tok_values[i] = datumCopy(tok_values[i], false, -1);

			/* type (column 2) */
			type_values[i] = SPI_getbinval(SPI_tuptable->vals[i],
										   SPI_tuptable->tupdesc,
										   2, &isnull);
			type_nulls[i] = isnull;
			if (!isnull)
				type_values[i] = datumCopy(type_values[i], false, -1);
		}

		MemoryContextSwitchTo(oldcxt);
	}

	PopActiveSnapshot();
	SPI_finish();

	/* Populate tuplestore from copied data */
	for (i = 0; i < proc; i++)
	{
		Datum		vals[3];
		bool		nuls[3];

		vals[0] = tok_values[i];
		nuls[0] = tok_nulls[i];
		vals[1] = type_values[i];
		nuls[1] = type_nulls[i];
		vals[2] = Int32GetDatum((int32) (i + 1));	/* 1-based position */
		nuls[2] = false;

		tuplestore_putvalues(tupstore, tupdesc, vals, nuls);
	}

	pfree(query.data);
	pfree(input);
	pfree(config);

	return (Datum) 0;
}
