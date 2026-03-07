/*-------------------------------------------------------------------------
 *
 * search_bm25.c
 *	  BM25 ranking function for full-text search.
 *
 *	  Computes the Okapi BM25 score for a document (tsvector) against
 *	  a query (tsquery).  Uses term frequencies extracted from the
 *	  tsvector and configurable k1/b parameters.
 *
 *	  BM25 formula per matching term:
 *	    IDF * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl / avgdl))
 *
 *	  where:
 *	    IDF  = log((N - n + 0.5) / (n + 0.5))
 *	    tf   = term frequency in document
 *	    dl   = document length (total positions in tsvector)
 *	    avgdl = average document length (default 256)
 *	    N    = estimated total documents (default 1000)
 *	    n    = estimated matching documents (default 10)
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_search/search_bm25.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <math.h>

#include "fmgr.h"
#include "tsearch/ts_type.h"
#include "tsearch/ts_utils.h"
#include "utils/builtins.h"

#include "alohadb_search.h"

PG_FUNCTION_INFO_V1(search_bm25);

/*
 * search_bm25(document tsvector, query tsquery,
 *              k1 float8 DEFAULT 1.2, b float8 DEFAULT 0.75)
 * RETURNS float8
 *
 * Compute BM25 relevance score for a document against a query.
 */
Datum
search_bm25(PG_FUNCTION_ARGS)
{
	TSVector	doc = PG_GETARG_TSVECTOR(0);
	TSQuery		query = PG_GETARG_TSQUERY(1);
	float8		k1 = PG_GETARG_FLOAT8(2);
	float8		b = PG_GETARG_FLOAT8(3);

	float8		score = 0.0;
	float8		avg_doc_len = BM25_DEFAULT_AVG_DOC_LEN;
	float8		N = BM25_DEFAULT_TOTAL_DOCS;
	float8		n = BM25_DEFAULT_MATCHING_DOCS;
	float8		idf;
	int			doc_len;
	int			num_doc_lexemes;
	int			num_query_items;
	WordEntry  *doc_entries;
	QueryItem  *query_items;
	char	   *doc_operand;
	char	   *query_operand;
	int			i;
	int			j;

	/* Calculate the IDF component (same for all terms in this simple model) */
	idf = log((N - n + 0.5) / (n + 0.5));
	if (idf < 0.0)
		idf = 0.0;

	/* Get document lexemes and compute document length */
	num_doc_lexemes = doc->size;
	doc_entries = ARRPTR(doc);
	doc_operand = STRPTR(doc);

	/*
	 * Document length = total number of positions across all lexemes.
	 * If no positions are stored, approximate as number of lexemes.
	 */
	doc_len = 0;
	for (i = 0; i < num_doc_lexemes; i++)
	{
		if (doc_entries[i].haspos)
		{
			int			npos = POSDATALEN(doc, &doc_entries[i]);

			doc_len += npos;
		}
		else
		{
			doc_len += 1;
		}
	}

	if (doc_len == 0)
		doc_len = num_doc_lexemes;

	/* Walk through query items looking for operands (terms) */
	num_query_items = query->size;
	query_items = GETQUERY(query);
	query_operand = GETOPERAND(query);

	for (i = 0; i < num_query_items; i++)
	{
		QueryOperand *qop;
		char	   *query_term;
		int			query_term_len;
		float8		tf = 0.0;

		/* Skip operators, only process operands */
		if (query_items[i].type != QI_VAL)
			continue;

		qop = &query_items[i].qoperand;
		query_term = query_operand + qop->distance;
		query_term_len = qop->length;

		/*
		 * Search for this query term in the document tsvector.
		 * The tsvector lexemes are sorted, so we can use binary search.
		 */
		for (j = 0; j < num_doc_lexemes; j++)
		{
			char	   *doc_term = doc_operand + doc_entries[j].pos;
			int			doc_term_len = doc_entries[j].len;
			int			cmp;

			/* Compare lengths first for efficiency */
			if (doc_term_len == query_term_len)
				cmp = memcmp(doc_term, query_term, query_term_len);
			else if (doc_term_len < query_term_len)
				cmp = memcmp(doc_term, query_term, doc_term_len) <= 0 ? -1 : 1;
			else
				cmp = memcmp(doc_term, query_term, query_term_len) >= 0 ? 1 : -1;

			if (cmp == 0)
			{
				/* Found matching term; count positions as term frequency */
				if (doc_entries[j].haspos)
					tf = (float8) POSDATALEN(doc, &doc_entries[j]);
				else
					tf = 1.0;
				break;
			}
		}

		if (tf > 0.0)
		{
			/*
			 * BM25 per-term score:
			 *   IDF * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl/avgdl))
			 */
			float8		numerator = tf * (k1 + 1.0);
			float8		denominator = tf + k1 * (1.0 - b + b * ((float8) doc_len / avg_doc_len));

			if (denominator > 0.0)
				score += idf * (numerator / denominator);
		}
	}

	PG_FREE_IF_COPY(doc, 0);
	PG_FREE_IF_COPY(query, 1);

	PG_RETURN_FLOAT8(score);
}
