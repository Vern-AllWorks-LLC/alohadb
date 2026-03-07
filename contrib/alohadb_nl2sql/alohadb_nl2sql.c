/*-------------------------------------------------------------------------
 *
 * alohadb_nl2sql.c
 *	  Main entry point for the alohadb_nl2sql extension.
 *
 *	  Provides SQL functions that translate natural language queries to SQL
 *	  via an external LLM API (Claude, OpenAI, or generic POST endpoint).
 *
 *	  UDFs:
 *	    - alohadb_nl2sql(question text) RETURNS text
 *	    - alohadb_nl2sql_execute(question text) RETURNS SETOF record
 *	    - alohadb_explain_query(sql text) RETURNS text
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_nl2sql/alohadb_nl2sql.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <curl/curl.h>
#include <string.h>

#include "executor/spi.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/guc.h"

#include "alohadb_nl2sql.h"

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_nl2sql",
					.version = "1.0"
);

/* GUC variables */
char	   *nl2sql_endpoint = NULL;
char	   *nl2sql_api_key = NULL;
char	   *nl2sql_model = NULL;

/* Function declarations */
PG_FUNCTION_INFO_V1(alohadb_nl2sql);
PG_FUNCTION_INFO_V1(alohadb_nl2sql_execute);
PG_FUNCTION_INFO_V1(alohadb_explain_query);

/* ----------------------------------------------------------------
 * curl write callback: accumulate response data into a StringInfo
 * ----------------------------------------------------------------
 */
typedef struct CurlResponseData
{
	StringInfoData buf;
	bool		overflow;
} CurlResponseData;

static size_t
nl2sql_curl_write_cb(void *contents, size_t size, size_t nmemb, void *userp)
{
	size_t			realsize = size * nmemb;
	CurlResponseData *resp = (CurlResponseData *) userp;

	if (resp->overflow)
		return realsize;

	if (resp->buf.len + (int) realsize > NL2SQL_MAX_RESPONSE_LEN)
	{
		resp->overflow = true;
		return realsize;
	}

	appendBinaryStringInfo(&resp->buf, (const char *) contents, (int) realsize);
	return realsize;
}

/* ----------------------------------------------------------------
 * detect_api_format
 *
 * Return 'C' for Claude (Anthropic), 'O' for OpenAI, 'G' for generic.
 * Detection is based on the endpoint URL.
 * ----------------------------------------------------------------
 */
static char
detect_api_format(const char *endpoint)
{
	if (strstr(endpoint, "anthropic.com") != NULL)
		return 'C';
	if (strstr(endpoint, "openai.com") != NULL)
		return 'O';
	return 'G';
}

/* ----------------------------------------------------------------
 * json_escape_string
 *
 * Escape a C string for inclusion inside a JSON string value.
 * Returns a palloc'd string.
 * ----------------------------------------------------------------
 */
static char *
json_escape_string(const char *input)
{
	StringInfoData buf;
	const char *p;

	initStringInfo(&buf);

	for (p = input; *p != '\0'; p++)
	{
		switch (*p)
		{
			case '"':
				appendStringInfoString(&buf, "\\\"");
				break;
			case '\\':
				appendStringInfoString(&buf, "\\\\");
				break;
			case '\n':
				appendStringInfoString(&buf, "\\n");
				break;
			case '\r':
				appendStringInfoString(&buf, "\\r");
				break;
			case '\t':
				appendStringInfoString(&buf, "\\t");
				break;
			case '\b':
				appendStringInfoString(&buf, "\\b");
				break;
			case '\f':
				appendStringInfoString(&buf, "\\f");
				break;
			default:
				if ((unsigned char) *p < 0x20)
				{
					/* Control character: emit as \u00XX */
					appendStringInfo(&buf, "\\u%04x", (unsigned char) *p);
				}
				else
				{
					appendStringInfoChar(&buf, *p);
				}
				break;
		}
	}

	return buf.data;
}

/* ----------------------------------------------------------------
 * build_request_body
 *
 * Build the JSON request body for the configured API format.
 * ----------------------------------------------------------------
 */
static char *
build_request_body(const char *prompt, char api_format)
{
	StringInfoData body;
	char	   *escaped_prompt;
	char	   *escaped_model;

	escaped_prompt = json_escape_string(prompt);
	escaped_model = json_escape_string(nl2sql_model);

	initStringInfo(&body);

	switch (api_format)
	{
		case 'C':
			/* Anthropic Claude Messages API format */
			appendStringInfo(&body,
							 "{\"model\":\"%s\","
							 "\"max_tokens\":4096,"
							 "\"messages\":["
							 "{\"role\":\"user\","
							 "\"content\":\"%s\"}"
							 "]}",
							 escaped_model, escaped_prompt);
			break;

		case 'O':
			/* OpenAI Chat Completions API format */
			appendStringInfo(&body,
							 "{\"model\":\"%s\","
							 "\"max_tokens\":4096,"
							 "\"messages\":["
							 "{\"role\":\"user\","
							 "\"content\":\"%s\"}"
							 "]}",
							 escaped_model, escaped_prompt);
			break;

		default:
			/* Generic POST: send model and prompt as top-level fields */
			appendStringInfo(&body,
							 "{\"model\":\"%s\","
							 "\"prompt\":\"%s\","
							 "\"max_tokens\":4096}",
							 escaped_model, escaped_prompt);
			break;
	}

	pfree(escaped_prompt);
	pfree(escaped_model);

	return body.data;
}

/* ----------------------------------------------------------------
 * extract_json_string_value
 *
 * Simple JSON string extraction: find the first occurrence of
 * "key":"value" and return the value.  Handles escaped quotes
 * inside the value.  Returns palloc'd string or NULL.
 * ----------------------------------------------------------------
 */
static char *
extract_json_string_value(const char *json, const char *key)
{
	StringInfoData keybuf;
	const char *pos;
	const char *start;
	StringInfoData valbuf;

	initStringInfo(&keybuf);
	appendStringInfo(&keybuf, "\"%s\"", key);

	pos = strstr(json, keybuf.data);
	pfree(keybuf.data);

	if (pos == NULL)
		return NULL;

	/* Advance past the key and find the colon, then the opening quote */
	pos += strlen(key) + 2;		/* skip past "key" */
	while (*pos != '\0' && (*pos == ' ' || *pos == ':' || *pos == '\t'))
		pos++;

	if (*pos != '"')
		return NULL;

	start = pos + 1;			/* skip the opening quote */

	initStringInfo(&valbuf);

	for (pos = start; *pos != '\0'; pos++)
	{
		if (*pos == '\\' && *(pos + 1) != '\0')
		{
			/* Handle escape sequences */
			pos++;
			switch (*pos)
			{
				case '"':
					appendStringInfoChar(&valbuf, '"');
					break;
				case '\\':
					appendStringInfoChar(&valbuf, '\\');
					break;
				case 'n':
					appendStringInfoChar(&valbuf, '\n');
					break;
				case 'r':
					appendStringInfoChar(&valbuf, '\r');
					break;
				case 't':
					appendStringInfoChar(&valbuf, '\t');
					break;
				default:
					appendStringInfoChar(&valbuf, '\\');
					appendStringInfoChar(&valbuf, *pos);
					break;
			}
		}
		else if (*pos == '"')
		{
			/* End of string value */
			break;
		}
		else
		{
			appendStringInfoChar(&valbuf, *pos);
		}
	}

	return valbuf.data;
}

/* ----------------------------------------------------------------
 * parse_llm_response
 *
 * Extract the generated text from the LLM JSON response.
 * Supports Claude, OpenAI, and generic formats.
 * Returns a palloc'd string.
 * ----------------------------------------------------------------
 */
static char *
parse_llm_response(const char *response, char api_format)
{
	char	   *text = NULL;

	switch (api_format)
	{
		case 'C':
			/*
			 * Claude response format:
			 * {"content":[{"type":"text","text":"..."}], ...}
			 *
			 * Look for "text" key within the content array.
			 */
			text = extract_json_string_value(response, "text");
			break;

		case 'O':
			/*
			 * OpenAI response format:
			 * {"choices":[{"message":{"content":"..."}}]}
			 *
			 * Look for "content" key within choices.
			 */
			text = extract_json_string_value(response, "content");
			break;

		default:
			/*
			 * Generic: try "text", then "content", then "response",
			 * then "output".
			 */
			text = extract_json_string_value(response, "text");
			if (text == NULL)
				text = extract_json_string_value(response, "content");
			if (text == NULL)
				text = extract_json_string_value(response, "response");
			if (text == NULL)
				text = extract_json_string_value(response, "output");
			break;
	}

	if (text == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("alohadb_nl2sql: could not parse LLM response"),
				 errdetail("Raw response (first 512 chars): %.512s",
						   response)));

	return text;
}

/* ----------------------------------------------------------------
 * nl2sql_call_llm
 *
 * Send a prompt to the configured LLM endpoint via HTTP POST using
 * libcurl.  Returns a palloc'd string with the extracted text from
 * the response.
 * ----------------------------------------------------------------
 */
char *
nl2sql_call_llm(const char *prompt)
{
	CURL	   *curl;
	CURLcode	res;
	struct curl_slist *headers = NULL;
	CurlResponseData response;
	char		api_format;
	char	   *request_body;
	long		http_code;
	char	   *result;
	StringInfoData auth_header;

	/* Validate GUC settings */
	if (nl2sql_endpoint == NULL || nl2sql_endpoint[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("alohadb.nl2sql_endpoint is not configured"),
				 errhint("Set it via: ALTER SYSTEM SET alohadb.nl2sql_endpoint = 'https://api.anthropic.com/v1/messages';")));

	if (nl2sql_api_key == NULL || nl2sql_api_key[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("alohadb.nl2sql_api_key is not configured"),
				 errhint("Set it via: ALTER SYSTEM SET alohadb.nl2sql_api_key = 'your-api-key';")));

	api_format = detect_api_format(nl2sql_endpoint);
	request_body = build_request_body(prompt, api_format);

	/* Check request size */
	if ((int) strlen(request_body) > NL2SQL_MAX_REQUEST_LEN)
	{
		pfree(request_body);
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("alohadb_nl2sql: request body exceeds maximum size (%d bytes)",
						NL2SQL_MAX_REQUEST_LEN)));
	}

	/* Initialize curl */
	curl = curl_easy_init();
	if (curl == NULL)
	{
		pfree(request_body);
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("alohadb_nl2sql: could not initialize libcurl")));
	}

	/* Initialize response buffer */
	initStringInfo(&response.buf);
	response.overflow = false;

	/* Set common curl options */
	curl_easy_setopt(curl, CURLOPT_URL, nl2sql_endpoint);
	curl_easy_setopt(curl, CURLOPT_POST, 1L);
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request_body);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, nl2sql_curl_write_cb);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long) NL2SQL_HTTP_TIMEOUT_SECS);
	curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

	/* Set HTTP headers based on API format */
	headers = curl_slist_append(headers, "Content-Type: application/json");

	initStringInfo(&auth_header);
	switch (api_format)
	{
		case 'C':
			/* Anthropic uses x-api-key header and anthropic-version */
			appendStringInfo(&auth_header, "x-api-key: %s", nl2sql_api_key);
			headers = curl_slist_append(headers, auth_header.data);
			headers = curl_slist_append(headers,
										"anthropic-version: 2023-06-01");
			break;

		case 'O':
			/* OpenAI uses Bearer token */
			appendStringInfo(&auth_header, "Authorization: Bearer %s",
							 nl2sql_api_key);
			headers = curl_slist_append(headers, auth_header.data);
			break;

		default:
			/* Generic: try Bearer token */
			appendStringInfo(&auth_header, "Authorization: Bearer %s",
							 nl2sql_api_key);
			headers = curl_slist_append(headers, auth_header.data);
			break;
	}

	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

	/* Perform the request */
	res = curl_easy_perform(curl);

	if (res != CURLE_OK)
	{
		const char *err_msg = curl_easy_strerror(res);

		curl_slist_free_all(headers);
		curl_easy_cleanup(curl);
		pfree(request_body);
		pfree(auth_header.data);
		if (response.buf.data)
			pfree(response.buf.data);

		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("alohadb_nl2sql: HTTP request failed: %s", err_msg)));
	}

	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

	curl_slist_free_all(headers);
	curl_easy_cleanup(curl);
	pfree(request_body);
	pfree(auth_header.data);

	if (response.overflow)
	{
		pfree(response.buf.data);
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("alohadb_nl2sql: LLM response exceeded maximum size (%d bytes)",
						NL2SQL_MAX_RESPONSE_LEN)));
	}

	if (http_code < 200 || http_code >= 300)
	{
		char	   *resp_copy = pstrdup(response.buf.data);

		pfree(response.buf.data);
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("alohadb_nl2sql: LLM API returned HTTP %ld",
						http_code),
				 errdetail("Response: %.1024s", resp_copy)));
	}

	result = parse_llm_response(response.buf.data, api_format);
	pfree(response.buf.data);

	return result;
}

/* ----------------------------------------------------------------
 * nl2sql_extract_sql
 *
 * Clean up the LLM response to extract just the SQL statement.
 * Handles markdown code fences (```sql ... ```) and leading/trailing
 * whitespace.  Returns a palloc'd string.
 * ----------------------------------------------------------------
 */
char *
nl2sql_extract_sql(const char *response)
{
	const char *start;
	const char *end;
	size_t		len;
	char	   *result;

	if (response == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_nl2sql: NULL response from LLM")));

	start = response;

	/* Skip leading whitespace */
	while (*start != '\0' && (*start == ' ' || *start == '\t' ||
							  *start == '\n' || *start == '\r'))
		start++;

	/*
	 * Check for markdown code fences: ```sql or ``` at the beginning.
	 */
	if (strncmp(start, "```sql", 6) == 0)
	{
		start += 6;
		/* Skip to end of the opening line */
		while (*start != '\0' && *start != '\n')
			start++;
		if (*start == '\n')
			start++;
	}
	else if (strncmp(start, "```", 3) == 0)
	{
		start += 3;
		while (*start != '\0' && *start != '\n')
			start++;
		if (*start == '\n')
			start++;
	}

	/* Find the end; trim trailing code fence if present */
	end = start + strlen(start);

	/* Trim trailing whitespace */
	while (end > start && (*(end - 1) == ' ' || *(end - 1) == '\t' ||
						   *(end - 1) == '\n' || *(end - 1) == '\r'))
		end--;

	/* Check for closing code fence */
	if (end - start >= 3 && strncmp(end - 3, "```", 3) == 0)
	{
		end -= 3;
		/* Trim whitespace before the closing fence */
		while (end > start && (*(end - 1) == ' ' || *(end - 1) == '\t' ||
							   *(end - 1) == '\n' || *(end - 1) == '\r'))
			end--;
	}

	len = end - start;
	if (len == 0)
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("alohadb_nl2sql: LLM returned an empty SQL response")));

	result = palloc(len + 1);
	memcpy(result, start, len);
	result[len] = '\0';

	return result;
}

/* ----------------------------------------------------------------
 * build_nl2sql_prompt
 *
 * Combine the schema context with the user question into a full
 * prompt for the LLM.  Returns a palloc'd string.
 * ----------------------------------------------------------------
 */
static char *
build_nl2sql_prompt(const char *question)
{
	StringInfoData prompt;
	char	   *schema;

	schema = nl2sql_build_schema_context();

	initStringInfo(&prompt);
	appendStringInfo(&prompt,
					 "Given this database schema:\n%s\n\n"
					 "Generate a SQL query for: %s\n\n"
					 "Respond with ONLY the SQL query, no explanation.",
					 schema, question);

	pfree(schema);
	return prompt.data;
}

/* ----------------------------------------------------------------
 * build_explain_prompt
 *
 * Build a prompt asking the LLM to explain a SQL query in the
 * context of the current database schema.  Returns a palloc'd string.
 * ----------------------------------------------------------------
 */
static char *
build_explain_prompt(const char *sql)
{
	StringInfoData prompt;
	char	   *schema;

	schema = nl2sql_build_schema_context();

	initStringInfo(&prompt);
	appendStringInfo(&prompt,
					 "Given this database schema:\n%s\n\n"
					 "Explain the following SQL query in clear, concise English. "
					 "Describe what it does, which tables and columns it uses, "
					 "any joins or filters, and what the result set will contain.\n\n"
					 "SQL:\n%s",
					 schema, sql);

	pfree(schema);
	return prompt.data;
}

/* ----------------------------------------------------------------
 * alohadb_nl2sql
 *
 * Translate a natural language question to a SQL query.
 *
 * Usage: SELECT alohadb_nl2sql('show me all active users');
 * Returns: text containing the generated SQL query
 * ----------------------------------------------------------------
 */
Datum
alohadb_nl2sql(PG_FUNCTION_ARGS)
{
	text	   *question_text = PG_GETARG_TEXT_PP(0);
	char	   *question;
	char	   *prompt;
	char	   *llm_response;
	char	   *sql;

	question = text_to_cstring(question_text);

	prompt = build_nl2sql_prompt(question);
	llm_response = nl2sql_call_llm(prompt);
	sql = nl2sql_extract_sql(llm_response);

	pfree(question);
	pfree(prompt);
	pfree(llm_response);

	PG_RETURN_TEXT_P(cstring_to_text(sql));
}

/* ----------------------------------------------------------------
 * alohadb_nl2sql_execute
 *
 * Translate a natural language question to SQL, then execute it
 * in a read-only context using SPI, returning the result set.
 *
 * Usage:
 *   SELECT * FROM alohadb_nl2sql_execute('show me all active users')
 *     AS t(id int, name text, active boolean);
 * ----------------------------------------------------------------
 */
Datum
alohadb_nl2sql_execute(PG_FUNCTION_ARGS)
{
	text	   *question_text = PG_GETARG_TEXT_PP(0);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	char	   *question;
	char	   *prompt;
	char	   *llm_response;
	char	   *sql;
	int			ret;
	uint64		proc;
	uint64		i;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	/* Check that caller can accept a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;

	question = text_to_cstring(question_text);

	/* Generate the SQL from the natural language question */
	prompt = build_nl2sql_prompt(question);
	llm_response = nl2sql_call_llm(prompt);
	sql = nl2sql_extract_sql(llm_response);

	pfree(question);
	pfree(prompt);
	pfree(llm_response);

	ereport(NOTICE,
			(errmsg("alohadb_nl2sql: executing generated SQL: %s", sql)));

	/* Connect to SPI and execute in a read-only context */
	SPI_connect();

	/* Execute with read_only = true to prevent mutations */
	ret = SPI_execute(sql, true, 0);

	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("alohadb_nl2sql: generated SQL did not return a result set"),
				 errdetail("SPI returned: %s",
						   SPI_result_code_string(ret)),
				 errhint("Generated SQL: %s", sql)));
	}

	proc = SPI_processed;

	/* Build the result tupdesc from SPI result */
	tupdesc = CreateTupleDescCopy(SPI_tuptable->tupdesc);

	/* Switch to per-query context for the tuplestore */
	oldcontext = MemoryContextSwitchTo(per_query_ctx);
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	MemoryContextSwitchTo(oldcontext);

	/* Copy rows into the tuplestore */
	for (i = 0; i < proc; i++)
	{
		HeapTuple	tuple;

		tuple = SPI_copytuple(SPI_tuptable->vals[i]);
		tuplestore_puttuple(tupstore, tuple);
		heap_freetuple(tuple);
	}

	SPI_finish();
	pfree(sql);

	/* Set up the result */
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	return (Datum) 0;
}

/* ----------------------------------------------------------------
 * alohadb_explain_query
 *
 * Use the LLM to explain a SQL query in plain English.
 *
 * Usage: SELECT alohadb_explain_query('SELECT * FROM users WHERE active');
 * Returns: text containing the explanation
 * ----------------------------------------------------------------
 */
Datum
alohadb_explain_query(PG_FUNCTION_ARGS)
{
	text	   *sql_text = PG_GETARG_TEXT_PP(0);
	char	   *sql;
	char	   *prompt;
	char	   *explanation;

	sql = text_to_cstring(sql_text);

	prompt = build_explain_prompt(sql);
	explanation = nl2sql_call_llm(prompt);

	pfree(sql);
	pfree(prompt);

	PG_RETURN_TEXT_P(cstring_to_text(explanation));
}

/* ----------------------------------------------------------------
 * _PG_init
 *
 * Module load callback: define custom GUC variables.
 * ----------------------------------------------------------------
 */
void
_PG_init(void)
{
	DefineCustomStringVariable("alohadb.nl2sql_endpoint",
							   "URL of the LLM API endpoint for NL2SQL translation.",
							   "Supports Anthropic Claude, OpenAI, or generic POST endpoints.",
							   &nl2sql_endpoint,
							   "",
							   PGC_SUSET,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable("alohadb.nl2sql_api_key",
							   "API key for authenticating with the LLM endpoint.",
							   NULL,
							   &nl2sql_api_key,
							   "",
							   PGC_SUSET,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable("alohadb.nl2sql_model",
							   "LLM model name to use for NL2SQL translation.",
							   NULL,
							   &nl2sql_model,
							   "claude-sonnet-4-20250514",
							   PGC_SUSET,
							   0,
							   NULL, NULL, NULL);

	MarkGUCPrefixReserved("alohadb");
}
