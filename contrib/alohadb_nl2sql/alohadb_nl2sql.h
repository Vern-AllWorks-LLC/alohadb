/*-------------------------------------------------------------------------
 *
 * alohadb_nl2sql.h
 *	  Shared declarations for the alohadb_nl2sql extension.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * contrib/alohadb_nl2sql/alohadb_nl2sql.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef ALOHADB_NL2SQL_H
#define ALOHADB_NL2SQL_H

#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"

/*
 * Maximum size of the schema context string sent to the LLM.
 * This limits how much schema information we pack into the prompt
 * to avoid exceeding typical LLM context windows.
 */
#define NL2SQL_MAX_SCHEMA_CONTEXT_LEN	(32 * 1024)

/*
 * Maximum size of the HTTP response buffer from the LLM API.
 */
#define NL2SQL_MAX_RESPONSE_LEN			(64 * 1024)

/*
 * Maximum size of the request body sent to the LLM API.
 */
#define NL2SQL_MAX_REQUEST_LEN			(128 * 1024)

/*
 * HTTP timeout for LLM API calls, in seconds.
 */
#define NL2SQL_HTTP_TIMEOUT_SECS		60

/* GUC variables (defined in alohadb_nl2sql.c) */
extern char *nl2sql_endpoint;
extern char *nl2sql_api_key;
extern char *nl2sql_model;

/*
 * schema_context.c: build a concise text description of the current
 * database schema suitable for inclusion in an LLM prompt.
 */
extern char *nl2sql_build_schema_context(void);

/*
 * Utility: call the configured LLM endpoint with the given prompt and
 * return the response text.  Returns a palloc'd string, or ereport's
 * on failure.
 */
extern char *nl2sql_call_llm(const char *prompt);

/*
 * Utility: extract SQL from an LLM response string.  Handles common
 * patterns like markdown code fences.  Returns a palloc'd string.
 */
extern char *nl2sql_extract_sql(const char *response);

#endif							/* ALOHADB_NL2SQL_H */
