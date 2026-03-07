/*-------------------------------------------------------------------------
 *
 * alohadb_audit.c
 *	  DML+DDL audit logging extension for AlohaDB.
 *
 *	  Intercepts INSERT, UPDATE, DELETE operations via ExecutorEnd_hook
 *	  and DDL (CREATE/ALTER/DROP ROLE, etc.) via ProcessUtility_hook.
 *	  Writes structured audit log entries (CSV or JSON) to a
 *	  configurable central directory with timestamps+timezone.
 *	  Optionally encrypts log lines with AES-256-GCM (OpenSSL).
 *
 *	  Must be loaded via shared_preload_libraries.
 *
 * Prior art: pgAudit (2014), Oracle Audit Vault (2006), IBM Guardium,
 *            PostgreSQL log_statement, NIST AES-256-GCM (SP 800-38D, 2007).
 *
 * Copyright (c) 2025-2026, OpenCAN / AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_audit/alohadb_audit.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <openssl/evp.h>
#include <openssl/rand.h>

#include "access/xact.h"
#include "catalog/namespace.h"
#include "commands/dbcommands.h"
#include "common/file_perm.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "nodes/plannodes.h"
#include "postmaster/bgworker.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC_EXT(
	.name = "alohadb_audit",
	.version = "1.1"
);

/* SQL-callable functions */
PG_FUNCTION_INFO_V1(audit_log_status);
PG_FUNCTION_INFO_V1(audit_decrypt_log);

/* ----------------------------------------------------------------
 * GUC variables
 * ---------------------------------------------------------------- */
static bool audit_enabled = false;
static char *audit_log_directory = NULL;
static int audit_log_format = 0;	/* 0=csv, 1=json */
static char *audit_databases = NULL;
static char *audit_operations = NULL;
static bool audit_log_query_text = true;
static char *audit_encryption_key = NULL;	/* hex-encoded 256-bit key */

/* Enum options for log format */
static const struct config_enum_entry audit_format_options[] = {
	{"csv", 0, false},
	{"json", 1, false},
	{NULL, 0, false}
};

/* Saved hook values */
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;
static ProcessUtility_hook_type prev_ProcessUtility_hook = NULL;

/* ----------------------------------------------------------------
 * AES-256-GCM encryption helpers
 *
 * Line format when encrypted: "ENC:" + base64(nonce_12 + ciphertext + tag_16)
 * Standard NIST SP 800-38D pattern, same as AWS Secrets Manager,
 * Ansible Vault, HashiCorp Vault transit engine.
 * ---------------------------------------------------------------- */

/* Base64 encode (simple, no line breaks) */
static char *
b64_encode(const unsigned char *data, int len)
{
	static const char b64[] =
		"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
	int		outlen = ((len + 2) / 3) * 4;
	char   *out = palloc(outlen + 1);
	int		i, j;

	for (i = 0, j = 0; i < len; i += 3, j += 4)
	{
		int		n = (data[i] << 16) |
					(i + 1 < len ? data[i + 1] << 8 : 0) |
					(i + 2 < len ? data[i + 2] : 0);
		out[j]     = b64[(n >> 18) & 0x3f];
		out[j + 1] = b64[(n >> 12) & 0x3f];
		out[j + 2] = (i + 1 < len) ? b64[(n >> 6) & 0x3f] : '=';
		out[j + 3] = (i + 2 < len) ? b64[n & 0x3f] : '=';
	}
	out[outlen] = '\0';
	return out;
}

/* Base64 decode */
static int
b64_decode(const char *src, int srclen, unsigned char *dst)
{
	static const unsigned char d[] = {
		255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,
		255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,
		255,255,255,255,255,255,255,255,255,255,255, 62,255,255,255, 63,
		 52, 53, 54, 55, 56, 57, 58, 59, 60, 61,255,255,255,  0,255,255,
		255,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,
		 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,255,255,255,255,255,
		255, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
		 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51,255,255,255,255,255
	};
	int		i, j, pad = 0;
	unsigned char *p = dst;

	if (srclen > 0 && src[srclen - 1] == '=') pad++;
	if (srclen > 1 && src[srclen - 2] == '=') pad++;

	for (i = 0, j = 0; i < srclen; i += 4, j += 3)
	{
		unsigned int n = 0;
		int k;
		for (k = 0; k < 4 && (i + k) < srclen; k++)
		{
			unsigned char c = (unsigned char) src[i + k];
			if (c >= 128 || d[c] == 255)
				n = (n << 6);
			else
				n = (n << 6) | d[c];
		}
		*p++ = (n >> 16) & 0xff;
		if (j + 1 < (srclen * 3 / 4) - pad) *p++ = (n >> 8) & 0xff;
		if (j + 2 < (srclen * 3 / 4) - pad) *p++ = n & 0xff;
	}
	return (int)(p - dst);
}

/* Parse hex key string to 32-byte key */
static bool
parse_hex_key(const char *hex, unsigned char *key)
{
	int		i;

	if (hex == NULL || strlen(hex) != 64)
		return false;

	for (i = 0; i < 32; i++)
	{
		unsigned int byte;
		if (sscanf(hex + i * 2, "%02x", &byte) != 1)
			return false;
		key[i] = (unsigned char) byte;
	}
	return true;
}

/* Encrypt a line with AES-256-GCM. Returns "ENC:<base64>" or NULL on failure. */
static char *
audit_encrypt_line(const char *plaintext)
{
	unsigned char key[32];
	unsigned char nonce[12];
	unsigned char *ciphertext;
	unsigned char tag[16];
	int			pt_len, ct_len, outlen;
	EVP_CIPHER_CTX *ctx;
	unsigned char *combined;
	int			combined_len;
	char	   *encoded;
	char	   *result;

	if (!parse_hex_key(audit_encryption_key, key))
		return NULL;

	pt_len = strlen(plaintext);
	ciphertext = palloc(pt_len + 16);

	/* Generate random 12-byte nonce */
	if (RAND_bytes(nonce, 12) != 1)
	{
		pfree(ciphertext);
		return NULL;
	}

	ctx = EVP_CIPHER_CTX_new();
	if (!ctx)
	{
		pfree(ciphertext);
		return NULL;
	}

	if (EVP_EncryptInit_ex(ctx, EVP_aes_256_gcm(), NULL, NULL, NULL) != 1 ||
		EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, 12, NULL) != 1 ||
		EVP_EncryptInit_ex(ctx, NULL, NULL, key, nonce) != 1 ||
		EVP_EncryptUpdate(ctx, ciphertext, &outlen, (unsigned char *) plaintext, pt_len) != 1)
	{
		EVP_CIPHER_CTX_free(ctx);
		pfree(ciphertext);
		return NULL;
	}
	ct_len = outlen;

	if (EVP_EncryptFinal_ex(ctx, ciphertext + ct_len, &outlen) != 1)
	{
		EVP_CIPHER_CTX_free(ctx);
		pfree(ciphertext);
		return NULL;
	}
	ct_len += outlen;

	if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, 16, tag) != 1)
	{
		EVP_CIPHER_CTX_free(ctx);
		pfree(ciphertext);
		return NULL;
	}
	EVP_CIPHER_CTX_free(ctx);

	/* Combine: nonce(12) + ciphertext + tag(16) */
	combined_len = 12 + ct_len + 16;
	combined = palloc(combined_len);
	memcpy(combined, nonce, 12);
	memcpy(combined + 12, ciphertext, ct_len);
	memcpy(combined + 12 + ct_len, tag, 16);
	pfree(ciphertext);

	encoded = b64_encode(combined, combined_len);
	pfree(combined);

	/* Prefix with "ENC:" */
	result = palloc(4 + strlen(encoded) + 1);
	sprintf(result, "ENC:%s", encoded);
	pfree(encoded);

	return result;
}

/* Decrypt an "ENC:<base64>" line. Returns palloc'd plaintext or NULL. */
static char *
audit_decrypt_line(const char *encrypted, const unsigned char *key)
{
	const char *b64data;
	int			b64len;
	unsigned char *combined;
	int			combined_len;
	unsigned char *nonce;
	unsigned char *ct;
	unsigned char *tag;
	int			ct_len;
	unsigned char *plaintext;
	int			pt_len, outlen;
	EVP_CIPHER_CTX *ctx;

	if (strncmp(encrypted, "ENC:", 4) != 0)
		return NULL;

	b64data = encrypted + 4;
	b64len = strlen(b64data);

	combined = palloc(b64len);		/* oversize is fine */
	combined_len = b64_decode(b64data, b64len, combined);

	if (combined_len < 12 + 16)
	{
		pfree(combined);
		return NULL;
	}

	nonce = combined;
	ct = combined + 12;
	ct_len = combined_len - 12 - 16;
	tag = combined + 12 + ct_len;

	plaintext = palloc(ct_len + 1);

	ctx = EVP_CIPHER_CTX_new();
	if (!ctx)
	{
		pfree(combined);
		pfree(plaintext);
		return NULL;
	}

	if (EVP_DecryptInit_ex(ctx, EVP_aes_256_gcm(), NULL, NULL, NULL) != 1 ||
		EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, 12, NULL) != 1 ||
		EVP_DecryptInit_ex(ctx, NULL, NULL, key, nonce) != 1 ||
		EVP_DecryptUpdate(ctx, plaintext, &outlen, ct, ct_len) != 1)
	{
		EVP_CIPHER_CTX_free(ctx);
		pfree(combined);
		pfree(plaintext);
		return NULL;
	}
	pt_len = outlen;

	if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, 16, tag) != 1 ||
		EVP_DecryptFinal_ex(ctx, plaintext + pt_len, &outlen) != 1)
	{
		EVP_CIPHER_CTX_free(ctx);
		pfree(combined);
		pfree(plaintext);
		return NULL;
	}
	pt_len += outlen;
	EVP_CIPHER_CTX_free(ctx);
	pfree(combined);

	plaintext[pt_len] = '\0';
	return (char *) plaintext;
}

/* ----------------------------------------------------------------
 * Password redaction patterns (applied on decrypt, not on write).
 *
 * Same approach as pgAudit log redaction, Oracle Data Redaction,
 * PostgreSQL log_min_error_statement filtering.
 * ---------------------------------------------------------------- */
static char *
audit_redact_passwords(const char *line)
{
	/*
	 * Patterns to redact (case-insensitive):
	 *   PASSWORD 'xxx'  →  PASSWORD '***'
	 *   PASSWORD "xxx"  →  PASSWORD "***"
	 *   password=xxx    →  password=***
	 *   md5...          →  md5***  (32+ hex chars after md5)
	 *   SCRAM-SHA-256$  →  SCRAM-SHA-256$***
	 */
	StringInfoData buf;
	const char *p = line;
	int			len = strlen(line);

	initStringInfo(&buf);

	while (p < line + len)
	{
		/* Case-insensitive match for PASSWORD */
		if (pg_strncasecmp(p, "password", 8) == 0)
		{
			const char *after = p + 8;

			/* PASSWORD 'xxx' or PASSWORD "xxx" */
			if (*after == ' ' || *after == '\t')
			{
				/* Skip whitespace */
				const char *q = after;
				while (*q == ' ' || *q == '\t')
					q++;

				if (*q == '\'' || *q == '"')
				{
					char		quote = *q;
					const char *end = q + 1;

					while (*end && *end != quote)
					{
						if (*end == '\\' && *(end + 1))
							end++;
						end++;
					}
					if (*end == quote)
						end++;

					appendStringInfoString(&buf, "PASSWORD ");
					appendStringInfoChar(&buf, quote);
					appendStringInfoString(&buf, "***");
					appendStringInfoChar(&buf, quote);
					p = end;
					continue;
				}
			}
			/* password=xxx (up to whitespace, comma, or end) */
			else if (*after == '=')
			{
				const char *end = after + 1;
				while (*end && *end != ' ' && *end != ',' && *end != ';'
					   && *end != ')' && *end != '\n')
					end++;
				appendStringInfoString(&buf, "password=***");
				p = end;
				continue;
			}
		}

		/* SCRAM-SHA-256$ pattern */
		if (pg_strncasecmp(p, "SCRAM-SHA-256$", 14) == 0)
		{
			appendStringInfoString(&buf, "SCRAM-SHA-256$***");
			p += 14;
			/* Skip rest of hash */
			while (*p && *p != ',' && *p != '\'' && *p != '"' && *p != ' '
				   && *p != '\n')
				p++;
			continue;
		}

		/* md5 followed by 32+ hex chars */
		if (p[0] == 'm' && p[1] == 'd' && p[2] == '5')
		{
			const char *q = p + 3;
			int			hexcount = 0;
			while ((*q >= '0' && *q <= '9') || (*q >= 'a' && *q <= 'f') ||
				   (*q >= 'A' && *q <= 'F'))
			{
				q++;
				hexcount++;
			}
			if (hexcount >= 32)
			{
				appendStringInfoString(&buf, "md5***");
				p = q;
				continue;
			}
		}

		appendStringInfoChar(&buf, *p);
		p++;
	}

	return buf.data;
}

/* ----------------------------------------------------------------
 * Helper: check if current database is in the audit_databases list
 * ---------------------------------------------------------------- */
static bool
audit_check_database(void)
{
	const char *dbname;
	char	   *list_copy;
	char	   *token;
	char	   *saveptr;

	if (audit_databases == NULL || audit_databases[0] == '\0')
		return false;

	/* '*' means all databases */
	if (strcmp(audit_databases, "*") == 0)
		return true;

	dbname = get_database_name(MyDatabaseId);
	if (dbname == NULL)
		return false;

	list_copy = pstrdup(audit_databases);
	for (token = strtok_r(list_copy, ",", &saveptr);
		 token != NULL;
		 token = strtok_r(NULL, ",", &saveptr))
	{
		char   *end;

		/* trim whitespace */
		while (*token == ' ')
			token++;
		end = token + strlen(token) - 1;
		while (end > token && *end == ' ')
			*end-- = '\0';

		if (strcmp(token, dbname) == 0)
		{
			pfree(list_copy);
			return true;
		}
	}

	pfree(list_copy);
	return false;
}

/* ----------------------------------------------------------------
 * Helper: check if the given operation is in audit_operations
 * ---------------------------------------------------------------- */
static bool
audit_check_operation(const char *op_str)
{
	char	   *list_copy;
	char	   *token;
	char	   *saveptr;

	if (audit_operations == NULL || audit_operations[0] == '\0')
		return false;

	list_copy = pstrdup(audit_operations);
	for (token = strtok_r(list_copy, ",", &saveptr);
		 token != NULL;
		 token = strtok_r(NULL, ",", &saveptr))
	{
		char   *end;

		while (*token == ' ')
			token++;
		end = token + strlen(token) - 1;
		while (end > token && *end == ' ')
			*end-- = '\0';

		if (pg_strcasecmp(token, op_str) == 0)
		{
			pfree(list_copy);
			return true;
		}
	}

	pfree(list_copy);
	return false;
}

/* ----------------------------------------------------------------
 * Helper: escape a string for CSV (double-quote if contains comma,
 * quote, or newline)
 * ---------------------------------------------------------------- */
static void
audit_csv_escape(StringInfo buf, const char *str)
{
	bool		needs_quote = false;
	const char *p;

	if (str == NULL)
	{
		appendStringInfoString(buf, "");
		return;
	}

	for (p = str; *p; p++)
	{
		if (*p == ',' || *p == '"' || *p == '\n' || *p == '\r')
		{
			needs_quote = true;
			break;
		}
	}

	if (needs_quote)
	{
		appendStringInfoChar(buf, '"');
		for (p = str; *p; p++)
		{
			if (*p == '"')
				appendStringInfoChar(buf, '"');
			appendStringInfoChar(buf, *p);
		}
		appendStringInfoChar(buf, '"');
	}
	else
	{
		appendStringInfoString(buf, str);
	}
}

/* ----------------------------------------------------------------
 * Helper: escape a string for JSON value
 * ---------------------------------------------------------------- */
static void
audit_json_escape(StringInfo buf, const char *str)
{
	const char *p;

	if (str == NULL)
	{
		appendStringInfoString(buf, "null");
		return;
	}

	appendStringInfoChar(buf, '"');
	for (p = str; *p; p++)
	{
		switch (*p)
		{
			case '"':
				appendStringInfoString(buf, "\\\"");
				break;
			case '\\':
				appendStringInfoString(buf, "\\\\");
				break;
			case '\n':
				appendStringInfoString(buf, "\\n");
				break;
			case '\r':
				appendStringInfoString(buf, "\\r");
				break;
			case '\t':
				appendStringInfoString(buf, "\\t");
				break;
			default:
				appendStringInfoChar(buf, *p);
				break;
		}
	}
	appendStringInfoChar(buf, '"');
}

/* ----------------------------------------------------------------
 * Helper: get current timestamp as ISO 8601 string with timezone
 * ---------------------------------------------------------------- */
static void
audit_get_timestamp(char *buf, size_t buflen)
{
	TimestampTz now = GetCurrentTimestamp();
	struct pg_tm tm;
	fsec_t		fsec;
	int			tz;
	const char *tzn;

	if (timestamp2tm(now, &tz, &tm, &fsec, &tzn, NULL) == 0)
	{
		int		tz_hour = -(tz / 3600);
		int		tz_min = abs((tz % 3600) / 60);

		snprintf(buf, buflen,
				 "%04d-%02d-%02dT%02d:%02d:%02d.%03d%+03d:%02d",
				 tm.tm_year, tm.tm_mon, tm.tm_mday,
				 tm.tm_hour, tm.tm_min, tm.tm_sec,
				 (int) (fsec / 1000),
				 tz_hour, tz_min);
	}
	else
	{
		snprintf(buf, buflen, "unknown");
	}
}

/* ----------------------------------------------------------------
 * Helper: write audit log entry to file (with optional encryption)
 * ---------------------------------------------------------------- */
static void
audit_write_log(const char *line)
{
	char		datebuf[16];
	char		filepath[MAXPGPATH];
	int			fd;
	const char *to_write;
	char	   *encrypted = NULL;
	TimestampTz now = GetCurrentTimestamp();
	struct pg_tm tm;
	fsec_t		fsec;
	int			tz;

	if (audit_log_directory == NULL || audit_log_directory[0] == '\0')
		return;

	/* Create directory if needed */
	if (pg_mkdir_p(audit_log_directory, pg_dir_create_mode) != 0 &&
		errno != EEXIST)
	{
		elog(WARNING, "alohadb_audit: could not create directory \"%s\": %m",
			 audit_log_directory);
		return;
	}

	/* Build daily filename */
	if (timestamp2tm(now, &tz, &tm, &fsec, NULL, NULL) == 0)
		snprintf(datebuf, sizeof(datebuf), "%04d-%02d-%02d",
				 tm.tm_year, tm.tm_mon, tm.tm_mday);
	else
		snprintf(datebuf, sizeof(datebuf), "unknown");

	/* Encrypt if key is configured */
	if (audit_encryption_key != NULL && audit_encryption_key[0] != '\0')
	{
		encrypted = audit_encrypt_line(line);
		if (encrypted)
		{
			to_write = encrypted;
			/* Use .enc extension for encrypted logs */
			snprintf(filepath, sizeof(filepath), "%s/audit-%s.enc.log",
					 audit_log_directory, datebuf);
		}
		else
		{
			/* Encryption failed, fall back to plaintext with warning */
			elog(WARNING, "alohadb_audit: encryption failed, writing plaintext");
			to_write = line;
			snprintf(filepath, sizeof(filepath), "%s/audit-%s.log",
					 audit_log_directory, datebuf);
		}
	}
	else
	{
		to_write = line;
		snprintf(filepath, sizeof(filepath), "%s/audit-%s.log",
				 audit_log_directory, datebuf);
	}

	/* Open with O_APPEND for safe concurrent writes */
	fd = open(filepath, O_WRONLY | O_CREAT | O_APPEND, pg_file_create_mode);
	if (fd < 0)
	{
		elog(WARNING, "alohadb_audit: could not open \"%s\": %m", filepath);
		if (encrypted)
			pfree(encrypted);
		return;
	}

	/* Write the line + newline */
	{
		int		len = strlen(to_write);
		char	nl = '\n';
		ssize_t	rc;

		rc = write(fd, to_write, len);
		if (rc == len)
			rc = write(fd, &nl, 1);
		if (rc < 0)
			elog(WARNING, "alohadb_audit: could not write to \"%s\": %m",
				 filepath);
	}

	close(fd);

	if (encrypted)
		pfree(encrypted);
}

/* ----------------------------------------------------------------
 * Helper: build a log line for a given operation
 * ---------------------------------------------------------------- */
static void
audit_log_entry(const char *op_str, const char *table_ref,
				uint64 rows_affected, const char *source_text)
{
	const char *dbname;
	const char *username;
	const char *client_addr = "";
	char		tsbuf[64];
	StringInfoData buf;

	/* Database name */
	dbname = get_database_name(MyDatabaseId);

	/* Username */
	username = GetUserNameFromId(GetUserId(), true);
	if (username == NULL)
		username = "unknown";

	/* Client address */
	if (MyProcPort != NULL)
	{
		if (MyProcPort->remote_hostname != NULL &&
			MyProcPort->remote_hostname[0] != '\0')
			client_addr = MyProcPort->remote_hostname;
		else if (MyProcPort->remote_host != NULL &&
				 MyProcPort->remote_host[0] != '\0')
			client_addr = MyProcPort->remote_host;
	}

	/* Timestamp */
	audit_get_timestamp(tsbuf, sizeof(tsbuf));

	initStringInfo(&buf);

	if (audit_log_format == 0)	/* CSV */
	{
		/* ts,db,user,client,op,table,rows,query,xid,pid */
		appendStringInfoString(&buf, tsbuf);
		appendStringInfoChar(&buf, ',');
		audit_csv_escape(&buf, dbname);
		appendStringInfoChar(&buf, ',');
		audit_csv_escape(&buf, username);
		appendStringInfoChar(&buf, ',');
		audit_csv_escape(&buf, client_addr);
		appendStringInfoChar(&buf, ',');
		appendStringInfoString(&buf, op_str);
		appendStringInfoChar(&buf, ',');
		if (table_ref)
			appendStringInfoString(&buf, table_ref);
		appendStringInfoChar(&buf, ',');
		appendStringInfo(&buf, "%lu", (unsigned long) rows_affected);
		appendStringInfoChar(&buf, ',');
		if (audit_log_query_text && source_text)
			audit_csv_escape(&buf, source_text);
		appendStringInfoChar(&buf, ',');
		appendStringInfo(&buf, "%u", GetTopTransactionIdIfAny());
		appendStringInfoChar(&buf, ',');
		appendStringInfo(&buf, "%d", MyProcPid);
	}
	else	/* JSON */
	{
		appendStringInfoChar(&buf, '{');
		appendStringInfo(&buf, "\"ts\":\"%s\"", tsbuf);

		appendStringInfoString(&buf, ",\"db\":");
		audit_json_escape(&buf, dbname);

		appendStringInfoString(&buf, ",\"user\":");
		audit_json_escape(&buf, username);

		appendStringInfoString(&buf, ",\"client\":");
		audit_json_escape(&buf, client_addr);

		appendStringInfo(&buf, ",\"op\":\"%s\"", op_str);

		appendStringInfoString(&buf, ",\"table\":\"");
		if (table_ref)
			appendStringInfoString(&buf, table_ref);
		appendStringInfoChar(&buf, '"');

		appendStringInfo(&buf, ",\"rows\":%lu", (unsigned long) rows_affected);

		if (audit_log_query_text && source_text)
		{
			appendStringInfoString(&buf, ",\"query\":");
			audit_json_escape(&buf, source_text);
		}

		appendStringInfo(&buf, ",\"xid\":%u", GetTopTransactionIdIfAny());
		appendStringInfo(&buf, ",\"pid\":%d", MyProcPid);

		appendStringInfoChar(&buf, '}');
	}

	audit_write_log(buf.data);
	pfree(buf.data);
}

/* ----------------------------------------------------------------
 * ExecutorEnd hook — DML audit logic
 * ---------------------------------------------------------------- */
static void
audit_ExecutorEnd(QueryDesc *queryDesc)
{
	CmdType		operation;
	uint64		rows_affected;
	const char *source_text;

	/*
	 * Capture info BEFORE calling the previous hook / standard function,
	 * because standard_ExecutorEnd may free resources we need.
	 */
	operation = queryDesc->operation;
	rows_affected = queryDesc->estate->es_processed;
	source_text = queryDesc->sourceText;

	/* Only care about DML */
	if (audit_enabled &&
		(operation == CMD_INSERT || operation == CMD_UPDATE || operation == CMD_DELETE) &&
		audit_check_database())
	{
		const char *op_str;
		char	   *relname = NULL;
		char	   *nspname = NULL;
		char		table_ref[256] = "";

		/* Operation name */
		switch (operation)
		{
			case CMD_INSERT:
				op_str = "INSERT";
				break;
			case CMD_UPDATE:
				op_str = "UPDATE";
				break;
			case CMD_DELETE:
				op_str = "DELETE";
				break;
			default:
				op_str = "UNKNOWN";
				break;
		}

		if (!audit_check_operation(op_str))
			goto chain;

		/* Get table name from result relations */
		if (queryDesc->plannedstmt->resultRelations != NIL)
		{
			int		rtindex = linitial_int(queryDesc->plannedstmt->resultRelations);
			RangeTblEntry *rte = list_nth(queryDesc->plannedstmt->rtable, rtindex - 1);

			if (rte->rtekind == RTE_RELATION)
			{
				relname = get_rel_name(rte->relid);
				nspname = get_namespace_name(get_rel_namespace(rte->relid));
			}
		}

		if (nspname && relname)
			snprintf(table_ref, sizeof(table_ref), "%s.%s", nspname, relname);
		else if (relname)
			snprintf(table_ref, sizeof(table_ref), "%s", relname);

		audit_log_entry(op_str, table_ref, rows_affected, source_text);

		if (relname)
			pfree(relname);
		if (nspname)
			pfree(nspname);
	}

chain:
	/* Chain to previous hook or standard function */
	if (prev_ExecutorEnd_hook)
		prev_ExecutorEnd_hook(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

/* ----------------------------------------------------------------
 * ProcessUtility hook — DDL audit logic
 *
 * Captures CREATE ROLE, ALTER ROLE, DROP ROLE, GRANT, REVOKE,
 * CREATE/ALTER/DROP DATABASE, and other DDL statements.
 * Same pattern as pgAudit (2014).
 * ---------------------------------------------------------------- */
static void
audit_ProcessUtility(PlannedStmt *pstmt,
					 const char *queryString,
					 bool readOnlyTree,
					 ProcessUtilityContext context,
					 ParamListInfo params,
					 QueryEnvironment *queryEnv,
					 DestReceiver *dest,
					 QueryCompletion *qc)
{
	NodeTag		tag;
	const char *op_str = NULL;

	/* Call the previous hook or standard first, so we log after success */
	if (prev_ProcessUtility_hook)
		prev_ProcessUtility_hook(pstmt, queryString, readOnlyTree, context,
								params, queryEnv, dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString, readOnlyTree, context,
								params, queryEnv, dest, qc);

	/* Now log if enabled */
	if (!audit_enabled || !audit_check_database())
		return;

	tag = nodeTag(pstmt->utilityStmt);

	switch (tag)
	{
		case T_CreateRoleStmt:
			op_str = "CREATE_ROLE";
			break;
		case T_AlterRoleStmt:
			op_str = "ALTER_ROLE";
			break;
		case T_DropRoleStmt:
			op_str = "DROP_ROLE";
			break;
		case T_GrantStmt:
			op_str = "GRANT";
			break;
		case T_GrantRoleStmt:
			op_str = "GRANT_ROLE";
			break;
		case T_CreatedbStmt:
			op_str = "CREATE_DATABASE";
			break;
		case T_AlterDatabaseStmt:
			op_str = "ALTER_DATABASE";
			break;
		case T_DropdbStmt:
			op_str = "DROP_DATABASE";
			break;
		case T_AlterSystemStmt:
			op_str = "ALTER_SYSTEM";
			break;
		default:
			/* Only audit DDL we specifically care about */
			return;
	}

	if (!audit_check_operation(op_str))
		return;

	/* Log the DDL with the full query text */
	audit_log_entry(op_str, NULL, 0, queryString);
}

/* ----------------------------------------------------------------
 * _PG_init — module load callback
 * ---------------------------------------------------------------- */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	/* Define GUC variables */
	DefineCustomBoolVariable("alohadb.audit_enabled",
							 "Enable DML/DDL audit logging.",
							 NULL,
							 &audit_enabled,
							 false,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	DefineCustomStringVariable("alohadb.audit_log_directory",
							   "Directory for audit log files.",
							   NULL,
							   &audit_log_directory,
							   "/var/log/alohadb/audit",
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	DefineCustomEnumVariable("alohadb.audit_log_format",
							 "Format for audit log entries (csv or json).",
							 NULL,
							 &audit_log_format,
							 0,	/* csv */
							 audit_format_options,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	DefineCustomStringVariable("alohadb.audit_databases",
							   "Comma-separated list of databases to audit, or '*' for all.",
							   NULL,
							   &audit_databases,
							   "*",
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable("alohadb.audit_operations",
							   "Comma-separated list of operations to audit "
							   "(insert,update,delete,create_role,alter_role,"
							   "drop_role,grant,grant_role,create_database,"
							   "alter_database,drop_database,alter_system).",
							   NULL,
							   &audit_operations,
							   "insert,update,delete,create_role,alter_role,drop_role,grant,alter_system",
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	DefineCustomBoolVariable("alohadb.audit_log_query_text",
							 "Include SQL query text in audit log entries.",
							 NULL,
							 &audit_log_query_text,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	DefineCustomStringVariable("alohadb.audit_encryption_key",
							   "Hex-encoded 256-bit key for AES-256-GCM log encryption. "
							   "Empty = no encryption.",
							   NULL,
							   &audit_encryption_key,
							   "",
							   PGC_SIGHUP,
							   GUC_SUPERUSER_ONLY,
							   NULL, NULL, NULL);

	MarkGUCPrefixReserved("alohadb.audit");

	/* Install ExecutorEnd hook for DML */
	prev_ExecutorEnd_hook = ExecutorEnd_hook;
	ExecutorEnd_hook = audit_ExecutorEnd;

	/* Install ProcessUtility hook for DDL */
	prev_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = audit_ProcessUtility;
}

/* ----------------------------------------------------------------
 * audit_log_status — show current audit configuration
 * ---------------------------------------------------------------- */
Datum
audit_log_status(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[7];
	bool		nulls[7] = {false};
	HeapTuple	tuple;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context that cannot accept type record")));

	tupdesc = BlessTupleDesc(tupdesc);

	values[0] = BoolGetDatum(audit_enabled);
	values[1] = CStringGetTextDatum(audit_log_directory ? audit_log_directory : "");
	values[2] = CStringGetTextDatum(audit_log_format == 0 ? "csv" : "json");
	values[3] = CStringGetTextDatum(audit_databases ? audit_databases : "");
	values[4] = CStringGetTextDatum(audit_operations ? audit_operations : "");
	values[5] = BoolGetDatum(audit_log_query_text);
	values[6] = BoolGetDatum(audit_encryption_key != NULL && audit_encryption_key[0] != '\0');

	tuple = heap_form_tuple(tupdesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/* ----------------------------------------------------------------
 * audit_decrypt_log — decrypt + redact an encrypted log line
 *
 * Usage: SELECT audit_decrypt_log('ENC:base64...', 'hex_key', true);
 *   - line: the encrypted log line
 *   - key: hex-encoded 256-bit AES key
 *   - redact: if true, apply password redaction patterns
 * ---------------------------------------------------------------- */
Datum
audit_decrypt_log(PG_FUNCTION_ARGS)
{
	text	   *line_text;
	text	   *key_text;
	bool		redact;
	char	   *line;
	char	   *key_hex;
	unsigned char key[32];
	char	   *decrypted;
	char	   *result;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	line_text = PG_GETARG_TEXT_PP(0);
	key_text = PG_GETARG_TEXT_PP(1);
	redact = PG_ARGISNULL(2) ? true : PG_GETARG_BOOL(2);

	line = text_to_cstring(line_text);
	key_hex = text_to_cstring(key_text);

	if (!parse_hex_key(key_hex, key))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("key must be a 64-character hex string (256-bit)")));

	decrypted = audit_decrypt_line(line, key);
	if (decrypted == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("decryption failed — invalid key or corrupted data")));

	if (redact)
	{
		result = audit_redact_passwords(decrypted);
		pfree(decrypted);
	}
	else
	{
		result = decrypted;
	}

	PG_RETURN_TEXT_P(cstring_to_text(result));
}
