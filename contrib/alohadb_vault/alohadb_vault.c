/*-------------------------------------------------------------------------
 *
 * alohadb_vault.c
 *	  Main entry point for the alohadb_vault extension.
 *
 *	  Implements an encrypted secrets store that uses pgcrypto's
 *	  pgp_sym_encrypt/pgp_sym_decrypt for symmetric PGP encryption.
 *	  The encryption passphrase is configured via the GUC
 *	  alohadb.vault_passphrase (PGC_SUSET, so only superusers can
 *	  set it).
 *
 *	  All operations are audited in the alohadb_vault_audit table.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_vault/alohadb_vault.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/tupdesc.h"
#include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"

#include "alohadb_vault.h"

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_vault",
					.version = "1.0"
);

/* SQL-callable function declarations */
PG_FUNCTION_INFO_V1(alohadb_vault_store);
PG_FUNCTION_INFO_V1(alohadb_vault_fetch);
PG_FUNCTION_INFO_V1(alohadb_vault_delete);
PG_FUNCTION_INFO_V1(alohadb_vault_list);
PG_FUNCTION_INFO_V1(alohadb_vault_rotate_key);

/* ----------------------------------------------------------------
 * GUC variables
 * ---------------------------------------------------------------- */

/* Passphrase used for PGP symmetric encryption/decryption */
static char *vault_passphrase = NULL;

/* ----------------------------------------------------------------
 * Helper: get the current passphrase, raising an error if unset.
 * ---------------------------------------------------------------- */
static const char *
vault_get_passphrase(void)
{
	if (vault_passphrase == NULL || vault_passphrase[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("alohadb.vault_passphrase is not set"),
				 errhint("Set the passphrase with: ALTER SYSTEM SET alohadb.vault_passphrase = 'your_passphrase';")));

	return vault_passphrase;
}

/* ----------------------------------------------------------------
 * Helper: insert an audit log entry via SPI.
 *
 * This must be called while SPI is already connected.
 * ---------------------------------------------------------------- */
static void
vault_audit_log(const char *operation, const char *key_name)
{
	Oid			argtypes[2] = {TEXTOID, TEXTOID};
	Datum		values[2];
	int			ret;

	values[0] = CStringGetTextDatum(operation);
	values[1] = CStringGetTextDatum(key_name);

	ret = SPI_execute_with_args(
		"INSERT INTO alohadb_vault_audit (operation, key_name) "
		"VALUES ($1, $2)",
		2, argtypes, values, NULL, false, 0);

	if (ret != SPI_OK_INSERT)
		elog(WARNING, "alohadb_vault: failed to insert audit log entry: error code %d", ret);
}

/* ----------------------------------------------------------------
 * _PG_init
 *
 * Module load callback.  Registers the vault_passphrase GUC.
 * ---------------------------------------------------------------- */
void
_PG_init(void)
{
	DefineCustomStringVariable(VAULT_PASSPHRASE_GUC,
							   "Passphrase used for PGP symmetric encryption of vault secrets.",
							   NULL,
							   &vault_passphrase,
							   "",
							   PGC_SUSET,
							   0,
							   NULL, NULL, NULL);

	MarkGUCPrefixReserved(VAULT_GUC_PREFIX);
}

/* ----------------------------------------------------------------
 * alohadb_vault_store
 *
 * Store or update an encrypted secret.  Uses INSERT ... ON CONFLICT
 * to perform an upsert.
 *
 * Args: key text, value text, description text DEFAULT NULL
 * ---------------------------------------------------------------- */
Datum
alohadb_vault_store(PG_FUNCTION_ARGS)
{
	text	   *key_text = PG_GETARG_TEXT_PP(0);
	text	   *value_text = PG_GETARG_TEXT_PP(1);
	text	   *desc_text = PG_ARGISNULL(2) ? NULL : PG_GETARG_TEXT_PP(2);
	const char *passphrase = vault_get_passphrase();
	Oid			argtypes[4] = {TEXTOID, TEXTOID, TEXTOID, TEXTOID};
	Datum		values[4];
	char		nulls[4] = {' ', ' ', ' ', ' '};
	int			ret;

	values[0] = PointerGetDatum(key_text);
	values[1] = PointerGetDatum(value_text);
	values[2] = CStringGetTextDatum(passphrase);

	if (desc_text != NULL)
		values[3] = PointerGetDatum(desc_text);
	else
		nulls[3] = 'n';

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute_with_args(
		"INSERT INTO alohadb_vault_secrets (key, value, description, created_at, updated_at) "
		"VALUES ($1, pgp_sym_encrypt($2, $3), $4, now(), now()) "
		"ON CONFLICT (key) DO UPDATE SET "
		"  value = pgp_sym_encrypt($2, $3), "
		"  description = COALESCE($4, alohadb_vault_secrets.description), "
		"  updated_at = now()",
		4, argtypes, values, nulls, false, 0);

	if (ret != SPI_OK_INSERT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_vault: failed to store secret: error code %d", ret)));

	/* Audit log */
	vault_audit_log("STORE", text_to_cstring(key_text));

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * alohadb_vault_fetch
 *
 * Fetch and decrypt a secret by key.  Returns NULL if not found.
 *
 * Args: key text
 * Returns: text (decrypted value)
 * ---------------------------------------------------------------- */
Datum
alohadb_vault_fetch(PG_FUNCTION_ARGS)
{
	text	   *key_text = PG_GETARG_TEXT_PP(0);
	const char *passphrase = vault_get_passphrase();
	Oid			argtypes[2] = {TEXTOID, TEXTOID};
	Datum		values[2];
	int			ret;
	bool		isnull;
	Datum		result;

	values[0] = PointerGetDatum(key_text);
	values[1] = CStringGetTextDatum(passphrase);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute_with_args(
		"SELECT pgp_sym_decrypt(value, $2) FROM alohadb_vault_secrets "
		"WHERE key = $1",
		2, argtypes, values, NULL, true, 1);

	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_vault: failed to fetch secret: error code %d", ret)));

	if (SPI_processed == 0)
	{
		/* Audit the fetch attempt even if not found */
		vault_audit_log("FETCH_NOT_FOUND", text_to_cstring(key_text));
		PopActiveSnapshot();
		SPI_finish();
		PG_RETURN_NULL();
	}

	/* Extract the decrypted value */
	result = SPI_getbinval(SPI_tuptable->vals[0],
						   SPI_tuptable->tupdesc, 1, &isnull);

	if (isnull)
	{
		vault_audit_log("FETCH", text_to_cstring(key_text));
		PopActiveSnapshot();
		SPI_finish();
		PG_RETURN_NULL();
	}

	/* Copy the result out of SPI memory context */
	result = SPI_datumTransfer(result, false, -1);

	/* Audit the successful fetch */
	vault_audit_log("FETCH", text_to_cstring(key_text));

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_DATUM(result);
}

/* ----------------------------------------------------------------
 * alohadb_vault_delete
 *
 * Delete a secret by key.
 *
 * Args: key text
 * ---------------------------------------------------------------- */
Datum
alohadb_vault_delete(PG_FUNCTION_ARGS)
{
	text	   *key_text = PG_GETARG_TEXT_PP(0);
	Oid			argtypes[1] = {TEXTOID};
	Datum		values[1];
	int			ret;

	values[0] = PointerGetDatum(key_text);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute_with_args(
		"DELETE FROM alohadb_vault_secrets WHERE key = $1",
		1, argtypes, values, NULL, false, 0);

	if (ret != SPI_OK_DELETE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_vault: failed to delete secret: error code %d", ret)));

	/* Audit the deletion */
	vault_audit_log("DELETE", text_to_cstring(key_text));

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * alohadb_vault_list
 *
 * List all secrets with metadata (key, description, created_at,
 * updated_at).  Does NOT return decrypted values.
 *
 * Returns: SETOF record (key text, description text,
 *          created_at timestamptz, updated_at timestamptz)
 * ---------------------------------------------------------------- */
Datum
alohadb_vault_list(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			ret;
	uint64		i;

	InitMaterializedSRF(fcinfo, 0);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(
		"SELECT key, description, created_at, updated_at "
		"FROM alohadb_vault_secrets ORDER BY key",
		true, 0);

	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_vault: failed to list secrets: error code %d", ret)));

	for (i = 0; i < SPI_processed; i++)
	{
		Datum		values[4];
		bool		nulls[4];
		int			j;

		for (j = 0; j < 4; j++)
		{
			Form_pg_attribute attr = TupleDescAttr(SPI_tuptable->tupdesc, j);

			values[j] = SPI_getbinval(SPI_tuptable->vals[i],
									   SPI_tuptable->tupdesc,
									   j + 1, &nulls[j]);

			/* Copy datums out of SPI context */
			if (!nulls[j])
				values[j] = SPI_datumTransfer(values[j],
											   attr->attbyval,
											   attr->attlen);
		}

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	PopActiveSnapshot();
	SPI_finish();

	return (Datum) 0;
}

/* ----------------------------------------------------------------
 * alohadb_vault_rotate_key
 *
 * Re-encrypt all secrets: decrypt with old_passphrase, encrypt
 * with new_passphrase.  Returns the number of secrets rotated.
 *
 * Args: old_passphrase text, new_passphrase text
 * Returns: int (count of rotated secrets)
 * ---------------------------------------------------------------- */
Datum
alohadb_vault_rotate_key(PG_FUNCTION_ARGS)
{
	text	   *old_pass_text = PG_GETARG_TEXT_PP(0);
	text	   *new_pass_text = PG_GETARG_TEXT_PP(1);
	Oid			argtypes[2] = {TEXTOID, TEXTOID};
	Datum		values[2];
	int			ret;
	uint64		rotated;

	values[0] = PointerGetDatum(old_pass_text);
	values[1] = PointerGetDatum(new_pass_text);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute_with_args(
		"UPDATE alohadb_vault_secrets "
		"SET value = pgp_sym_encrypt(pgp_sym_decrypt(value, $1), $2), "
		"    updated_at = now()",
		2, argtypes, values, NULL, false, 0);

	if (ret != SPI_OK_UPDATE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("alohadb_vault: failed to rotate keys: error code %d", ret)));

	rotated = SPI_processed;

	/* Audit the rotation */
	vault_audit_log("ROTATE_KEY", "*");

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_INT32((int32) rotated);
}
