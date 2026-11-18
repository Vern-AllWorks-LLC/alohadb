/*-------------------------------------------------------------------------
 *
 * pg_vault.h
 *	  Shared declarations for the pg_vault extension.
 *
 *	  Provides an encrypted secrets store backed by pgcrypto's
 *	  PGP symmetric encryption.  Secrets are stored in the
 *	  pg_vault_secrets table and all operations are audited
 *	  in the pg_vault_audit table.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * contrib/pg_vault/pg_vault.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef ALOHADB_VAULT_H
#define ALOHADB_VAULT_H

#include "postgres.h"
#include "fmgr.h"

/*
 * Table names used by the extension.
 */
#define VAULT_SECRETS_TABLE		"pg_vault_secrets"
#define VAULT_AUDIT_TABLE		"pg_vault_audit"

/*
 * GUC variable name for the vault passphrase.
 */
#define VAULT_PASSPHRASE_GUC	"pg_vault.passphrase"

/*
 * GUC prefix to reserve.
 */
#define VAULT_GUC_PREFIX		"pg_vault"

/*
 * Maximum length of a secret key name.
 */
#define VAULT_KEY_MAXLEN		256

/*
 * Maximum length of the passphrase.
 */
#define VAULT_PASSPHRASE_MAXLEN	1024

/* SQL-callable functions */
extern Datum pg_vault_store(PG_FUNCTION_ARGS);
extern Datum pg_vault_fetch(PG_FUNCTION_ARGS);
extern Datum pg_vault_delete(PG_FUNCTION_ARGS);
extern Datum pg_vault_list(PG_FUNCTION_ARGS);
extern Datum pg_vault_rotate_key(PG_FUNCTION_ARGS);

#endif							/* ALOHADB_VAULT_H */
