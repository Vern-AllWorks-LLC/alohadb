/*-------------------------------------------------------------------------
 *
 * alohadb_vault.h
 *	  Shared declarations for the alohadb_vault extension.
 *
 *	  Provides an encrypted secrets store backed by pgcrypto's
 *	  PGP symmetric encryption.  Secrets are stored in the
 *	  alohadb_vault_secrets table and all operations are audited
 *	  in the alohadb_vault_audit table.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * contrib/alohadb_vault/alohadb_vault.h
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
#define VAULT_SECRETS_TABLE		"alohadb_vault_secrets"
#define VAULT_AUDIT_TABLE		"alohadb_vault_audit"

/*
 * GUC variable name for the vault passphrase.
 */
#define VAULT_PASSPHRASE_GUC	"alohadb.vault_passphrase"

/*
 * GUC prefix to reserve.
 */
#define VAULT_GUC_PREFIX		"alohadb.vault"

/*
 * Maximum length of a secret key name.
 */
#define VAULT_KEY_MAXLEN		256

/*
 * Maximum length of the passphrase.
 */
#define VAULT_PASSPHRASE_MAXLEN	1024

/* SQL-callable functions */
extern Datum alohadb_vault_store(PG_FUNCTION_ARGS);
extern Datum alohadb_vault_fetch(PG_FUNCTION_ARGS);
extern Datum alohadb_vault_delete(PG_FUNCTION_ARGS);
extern Datum alohadb_vault_list(PG_FUNCTION_ARGS);
extern Datum alohadb_vault_rotate_key(PG_FUNCTION_ARGS);

#endif							/* ALOHADB_VAULT_H */
