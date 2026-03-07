/*-------------------------------------------------------------------------
 *
 * alohadb_pool.h
 *	  Shared declarations for the alohadb_pool extension.
 *
 *	  Provides a built-in connection pooler as a background worker that
 *	  proxies the PostgreSQL wire protocol between clients and
 *	  pre-established backend connections.  For transaction-level pooling,
 *	  the backend is released back to the pool on ReadyForQuery with 'I'
 *	  (idle) status.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * contrib/alohadb_pool/alohadb_pool.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef ALOHADB_POOL_H
#define ALOHADB_POOL_H

#include "postgres.h"
#include "fmgr.h"

/*
 * Maximum number of named pools tracked in shared memory.
 */
#define POOL_MAX_POOLS		64

/*
 * Maximum length (including NUL) for a pool name.
 */
#define POOL_MAX_NAME_LEN	64

/*
 * Per-pool statistics, stored in shared memory.
 */
typedef struct PoolEntry
{
	char	name[POOL_MAX_NAME_LEN];
	int		active;
	int		idle;
	int		waiting;
	int64	total_served;
	int		pool_size;
} PoolEntry;

/*
 * Top-level shared memory state for the connection pooler.
 */
typedef struct PoolSharedState
{
	LWLock	   *lock;
	int			num_pools;
	PoolEntry	pools[POOL_MAX_POOLS];
	int			pool_port;
	int			pool_size;
	bool		running;
} PoolSharedState;

/* Background worker entry point (exported for postmaster) */
extern PGDLLEXPORT void pool_main(Datum main_arg);

/* SQL-callable functions */
extern Datum pool_status(PG_FUNCTION_ARGS);
extern Datum pool_reset(PG_FUNCTION_ARGS);
extern Datum pool_settings(PG_FUNCTION_ARGS);

#endif							/* ALOHADB_POOL_H */
