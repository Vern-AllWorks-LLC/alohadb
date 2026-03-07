/*-------------------------------------------------------------------------
 *
 * tablecmds_online.h
 *	  Declarations for non-blocking online DDL helpers.
 *
 * This header declares helper functions and the GUC variable used to
 * implement Phase 6.2 non-blocking online DDL in ALTER TABLE.  When the
 * GUC enable_online_ddl is true (the default), certain ALTER TABLE
 * operations avoid holding AccessExclusiveLock for the entire duration
 * of the command, allowing concurrent reads and writes where possible.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/tablecmds_online.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TABLECMDS_ONLINE_H
#define TABLECMDS_ONLINE_H

#include "utils/relcache.h"

/* GUC variable */
extern PGDLLIMPORT bool enable_online_ddl;

/*
 * OnlineDDLCanUseFastSetNotNull
 *		Returns true when SET NOT NULL can use the reduced-lock path.
 *
 * The reduced-lock path acquires only ShareUpdateExclusiveLock for the
 * validation scan, then briefly takes AccessExclusiveLock to flip the
 * catalog flag.  This is safe for ordinary heap tables that are not
 * partitioned and have no inheritance children (we keep the simpler
 * path for those cases).
 */
extern bool OnlineDDLCanUseFastSetNotNull(Relation rel);

/*
 * OnlineDDLIsBinaryCoercibleType
 *		Returns true when ALTER COLUMN TYPE can skip the table rewrite.
 *
 * For binary-compatible type changes (e.g., int -> bigint where the
 * storage format does not actually change, or varchar(100) -> varchar(200)),
 * only the catalog metadata needs updating.  This function checks that:
 *   - IsBinaryCoercible(oldtype, newtype) returns true
 *   - No USING clause was specified
 *   - The column has no dependent indexes, constraints, or views that
 *     would require a full rewrite
 */
extern bool OnlineDDLIsBinaryCoercibleType(Oid oldtype, Oid newtype);

#endif							/* TABLECMDS_ONLINE_H */
