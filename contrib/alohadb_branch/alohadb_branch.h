/*-------------------------------------------------------------------------
 *
 * alohadb_branch.h
 *	  Shared declarations for the alohadb_branch extension.
 *
 *	  Lightweight database branching from point-in-time for testing
 *	  migrations and experiments.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * contrib/alohadb_branch/alohadb_branch.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef ALOHADB_BRANCH_H
#define ALOHADB_BRANCH_H

#include "postgres.h"
#include "fmgr.h"

/*
 * Port allocation: branch ports start at (current_port + BRANCH_PORT_OFFSET)
 * and increment by 1 for each subsequent branch.
 */
#define BRANCH_PORT_OFFSET		100

/*
 * Maximum length for a branch name.
 */
#define BRANCH_NAME_MAXLEN		63

/*
 * Branch status values stored in the alohadb_branches table.
 */
#define BRANCH_STATUS_RUNNING	"running"
#define BRANCH_STATUS_STOPPED	"stopped"
#define BRANCH_STATUS_ERROR		"error"

/*
 * Subdirectory name under the PGDATA parent for branch data directories.
 */
#define BRANCH_SUBDIR			"branches"

/*
 * Result columns for alohadb_create_branch().
 */
#define CREATE_BRANCH_COLS		3		/* branch_name, port, data_dir */

/*
 * Result columns for alohadb_list_branches().
 */
#define LIST_BRANCHES_COLS		6		/* name, lsn, port, data_dir, status,
										 * created_at */

/*
 * Internal helper prototypes.
 */
extern char *branch_get_parent_dir(void);
extern char *branch_get_data_dir(const char *branch_name);
extern int	branch_find_next_port(void);
extern bool branch_name_is_valid(const char *name);

#endif							/* ALOHADB_BRANCH_H */
