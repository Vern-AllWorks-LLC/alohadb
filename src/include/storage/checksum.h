/*-------------------------------------------------------------------------
 *
 * checksum.h
 *	  Checksum implementation for data pages.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/checksum.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CHECKSUM_H
#define CHECKSUM_H

#include "storage/block.h"

/*
 * Checksum algorithm version constants.
 *
 * DATA_CHECKSUM_VERSION_FNVA (1) is the original FNV-1a based algorithm
 * that has been used since PostgreSQL 9.3.
 *
 * DATA_CHECKSUM_VERSION_XXHASH3 (2) uses the xxHash3 64-bit algorithm,
 * which provides faster computation and better collision resistance.
 */
#define DATA_CHECKSUM_VERSION_FNVA		1
#define DATA_CHECKSUM_VERSION_XXHASH3	2

/*
 * Compute the checksum for a Postgres page.  The page must be aligned on a
 * 4-byte boundary.
 */
extern uint16 pg_checksum_page(char *page, BlockNumber blkno);

/*
 * Compute the checksum for a Postgres page using xxHash3 algorithm.
 * The page must be aligned on a 4-byte boundary.
 */
extern uint16 pg_checksum_page_xxhash3(char *page, BlockNumber blkno);

#endif							/* CHECKSUM_H */
