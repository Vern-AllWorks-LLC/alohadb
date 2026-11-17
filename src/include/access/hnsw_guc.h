/*-------------------------------------------------------------------------
 *
 * hnsw_guc.h
 *	  GUC variables and parallel-build entry point of the HNSW index
 *	  access method (ported from pgvector; see src/backend/access/hnsw).
 *
 * Copyright (c) 2025-2026, AlohaDB Project
 *
 * src/include/access/hnsw_guc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HNSW_GUC_H
#define HNSW_GUC_H

#include "storage/dsm.h"
#include "storage/shm_toc.h"

typedef enum HnswIterativeScanMode
{
	HNSW_ITERATIVE_SCAN_OFF,
	HNSW_ITERATIVE_SCAN_RELAXED,
	HNSW_ITERATIVE_SCAN_STRICT
}			HnswIterativeScanMode;

/* GUCs (defined in guc_tables.c) */
extern PGDLLIMPORT int hnsw_ef_search;
extern PGDLLIMPORT int hnsw_iterative_scan;
extern PGDLLIMPORT int hnsw_max_scan_tuples;
extern PGDLLIMPORT double hnsw_scan_mem_multiplier;

/* Parallel index build worker (registered in parallel.c) */
extern void HnswParallelBuildMain(dsm_segment *seg, shm_toc *toc);

#endif							/* HNSW_GUC_H */
