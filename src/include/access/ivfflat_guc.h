/*-------------------------------------------------------------------------
 *
 * ivfflat_guc.h
 *	  GUC variables and parallel-build entry point of the IVFFlat index
 *	  access method (ported from pgvector; see src/backend/access/ivfflat).
 *
 * Copyright (c) 2025-2026, AlohaDB Project
 *
 * src/include/access/ivfflat_guc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef IVFFLAT_GUC_H
#define IVFFLAT_GUC_H

#include "storage/dsm.h"
#include "storage/shm_toc.h"

typedef enum IvfflatIterativeScanMode
{
	IVFFLAT_ITERATIVE_SCAN_OFF,
	IVFFLAT_ITERATIVE_SCAN_RELAXED
}			IvfflatIterativeScanMode;

/* GUCs (defined in guc_tables.c) */
extern PGDLLIMPORT int ivfflat_probes;
extern PGDLLIMPORT int ivfflat_iterative_scan;
extern PGDLLIMPORT int ivfflat_max_probes;

/* Parallel index build worker (registered in parallel.c) */
extern void IvfflatParallelBuildMain(dsm_segment *seg, shm_toc *toc);

#endif							/* IVFFLAT_GUC_H */
