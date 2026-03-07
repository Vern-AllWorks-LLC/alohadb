/*-------------------------------------------------------------------------
 *
 * ivfflat_guc.h
 *	  GUC variable declarations for the IVFFlat index access method.
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * src/include/access/ivfflat_guc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef IVFFLAT_GUC_H
#define IVFFLAT_GUC_H

/* GUC: ivfflat.probes - number of lists to probe during index scan */
extern PGDLLIMPORT int ivfflat_probes;

#endif							/* IVFFLAT_GUC_H */
