/*-------------------------------------------------------------------------
 *
 * ivfflatoptions.c
 *	  Reloption handling for the IVFFlat index access method.
 *
 * Supports the following reloption:
 *
 *   lists (int, default 100, range 1-10000)
 *     Number of centroid lists (Voronoi cells) to partition the data into.
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * IDENTIFICATION
 *	  src/backend/access/ivfflat/ivfflatoptions.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/reloptions.h"

#include "ivfflat.h"

/* Reloption kind for IVFFlat -- allocated dynamically at startup */
static relopt_kind ivfflat_relopt_kind = RELOPT_KIND_LOCAL;

/*
 * Ensure the IVFFlat reloption kind and "lists" option are registered.
 *
 * This is called lazily from ivfflatoptions() on first use.
 */
static void
ivfflat_init_reloptions(void)
{
	if (ivfflat_relopt_kind != RELOPT_KIND_LOCAL)
		return;					/* already initialized */

	ivfflat_relopt_kind = add_reloption_kind();

	add_int_reloption(ivfflat_relopt_kind, "lists",
					  "Number of centroid lists for IVFFlat index",
					  IVFFLAT_DEFAULT_LISTS,
					  IVFFLAT_MIN_LISTS,
					  IVFFLAT_MAX_LISTS,
					  AccessExclusiveLock);
}

/*
 * ivfflatoptions - parse reloptions for a CREATE INDEX ... WITH (...) clause.
 */
bytea *
ivfflatoptions(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"lists", RELOPT_TYPE_INT, offsetof(IvfflatOptions, lists)},
	};

	/* Ensure our reloption kind and option are registered */
	ivfflat_init_reloptions();

	return (bytea *) build_reloptions(reloptions, validate,
									  ivfflat_relopt_kind,
									  sizeof(IvfflatOptions),
									  tab, lengthof(tab));
}
