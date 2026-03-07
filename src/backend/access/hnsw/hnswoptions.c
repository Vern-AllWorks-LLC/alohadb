/*-------------------------------------------------------------------------
 *
 * hnswoptions.c
 *	  HNSW index reloptions parsing.
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * IDENTIFICATION
 *	  src/backend/access/hnsw/hnswoptions.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/reloptions.h"
#include "hnsw.h"

/* Module-level state for reloption registration */
static relopt_kind hnsw_relopt_kind = 0;
static bool hnsw_options_registered = false;

/*
 * hnsw_register_reloptions - register HNSW-specific reloptions.
 *
 * Called once to register the custom reloption kind and the individual options.
 */
static void
hnsw_register_reloptions(void)
{
	if (hnsw_options_registered)
		return;

	hnsw_relopt_kind = add_reloption_kind();

	add_int_reloption(hnsw_relopt_kind, "m",
					  "Max number of connections per layer",
					  HNSW_DEFAULT_M, 2, 100,
					  AccessExclusiveLock);
	add_int_reloption(hnsw_relopt_kind, "ef_construction",
					  "Size of the dynamic candidate list during index construction",
					  HNSW_DEFAULT_EF_CONSTRUCTION, 4, 1000,
					  AccessExclusiveLock);

	hnsw_options_registered = true;
}

/*
 * hnswoptions - parse and validate HNSW-specific reloptions.
 *
 * Valid options:
 *   m (int, default 16, range 2-100): max connections per layer
 *   ef_construction (int, default 64, range 4-1000): search width during build
 */
bytea *
hnswoptions(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"m", RELOPT_TYPE_INT, offsetof(HnswOptions, m)},
		{"ef_construction", RELOPT_TYPE_INT, offsetof(HnswOptions, efConstruction)},
	};

	/* Ensure our custom reloptions are registered */
	hnsw_register_reloptions();

	return (bytea *) build_reloptions(reloptions, validate,
									  hnsw_relopt_kind,
									  sizeof(HnswOptions),
									  tab, lengthof(tab));
}
