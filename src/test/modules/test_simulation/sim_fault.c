/*-------------------------------------------------------------------------
 *
 * sim_fault.c
 *	  Fault injection for simulation testing.
 *
 * Provides functions to corrupt pages, simulate I/O errors, and inject
 * other faults in a deterministic manner controlled by the PRNG.
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * src/test/modules/test_simulation/sim_fault.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "test_simulation.h"
#include "storage/bufpage.h"

/*
 * Inject page corruption by flipping random bytes.
 *
 * Corrupts between 1 and 8 bytes at random positions in the page,
 * simulating storage media bit-rot or firmware bugs.
 */
void
sim_inject_page_corruption(SimState *state, char *page, int page_size)
{
	int			num_corruptions;
	int			i;

	num_corruptions = 1 + (int) (sim_prng_next(state) % 8);

	for (i = 0; i < num_corruptions; i++)
	{
		int			offset;
		uint8		xor_mask;

		/* Pick a random byte offset, avoiding the pd_checksum field */
		offset = (int) (sim_prng_next(state) % page_size);

		/* Skip the checksum field at offset 6-7 in PageHeaderData */
		if (offset == offsetof(PageHeaderData, pd_checksum) ||
			offset == offsetof(PageHeaderData, pd_checksum) + 1)
			offset = (offset + 2) % page_size;

		/* Flip random bits */
		xor_mask = (uint8) (sim_prng_next(state) & 0xFF);
		if (xor_mask == 0)
			xor_mask = 1;		/* ensure at least one bit flips */

		page[offset] ^= xor_mask;
	}

	state->faults_injected++;
}

/*
 * Decide whether to inject an I/O fault based on config.
 */
void
sim_inject_io_fault(SimState *state, SimConfig *config)
{
	if (!sim_should_fault(state, config->fault_probability))
		return;

	state->faults_injected++;

	/* The caller checks this and raises the appropriate error */
}
