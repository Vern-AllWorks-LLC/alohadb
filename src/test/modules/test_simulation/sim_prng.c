/*-------------------------------------------------------------------------
 *
 * sim_prng.c
 *	  Deterministic PRNG for simulation testing.
 *
 * Uses splitmix64 for fast, deterministic pseudo-random number generation.
 * All simulation randomness derives from a single seed, enabling
 * deterministic replay of any failure scenario.
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * src/test/modules/test_simulation/sim_prng.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "test_simulation.h"

/*
 * Initialize PRNG with seed.
 */
void
sim_prng_init(SimState *state, uint64 seed)
{
	state->prng_state = seed;
	state->operations_count = 0;
	state->faults_injected = 0;
	state->faults_detected = 0;
	state->faults_missed = 0;
	state->sim_time = 0;
}

/*
 * Generate next pseudo-random uint64 using splitmix64.
 */
uint64
sim_prng_next(SimState *state)
{
	uint64		z;

	state->prng_state += 0x9e3779b97f4a7c15ULL;
	z = state->prng_state;
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
	z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
	return z ^ (z >> 31);
}

/*
 * Generate a random double in [0.0, 1.0).
 */
double
sim_prng_double(SimState *state)
{
	return (double) (sim_prng_next(state) >> 11) / (double) (1ULL << 53);
}

/*
 * Determine whether a fault should be injected based on probability.
 */
bool
sim_should_fault(SimState *state, double probability)
{
	return sim_prng_double(state) < probability;
}
