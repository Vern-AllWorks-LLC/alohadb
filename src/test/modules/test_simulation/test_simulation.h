/*-------------------------------------------------------------------------
 *
 * test_simulation.h
 *	  Header for deterministic simulation testing framework.
 *
 * This module provides FoundationDB-style deterministic simulation for
 * testing crash recovery, page corruption detection, and I/O failures.
 * It uses PostgreSQL 18's injection points API for intercepting operations.
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * src/test/modules/test_simulation/test_simulation.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TEST_SIMULATION_H
#define TEST_SIMULATION_H

#include "postgres.h"
#include "fmgr.h"
#include "utils/timestamp.h"

/*
 * Simulation seed for deterministic replay.
 * All randomness in the simulation derives from this seed.
 */
extern uint64 sim_seed;

/*
 * Fault injection types.
 */
typedef enum SimFaultType
{
	SIM_FAULT_NONE = 0,
	SIM_FAULT_IO_READ_ERROR,		/* Read returns EIO */
	SIM_FAULT_IO_WRITE_ERROR,		/* Write returns EIO */
	SIM_FAULT_PAGE_CORRUPTION,		/* Corrupt page data after write */
	SIM_FAULT_OOM,					/* palloc returns NULL / ereport */
	SIM_FAULT_CRASH,				/* Simulate process crash */
	SIM_FAULT_SLOW_IO,				/* Inject latency into I/O */
	SIM_FAULT_WAL_WRITE_PARTIAL		/* Partial WAL write */
} SimFaultType;

/*
 * Simulation configuration for a test run.
 */
typedef struct SimConfig
{
	uint64		seed;				/* PRNG seed */
	double		fault_probability;	/* 0.0 to 1.0 */
	SimFaultType allowed_faults;	/* bitmask of enabled fault types */
	int			max_operations;		/* stop after N operations */
	bool		deterministic_time; /* use simulated clock */
} SimConfig;

/*
 * Simulation state - tracks what happened during the run.
 */
typedef struct SimState
{
	uint64		prng_state;			/* current PRNG state (splitmix64) */
	int			operations_count;	/* total operations performed */
	int			faults_injected;	/* total faults injected */
	int			faults_detected;	/* faults properly detected */
	int			faults_missed;		/* faults not detected (BUG) */
	TimestampTz sim_time;			/* simulated clock */
} SimState;

/* PRNG functions */
extern void		sim_prng_init(SimState *state, uint64 seed);
extern uint64	sim_prng_next(SimState *state);
extern double	sim_prng_double(SimState *state);
extern bool		sim_should_fault(SimState *state, double probability);

/* Fault injection */
extern void		sim_inject_io_fault(SimState *state, SimConfig *config);
extern void		sim_inject_page_corruption(SimState *state, char *page,
										   int page_size);

/* Simulation scenarios */
extern bool		sim_scenario_crash_recovery(SimConfig *config);
extern bool		sim_scenario_checksum_detection(SimConfig *config);
extern bool		sim_scenario_wal_corruption(SimConfig *config);
extern bool		sim_scenario_oom_during_build(SimConfig *config);

#endif							/* TEST_SIMULATION_H */
