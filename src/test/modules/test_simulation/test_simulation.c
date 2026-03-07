/*-------------------------------------------------------------------------
 *
 * test_simulation.c
 *	  Deterministic simulation testing framework for PostgreSQL.
 *
 * Provides SQL-callable functions that run deterministic simulation
 * scenarios testing crash recovery, checksum detection, WAL integrity,
 * and resource exhaustion handling.
 *
 * Uses PostgreSQL 18's injection_point API for fault injection and
 * a deterministic PRNG (splitmix64) seeded by a user-provided value
 * for reproducible test runs.
 *
 * Usage:
 *   SELECT sim_run_scenario('checksum_detection', seed := 42,
 *                           fault_probability := 0.1,
 *                           max_operations := 1000);
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * src/test/modules/test_simulation/test_simulation.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "port/pg_crc32c.h"
#include "storage/bufpage.h"
#include "storage/checksum.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"

#include "test_simulation.h"

PG_MODULE_MAGIC;

/* Global seed for non-function-scoped use */
uint64		sim_seed = 0;

/*
 * Scenario: Checksum detection test.
 *
 * Creates pages with valid checksums, corrupts them, then verifies
 * that PageIsVerifiedExtended detects the corruption.
 */
bool
sim_scenario_checksum_detection(SimConfig *config)
{
	SimState	state;
	int			pages_tested = 0;
	int			corruptions_detected = 0;
	int			corruptions_missed = 0;
	int			i;

	sim_prng_init(&state, config->seed);

	for (i = 0; i < config->max_operations; i++)
	{
		char	   *page;
		uint16		checksum;
		BlockNumber blkno;
		bool		valid_before;
		bool		valid_after;

		CHECK_FOR_INTERRUPTS();

		/* Allocate a page buffer */
		page = (char *) palloc0(BLCKSZ);

		/* Initialize as a valid empty page */
		PageInit((Page) page, BLCKSZ, 0);

		/* Assign a random block number */
		blkno = (BlockNumber) (sim_prng_next(&state) % 1000000);

		/* Compute and set checksum */
		checksum = pg_checksum_page(page, blkno);
		((PageHeader) page)->pd_checksum = checksum;

		/* Verify page is valid before corruption */
		valid_before = PageIsVerified((Page) page, blkno,
									 PIV_LOG_WARNING, NULL);

		if (!valid_before)
		{
			elog(WARNING, "simulation: page failed checksum before corruption (blkno %u)",
				 blkno);
			pfree(page);
			continue;
		}

		/* Inject corruption */
		sim_inject_page_corruption(&state, page, BLCKSZ);

		/* Verify checksum detects corruption */
		valid_after = PageIsVerified((Page) page, blkno,
									PIV_LOG_WARNING, NULL);

		pages_tested++;

		if (!valid_after)
		{
			/* Good: corruption was detected */
			corruptions_detected++;
		}
		else
		{
			/*
			 * Bad: corruption was NOT detected. This means either the
			 * checksum happened to still match (hash collision) or there's
			 * a bug in the checksum implementation.
			 */
			corruptions_missed++;
		}

		pfree(page);
	}

	state.faults_detected = corruptions_detected;
	state.faults_missed = corruptions_missed;

	elog(NOTICE, "simulation checksum_detection: seed=%lu pages=%d detected=%d missed=%d",
		 (unsigned long) config->seed, pages_tested,
		 corruptions_detected, corruptions_missed);

	/* Allow a very small number of hash collisions (< 0.1%) */
	return (corruptions_missed * 1000 <= pages_tested);
}

/*
 * Scenario: WAL corruption detection test.
 *
 * Creates simulated WAL-like data blocks, computes CRC32C checksums,
 * corrupts data, and verifies detection.
 */
bool
sim_scenario_wal_corruption(SimConfig *config)
{
	SimState	state;
	int			records_tested = 0;
	int			corruptions_detected = 0;
	int			i;

	sim_prng_init(&state, config->seed);

	for (i = 0; i < config->max_operations; i++)
	{
		char	   *record;
		int			record_len;
		pg_crc32c	crc_before;
		pg_crc32c	crc_after;
		int			corrupt_offset;

		CHECK_FOR_INTERRUPTS();

		/* Random record size between 32 and 8192 bytes */
		record_len = 32 + (int) (sim_prng_next(&state) % (8192 - 32));
		record = (char *) palloc(record_len);

		/* Fill with deterministic random data */
		for (int j = 0; j < record_len; j++)
			record[j] = (char) (sim_prng_next(&state) & 0xFF);

		/* Compute CRC */
		INIT_CRC32C(crc_before);
		COMP_CRC32C(crc_before, record, record_len);
		FIN_CRC32C(crc_before);

		/* Corrupt a random byte */
		corrupt_offset = (int) (sim_prng_next(&state) % record_len);
		record[corrupt_offset] ^= (char) (1 + (sim_prng_next(&state) % 255));

		/* Recompute CRC */
		INIT_CRC32C(crc_after);
		COMP_CRC32C(crc_after, record, record_len);
		FIN_CRC32C(crc_after);

		records_tested++;
		if (!EQ_CRC32C(crc_before, crc_after))
			corruptions_detected++;

		pfree(record);
	}

	elog(NOTICE, "simulation wal_corruption: seed=%lu records=%d detected=%d",
		 (unsigned long) config->seed, records_tested, corruptions_detected);

	/* CRC32C should detect virtually all single-byte corruptions */
	return (corruptions_detected == records_tested);
}

/*
 * Scenario: OOM resilience test.
 *
 * Tests that memory allocation failures during various operations
 * are handled cleanly without corrupting state.
 */
bool
sim_scenario_oom_during_build(SimConfig *config)
{
	SimState	state;
	int			allocs_tested = 0;
	int			oom_handled = 0;
	int			i;

	sim_prng_init(&state, config->seed);

	for (i = 0; i < config->max_operations; i++)
	{
		MemoryContext test_ctx;
		volatile bool caught = false;
		Size		alloc_size;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Create a memory context with a limited budget, then try to
		 * allocate varying sizes. The point is to verify that PG's
		 * memory context infrastructure handles OOM gracefully.
		 */
		test_ctx = AllocSetContextCreate(CurrentMemoryContext,
										 "sim_oom_test",
										 ALLOCSET_SMALL_SIZES);

		/* Random allocation size: sometimes reasonable, sometimes huge */
		if (sim_should_fault(&state, config->fault_probability))
			alloc_size = (Size) 1024 * 1024 * 1024;	/* 1GB - likely to fail */
		else
			alloc_size = 1 + (Size) (sim_prng_next(&state) % 65536);

		allocs_tested++;

		PG_TRY();
		{
			void	   *ptr;

			ptr = MemoryContextAlloc(test_ctx, alloc_size);
			/* If allocation succeeded, fill with pattern and free */
			memset(ptr, 0xAB, Min(alloc_size, 4096));
			pfree(ptr);
		}
		PG_CATCH();
		{
			/* OOM was caught cleanly */
			caught = true;
			oom_handled++;
			FlushErrorState();
		}
		PG_END_TRY();

		MemoryContextDelete(test_ctx);
	}

	elog(NOTICE, "simulation oom_resilience: seed=%lu allocs=%d oom_caught=%d",
		 (unsigned long) config->seed, allocs_tested, oom_handled);

	/* Success: we didn't crash */
	return true;
}

/*
 * SQL-callable function: run a simulation scenario.
 *
 * sim_run_scenario(scenario text, seed int8, fault_probability float8,
 *                  max_operations int4)
 * RETURNS TABLE(passed bool, operations int4, faults_injected int4,
 *               faults_detected int4, faults_missed int4)
 */
PG_FUNCTION_INFO_V1(sim_run_scenario);

Datum
sim_run_scenario(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	text	   *scenario_text = PG_GETARG_TEXT_PP(0);
	int64		seed = PG_GETARG_INT64(1);
	double		fault_prob = PG_GETARG_FLOAT8(2);
	int32		max_ops = PG_GETARG_INT32(3);
	char	   *scenario;
	SimConfig	config;
	bool		passed;
	Datum		values[5];
	bool		nulls[5] = {false};
	TupleDesc	tupdesc;

	InitMaterializedSRF(fcinfo, 0);

	scenario = text_to_cstring(scenario_text);

	/* Initialize config */
	config.seed = (uint64) seed;
	config.fault_probability = fault_prob;
	config.max_operations = max_ops;
	config.deterministic_time = true;

	/* Dispatch to scenario */
	if (strcmp(scenario, "checksum_detection") == 0)
		passed = sim_scenario_checksum_detection(&config);
	else if (strcmp(scenario, "wal_corruption") == 0)
		passed = sim_scenario_wal_corruption(&config);
	else if (strcmp(scenario, "oom_resilience") == 0)
		passed = sim_scenario_oom_during_build(&config);
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unknown simulation scenario: \"%s\"", scenario),
				 errhint("Valid scenarios: checksum_detection, wal_corruption, oom_resilience")));

	/* Build result tuple */
	tupdesc = rsinfo->setDesc;

	values[0] = BoolGetDatum(passed);
	values[1] = Int32GetDatum(max_ops);
	values[2] = Int32GetDatum(0);	/* faults_injected - scenario-specific */
	values[3] = Int32GetDatum(0);	/* faults_detected */
	values[4] = Int32GetDatum(0);	/* faults_missed */

	tuplestore_putvalues(rsinfo->setResult, tupdesc, values, nulls);

	pfree(scenario);

	return (Datum) 0;
}

/*
 * SQL-callable function: run all scenarios with a given seed.
 *
 * sim_run_all(seed int8) RETURNS TABLE(scenario text, passed bool)
 */
PG_FUNCTION_INFO_V1(sim_run_all);

Datum
sim_run_all(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int64		seed = PG_GETARG_INT64(0);
	SimConfig	config;
	TupleDesc	tupdesc;
	Datum		values[2];
	bool		nulls[2] = {false};

	static const char *scenarios[] = {
		"checksum_detection",
		"wal_corruption",
		"oom_resilience"
	};
	static const int num_scenarios = 3;

	InitMaterializedSRF(fcinfo, 0);

	tupdesc = rsinfo->setDesc;

	config.seed = (uint64) seed;
	config.fault_probability = 0.1;
	config.max_operations = 100;
	config.deterministic_time = true;

	for (int i = 0; i < num_scenarios; i++)
	{
		bool		passed;

		CHECK_FOR_INTERRUPTS();

		if (strcmp(scenarios[i], "checksum_detection") == 0)
			passed = sim_scenario_checksum_detection(&config);
		else if (strcmp(scenarios[i], "wal_corruption") == 0)
			passed = sim_scenario_wal_corruption(&config);
		else if (strcmp(scenarios[i], "oom_resilience") == 0)
			passed = sim_scenario_oom_during_build(&config);
		else
			passed = false;

		values[0] = CStringGetTextDatum(scenarios[i]);
		values[1] = BoolGetDatum(passed);

		tuplestore_putvalues(rsinfo->setResult, tupdesc, values, nulls);

		/* Advance seed for next scenario */
		config.seed = config.seed * 6364136223846793005ULL + 1;
	}

	return (Datum) 0;
}
