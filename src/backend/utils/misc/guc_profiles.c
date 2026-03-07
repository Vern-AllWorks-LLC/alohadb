/*-------------------------------------------------------------------------
 *
 * guc_profiles.c
 *		Auto-tuning configuration profiles for PostgreSQL.
 *
 * This module implements profile-based configuration that auto-tunes
 * key GUC variables based on detected hardware characteristics and
 * the selected workload type.  Supported profiles:
 *
 *   oltp      - high concurrency, moderate memory, aggressive checkpoints
 *   analytics - low concurrency, large memory, parallel query maximized
 *   ai_ml     - large memory, GPU-aware, HNSW index defaults tuned
 *   hybrid    - balanced settings for mixed workloads
 *
 * Hardware detection reads /proc/meminfo for RAM, sysconf() for CPU
 * cores, /dev/nvidia for GPU presence, and sysfs block device attributes
 * to classify storage as NVMe, SSD, or HDD.
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * IDENTIFICATION
 *	  src/backend/utils/misc/guc_profiles.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/guc_profiles.h"

/* GUC variable — the currently active profile name (empty = none) */
char	   *guc_profile = NULL;

/*
 * Static table of profile tuning factors.
 *
 * shared_buffers_ratio: fraction of total RAM to allocate
 * max_connections: target value (may be bounded by PGC_POSTMASTER)
 * work_mem_divisor: used as (remaining_ram / (max_connections * divisor))
 * maintenance_work_mem_mb: fixed value in MB
 * effective_cache_size_ratio: fraction of total RAM
 * checkpoint_completion_target_pct: percentage (0-100)
 * wal_buffers_mb: WAL buffer size in MB
 * enable_parallel: whether to maximise parallel workers
 */
static const ProfileFactors profile_factors[] =
{
	{
		"oltp",
		PROFILE_OLTP,
		0.25,					/* shared_buffers = 25% RAM */
		500,					/* max_connections */
		2.0,					/* work_mem divisor */
		512,					/* maintenance_work_mem 512 MB */
		0.75,					/* effective_cache_size */
		90,						/* checkpoint_completion_target */
		64,						/* wal_buffers 64 MB */
		false					/* parallel not maximized */
	},
	{
		"analytics",
		PROFILE_ANALYTICS,
		0.40,					/* shared_buffers = 40% RAM */
		50,						/* max_connections */
		2.0,					/* work_mem divisor */
		2048,					/* maintenance_work_mem 2 GB */
		0.75,					/* effective_cache_size */
		90,						/* checkpoint_completion_target */
		128,					/* wal_buffers 128 MB */
		true					/* maximize parallel workers */
	},
	{
		"ai_ml",
		PROFILE_AI_ML,
		0.35,					/* shared_buffers = 35% RAM */
		100,					/* max_connections */
		2.0,					/* work_mem divisor */
		2048,					/* maintenance_work_mem 2 GB */
		0.75,					/* effective_cache_size */
		90,						/* checkpoint_completion_target */
		128,					/* wal_buffers 128 MB */
		true					/* maximize parallel workers */
	},
	{
		"hybrid",
		PROFILE_HYBRID,
		0.30,					/* shared_buffers = 30% RAM */
		200,					/* max_connections */
		2.0,					/* work_mem divisor */
		1024,					/* maintenance_work_mem 1 GB */
		0.75,					/* effective_cache_size */
		90,						/* checkpoint_completion_target */
		64,						/* wal_buffers 64 MB */
		true					/* moderate parallel workers */
	},
};

#define NUM_PROFILES	(sizeof(profile_factors) / sizeof(profile_factors[0]))


/* ----------------------------------------------------------------
 *					Hardware Detection
 * ----------------------------------------------------------------
 */

/*
 * DetectHardware
 *		Populate a HardwareInfo struct by probing the running system.
 *
 * On non-Linux systems or when /proc is unavailable, we fall back to
 * conservative defaults (4 GB RAM, 4 cores, no GPU, SSD assumed).
 */
void
DetectHardware(HardwareInfo *hw)
{
	memset(hw, 0, sizeof(HardwareInfo));

	/* --- RAM detection via /proc/meminfo --- */
	{
		FILE	   *fp;

		hw->total_ram_mb = 4096;	/* fallback: 4 GB */

		fp = fopen("/proc/meminfo", "r");
		if (fp != NULL)
		{
			char		line[256];

			while (fgets(line, sizeof(line), fp) != NULL)
			{
				long long	val;

				if (sscanf(line, "MemTotal: %lld kB", &val) == 1)
				{
					hw->total_ram_mb = (int64) (val / 1024);
					break;
				}
			}
			fclose(fp);
		}
	}

	/* --- CPU core count via sysconf --- */
	{
		long		ncpus;

		ncpus = sysconf(_SC_NPROCESSORS_ONLN);
		hw->cpu_cores = (ncpus > 0) ? (int) ncpus : 4;
	}

	/* --- GPU detection: check for /dev/nvidia* --- */
	{
		DIR		   *dir;
		struct dirent *entry;

		hw->has_gpu = false;
		dir = opendir("/dev");
		if (dir != NULL)
		{
			while ((entry = readdir(dir)) != NULL)
			{
				if (strncmp(entry->d_name, "nvidia", 6) == 0)
				{
					hw->has_gpu = true;
					break;
				}
			}
			closedir(dir);
		}
	}

	/* --- Storage type detection via sysfs block queue rotational --- */
	{
		DIR		   *dir;
		struct dirent *entry;

		hw->has_nvme = false;
		hw->has_ssd = false;
		hw->has_hdd = false;

		dir = opendir("/sys/block");
		if (dir != NULL)
		{
			while ((entry = readdir(dir)) != NULL)
			{
				char		path[MAXPGPATH];
				FILE	   *fp;
				int			rotational = -1;

				/* Skip . and .. and non-device entries */
				if (entry->d_name[0] == '.')
					continue;

				/* Skip loop, ram, and dm devices */
				if (strncmp(entry->d_name, "loop", 4) == 0 ||
					strncmp(entry->d_name, "ram", 3) == 0 ||
					strncmp(entry->d_name, "dm-", 3) == 0)
					continue;

				snprintf(path, sizeof(path),
						 "/sys/block/%s/queue/rotational",
						 entry->d_name);

				fp = fopen(path, "r");
				if (fp != NULL)
				{
					if (fscanf(fp, "%d", &rotational) == 1)
					{
						if (rotational == 1)
							hw->has_hdd = true;
						else
						{
							/* Non-rotational: distinguish NVMe from SATA SSD */
							if (strncmp(entry->d_name, "nvme", 4) == 0)
								hw->has_nvme = true;
							else
								hw->has_ssd = true;
						}
					}
					fclose(fp);
				}
			}
			closedir(dir);
		}

		/*
		 * If nothing was detected at all, assume SSD as a safe default.
		 */
		if (!hw->has_nvme && !hw->has_ssd && !hw->has_hdd)
			hw->has_ssd = true;
	}
}


/* ----------------------------------------------------------------
 *					Profile Lookup
 * ----------------------------------------------------------------
 */

/*
 * ParseProfileName
 *		Convert a profile name string to a ProfileType enum value.
 *		Returns PROFILE_NONE if the name is not recognized or is empty.
 */
ProfileType
ParseProfileName(const char *name)
{
	if (name == NULL || name[0] == '\0')
		return PROFILE_NONE;

	if (pg_strcasecmp(name, "oltp") == 0)
		return PROFILE_OLTP;
	if (pg_strcasecmp(name, "analytics") == 0)
		return PROFILE_ANALYTICS;
	if (pg_strcasecmp(name, "ai_ml") == 0)
		return PROFILE_AI_ML;
	if (pg_strcasecmp(name, "hybrid") == 0)
		return PROFILE_HYBRID;

	return PROFILE_NONE;
}

/*
 * LookupProfileFactors
 *		Return the ProfileFactors entry for a given ProfileType.
 *		Returns NULL if the type is PROFILE_NONE or unrecognized.
 */
static const ProfileFactors *
LookupProfileFactors(ProfileType ptype)
{
	for (int i = 0; i < (int) NUM_PROFILES; i++)
	{
		if (profile_factors[i].type == ptype)
			return &profile_factors[i];
	}
	return NULL;
}


/* ----------------------------------------------------------------
 *					GUC Application Helpers
 * ----------------------------------------------------------------
 */

/*
 * SetGUCFromProfile
 *		Helper to set a single GUC parameter to a computed string value.
 *		Uses PGC_S_SESSION source so that profile-set values can be
 *		overridden by explicit user SET commands.
 */
static void
SetGUCFromProfile(const char *name, const char *value)
{
	(void) set_config_option(name, value,
							 PGC_SUSET, PGC_S_SESSION,
							 GUC_ACTION_SET, true, LOG, false);
}


/* ----------------------------------------------------------------
 *					Main Profile Application
 * ----------------------------------------------------------------
 */

/*
 * ApplyProfile
 *		Apply the named workload profile by computing tuned GUC values
 *		from detected hardware and the profile's tuning factors.
 *
 *		This function can be called at postmaster startup (from
 *		PostmasterMain) or at runtime via the SQL function
 *		alohadb_apply_profile().
 */
void
ApplyProfile(const char *profile_name)
{
	HardwareInfo hw;
	ProfileType ptype;
	const ProfileFactors *pf;
	char		buf[64];
	int64		shared_buffers_mb;
	int64		work_mem_kb;
	int64		effective_cache_size_mb;
	int64		maintenance_work_mem_mb;
	int			max_parallel_workers;
	int			max_parallel_workers_per_gather;

	/* Parse the profile name */
	ptype = ParseProfileName(profile_name);
	if (ptype == PROFILE_NONE)
	{
		ereport(LOG,
				(errmsg("profile \"%s\" is not recognized, skipping auto-tuning",
						profile_name ? profile_name : "(null)")));
		return;
	}

	pf = LookupProfileFactors(ptype);
	if (pf == NULL)
		return;					/* should not happen */

	/* Detect hardware */
	DetectHardware(&hw);

	ereport(LOG,
			(errmsg("applying profile \"%s\": RAM=%lld MB, CPUs=%d, GPU=%s, "
					"NVMe=%s, SSD=%s, HDD=%s",
					pf->name,
					(long long) hw.total_ram_mb,
					hw.cpu_cores,
					hw.has_gpu ? "yes" : "no",
					hw.has_nvme ? "yes" : "no",
					hw.has_ssd ? "yes" : "no",
					hw.has_hdd ? "yes" : "no")));

	/* --- shared_buffers = RAM * profile_factor (in MB, clamped) --- */
	shared_buffers_mb = (int64) (hw.total_ram_mb * pf->shared_buffers_ratio);
	if (shared_buffers_mb < 128)
		shared_buffers_mb = 128;

	snprintf(buf, sizeof(buf), "%lldMB", (long long) shared_buffers_mb);
	SetGUCFromProfile("shared_buffers", buf);

	/* --- max_connections --- */
	snprintf(buf, sizeof(buf), "%d", pf->max_connections);
	SetGUCFromProfile("max_connections", buf);

	/* --- work_mem = (RAM - shared_buffers) / (max_connections * divisor) --- */
	work_mem_kb = ((hw.total_ram_mb - shared_buffers_mb) * 1024) /
		(int64) (pf->max_connections * pf->work_mem_divisor);
	if (work_mem_kb < 4096)		/* floor at 4 MB */
		work_mem_kb = 4096;

	snprintf(buf, sizeof(buf), "%lldkB", (long long) work_mem_kb);
	SetGUCFromProfile("work_mem", buf);

	/* --- effective_cache_size = RAM * 0.75 --- */
	effective_cache_size_mb = (int64) (hw.total_ram_mb * pf->effective_cache_size_ratio);
	if (effective_cache_size_mb < 512)
		effective_cache_size_mb = 512;

	snprintf(buf, sizeof(buf), "%lldMB", (long long) effective_cache_size_mb);
	SetGUCFromProfile("effective_cache_size", buf);

	/* --- maintenance_work_mem --- */
	maintenance_work_mem_mb = pf->maintenance_work_mem_mb;
	/* Cap at 25% of RAM */
	if (maintenance_work_mem_mb > hw.total_ram_mb / 4)
		maintenance_work_mem_mb = hw.total_ram_mb / 4;
	if (maintenance_work_mem_mb < 64)
		maintenance_work_mem_mb = 64;

	snprintf(buf, sizeof(buf), "%lldMB", (long long) maintenance_work_mem_mb);
	SetGUCFromProfile("maintenance_work_mem", buf);

	/* --- checkpoint_completion_target --- */
	snprintf(buf, sizeof(buf), "0.%d", pf->checkpoint_completion_target_pct);
	SetGUCFromProfile("checkpoint_completion_target", buf);

	/* --- wal_buffers --- */
	snprintf(buf, sizeof(buf), "%dMB", pf->wal_buffers_mb);
	SetGUCFromProfile("wal_buffers", buf);

	/* --- max_parallel_workers = CPU cores - 2 (minimum 2) --- */
	max_parallel_workers = hw.cpu_cores - 2;
	if (max_parallel_workers < 2)
		max_parallel_workers = 2;

	if (pf->enable_parallel)
	{
		snprintf(buf, sizeof(buf), "%d", max_parallel_workers);
		SetGUCFromProfile("max_parallel_workers", buf);

		max_parallel_workers_per_gather = max_parallel_workers / 2;
		if (max_parallel_workers_per_gather < 2)
			max_parallel_workers_per_gather = 2;

		snprintf(buf, sizeof(buf), "%d", max_parallel_workers_per_gather);
		SetGUCFromProfile("max_parallel_workers_per_gather", buf);

		/* max_parallel_maintenance_workers */
		snprintf(buf, sizeof(buf), "%d",
				 max_parallel_workers_per_gather > 4 ? 4 : max_parallel_workers_per_gather);
		SetGUCFromProfile("max_parallel_maintenance_workers", buf);
	}
	else
	{
		/* OLTP: keep default parallel settings but ensure reasonable value */
		snprintf(buf, sizeof(buf), "%d",
				 max_parallel_workers > 8 ? 8 : max_parallel_workers);
		SetGUCFromProfile("max_parallel_workers", buf);
	}

	/* --- max_worker_processes: parallel workers + background workers + headroom --- */
	{
		int			max_worker_processes = max_parallel_workers + 8;

		if (max_worker_processes < 16)
			max_worker_processes = 16;

		snprintf(buf, sizeof(buf), "%d", max_worker_processes);
		SetGUCFromProfile("max_worker_processes", buf);
	}

	/* --- random_page_cost: lower for SSD/NVMe --- */
	if (hw.has_nvme)
		SetGUCFromProfile("random_page_cost", "1.1");
	else if (hw.has_ssd)
		SetGUCFromProfile("random_page_cost", "1.5");
	else
		SetGUCFromProfile("random_page_cost", "4.0");

	/* --- effective_io_concurrency: higher for modern storage --- */
	if (hw.has_nvme)
		SetGUCFromProfile("effective_io_concurrency", "200");
	else if (hw.has_ssd)
		SetGUCFromProfile("effective_io_concurrency", "100");
	else
		SetGUCFromProfile("effective_io_concurrency", "2");

	/* --- AI/ML specific tuning --- */
	if (ptype == PROFILE_AI_ML)
	{
		/*
		 * For AI/ML workloads, increase work_mem further since these
		 * workloads typically have fewer concurrent queries but each
		 * requires much more memory for vector operations and HNSW
		 * index building.
		 */
		work_mem_kb = ((hw.total_ram_mb - shared_buffers_mb) * 1024) /
			(int64) (pf->max_connections * 1.0);
		if (work_mem_kb < 65536)	/* floor at 64 MB for AI/ML */
			work_mem_kb = 65536;

		snprintf(buf, sizeof(buf), "%lldkB", (long long) work_mem_kb);
		SetGUCFromProfile("work_mem", buf);

		/*
		 * Set HNSW-specific defaults if available.  These are custom GUCs
		 * that may not exist if the HNSW extension is not loaded, so we
		 * set them with LOG level to avoid errors.
		 */
		SetGUCFromProfile("hnsw.ef_construction", "128");
		SetGUCFromProfile("hnsw.m", "24");

		/* If GPU is present, log awareness */
		if (hw.has_gpu)
			ereport(LOG,
					(errmsg("GPU detected: ai_ml profile applied with GPU-awareness")));
	}

	ereport(LOG,
			(errmsg("profile \"%s\" applied successfully", pf->name)));
}


/* ----------------------------------------------------------------
 *					GUC Hooks for 'profile' Setting
 * ----------------------------------------------------------------
 */

/*
 * check_profile
 *		GUC check hook for the 'profile' string variable.
 *		Validates that the value is a recognized profile name or empty.
 */
bool
check_profile(char **newval, void **extra, GucSource source)
{
	const char *val = *newval;

	/* Empty string is allowed — it means "no profile" */
	if (val == NULL || val[0] == '\0')
		return true;

	/* Must be a known profile name */
	if (ParseProfileName(val) == PROFILE_NONE)
	{
		GUC_check_errdetail("Valid profiles are: oltp, analytics, ai_ml, hybrid.");
		return false;
	}

	return true;
}

/*
 * assign_profile
 *		GUC assign hook for the 'profile' string variable.
 *		When the profile GUC is changed via SIGHUP reload, this
 *		triggers re-application of the profile.
 */
void
assign_profile(const char *newval, void *extra)
{
	/*
	 * Only apply if we have a non-empty value and we are past the initial
	 * boot phase.  During bootstrap/initialization, ApplyProfile is called
	 * explicitly from PostmasterMain.
	 */
	if (newval != NULL && newval[0] != '\0' && IsUnderPostmaster)
		ApplyProfile(newval);
}


/* ----------------------------------------------------------------
 *					SQL-Callable Functions
 * ----------------------------------------------------------------
 */

/*
 * alohadb_apply_profile(text) RETURNS void
 *
 * Manually apply a configuration profile.  Requires superuser.
 */
Datum
alohadb_apply_profile(PG_FUNCTION_ARGS)
{
	text	   *profile_text;
	char	   *profile_name;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to apply a configuration profile")));

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("profile name must not be NULL")));

	profile_text = PG_GETARG_TEXT_PP(0);
	profile_name = text_to_cstring(profile_text);

	if (ParseProfileName(profile_name) == PROFILE_NONE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unrecognized profile: \"%s\"", profile_name),
				 errhint("Valid profiles are: oltp, analytics, ai_ml, hybrid.")));

	ApplyProfile(profile_name);

	pfree(profile_name);
	PG_RETURN_VOID();
}

/*
 * alohadb_detect_hardware() RETURNS TABLE(component text, value text)
 *
 * Returns detected hardware information as a set of (component, value) rows.
 */
Datum
alohadb_detect_hardware(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	HardwareInfo   *hw;

	/* Number of hardware components we report */
#define NUM_HW_COMPONENTS	6

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Build tuple descriptor for (component text, value text) */
		tupdesc = CreateTemplateTupleDesc(2);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "component",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "value",
						   TEXTOID, -1, 0);

		funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->max_calls = NUM_HW_COMPONENTS;

		/* Detect hardware once and store in user_fctx */
		hw = (HardwareInfo *) palloc(sizeof(HardwareInfo));
		DetectHardware(hw);
		funcctx->user_fctx = hw;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	hw = (HardwareInfo *) funcctx->user_fctx;

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		char	   *values[2];
		HeapTuple	tuple;
		Datum		result;
		char		valbuf[128];

		switch (funcctx->call_cntr)
		{
			case 0:
				values[0] = "total_ram_mb";
				snprintf(valbuf, sizeof(valbuf), INT64_FORMAT,
						 hw->total_ram_mb);
				values[1] = valbuf;
				break;
			case 1:
				values[0] = "cpu_cores";
				snprintf(valbuf, sizeof(valbuf), "%d", hw->cpu_cores);
				values[1] = valbuf;
				break;
			case 2:
				values[0] = "gpu_detected";
				values[1] = hw->has_gpu ? "true" : "false";
				break;
			case 3:
				values[0] = "storage_nvme";
				values[1] = hw->has_nvme ? "true" : "false";
				break;
			case 4:
				values[0] = "storage_ssd";
				values[1] = hw->has_ssd ? "true" : "false";
				break;
			case 5:
				values[0] = "storage_hdd";
				values[1] = hw->has_hdd ? "true" : "false";
				break;
			default:
				/* Should not reach here */
				values[0] = "unknown";
				values[1] = "unknown";
				break;
		}

		tuple = BuildTupleFromCStrings(funcctx->attinmeta, values);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);
}
