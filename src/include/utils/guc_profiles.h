/*-------------------------------------------------------------------------
 *
 * guc_profiles.h
 *		Declarations for auto-tuning configuration profiles.
 *
 * This module provides profile-based configuration that auto-tunes
 * key GUC variables based on detected hardware and the selected
 * workload type (oltp, analytics, ai_ml, hybrid).
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 *	  src/include/utils/guc_profiles.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GUC_PROFILES_H
#define GUC_PROFILES_H

#include "fmgr.h"

/*
 * Supported workload profile types.
 */
typedef enum ProfileType
{
	PROFILE_NONE = 0,
	PROFILE_OLTP,
	PROFILE_ANALYTICS,
	PROFILE_AI_ML,
	PROFILE_HYBRID,
} ProfileType;

/*
 * Hardware detection results, collected once and reused.
 */
typedef struct HardwareInfo
{
	int64		total_ram_mb;		/* total system RAM in megabytes */
	int			cpu_cores;			/* number of online CPU cores */
	bool		has_gpu;			/* true if NVIDIA GPU detected */
	bool		has_nvme;			/* true if NVMe storage detected */
	bool		has_ssd;			/* true if SSD (non-rotational) detected */
	bool		has_hdd;			/* true if HDD (rotational) detected */
} HardwareInfo;

/*
 * Profile-specific tuning factors.
 */
typedef struct ProfileFactors
{
	const char *name;				/* profile name string */
	ProfileType type;				/* enum value */
	double		shared_buffers_ratio;	/* fraction of RAM for shared_buffers */
	int			max_connections;	/* target max_connections */
	double		work_mem_divisor;	/* divisor applied to remaining RAM */
	int			maintenance_work_mem_mb;	/* maintenance_work_mem in MB */
	double		effective_cache_size_ratio;	/* fraction of RAM */
	int			checkpoint_completion_target_pct;	/* 0-100 */
	int			wal_buffers_mb;		/* WAL buffers in MB */
	bool		enable_parallel;	/* maximize parallel query workers */
} ProfileFactors;

/* GUC variable for the profile setting */
extern PGDLLIMPORT char *guc_profile;

/* Core functions */
extern void DetectHardware(HardwareInfo *hw);
extern ProfileType ParseProfileName(const char *name);
extern void ApplyProfile(const char *profile_name);

/*
 * GUC hooks for the 'profile' variable are declared in guc_hooks.h,
 * following PostgreSQL convention.  They are implemented in guc_profiles.c.
 */

/* SQL-callable functions */
extern Datum alohadb_apply_profile(PG_FUNCTION_ARGS);
extern Datum alohadb_detect_hardware(PG_FUNCTION_ARGS);

#endif							/* GUC_PROFILES_H */
