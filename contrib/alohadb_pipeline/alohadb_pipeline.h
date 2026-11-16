#ifndef ALOHADB_PIPELINE_H
#define ALOHADB_PIPELINE_H

#include "postgres.h"
#include "fmgr.h"

extern int	pipeline_check_interval;
extern int	pipeline_max_parallel;
extern char *pipeline_database;

pg_noreturn void		alohadb_pipeline_worker_main(Datum main_arg);

#endif							/* ALOHADB_PIPELINE_H */
