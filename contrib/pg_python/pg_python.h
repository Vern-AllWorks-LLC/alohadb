#ifndef ALOHADB_PYTHON_H
#define ALOHADB_PYTHON_H

#include "postgres.h"
#include "fmgr.h"

/* GUC variable: path to the Python executable (metadata only; execution uses plpython3u) */
extern char *pg_python_executable;

#endif							/* ALOHADB_PYTHON_H */
