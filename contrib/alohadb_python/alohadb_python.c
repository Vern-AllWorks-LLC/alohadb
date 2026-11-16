#include "postgres.h"
#include "fmgr.h"
#include "utils/guc.h"
#include "alohadb_python.h"

PG_MODULE_MAGIC_EXT(
	.name = "alohadb_python",
	.version = "1.0"
);

char	   *alohadb_python_executable = NULL;

void		_PG_init(void);

void
_PG_init(void)
{
	/*
	 * alohadb.python_executable is metadata only — it documents which Python
	 * binary corresponds to the plpython3u runtime. Actual script execution
	 * is delegated entirely to PostgreSQL's built-in plpython3u language
	 * handler (in-process, no subprocess fork).
	 */
	DefineCustomStringVariable(
		"alohadb.python_executable",
		"Python executable that matches the plpython3u runtime.",
		"Informational only. Execution uses the in-process plpython3u handler.",
		&alohadb_python_executable,
		"python3",
		PGC_SUSET,
		0,
		NULL, NULL, NULL
	);

	MarkGUCPrefixReserved("alohadb.python");
}
