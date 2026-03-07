/*-------------------------------------------------------------------------
 *
 * ivfflatvalidate.c
 *	  Opclass validation for the IVFFlat index access method.
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * IDENTIFICATION
 *	  src/backend/access/ivfflat/ivfflatvalidate.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amvalidate.h"
#include "access/htup_details.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/syscache.h"

#include "ivfflat.h"

/*
 * ivfflatvalidate - validate an IVFFlat opclass.
 *
 * Checks that the opclass has the expected operators and support functions.
 */
bool
ivfflatvalidate(Oid opclassoid)
{
	bool		result = true;
	HeapTuple	classtup;
	Form_pg_opclass classform;
	Oid			opfamilyoid;
	char	   *opfamilyname;
	CatCList   *proclist,
			   *oprlist;
	int			i;

	/* Fetch opclass info */
	classtup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassoid));
	if (!HeapTupleIsValid(classtup))
		elog(ERROR, "cache lookup failed for operator class %u", opclassoid);
	classform = (Form_pg_opclass) GETSTRUCT(classtup);

	opfamilyoid = classform->opcfamily;

	/* Fetch opfamily name for error messages */
	opfamilyname = get_opfamily_name(opfamilyoid, false);

	/* Check support functions */
	proclist = SearchSysCacheList1(AMPROCNUM,
								  ObjectIdGetDatum(opfamilyoid));

	for (i = 0; i < proclist->n_members; i++)
	{
		HeapTuple	proctup = &proclist->members[i]->tuple;
		Form_pg_amproc procform = (Form_pg_amproc) GETSTRUCT(proctup);

		/* We expect support function 1 (distance function) */
		if (procform->amprocnum != VECTOR_DISTANCE_PROC)
		{
			ereport(INFO,
					(errmsg("ivfflat opfamily %s contains unexpected support function %d for type %s",
							opfamilyname,
							procform->amprocnum,
							format_type_be(procform->amproclefttype))));
			result = false;
		}
	}

	ReleaseSysCacheList(proclist);

	/* Check operators */
	oprlist = SearchSysCacheList1(AMOPSTRATEGY,
								 ObjectIdGetDatum(opfamilyoid));

	for (i = 0; i < oprlist->n_members; i++)
	{
		HeapTuple	oprtup = &oprlist->members[i]->tuple;
		Form_pg_amop oprform = (Form_pg_amop) GETSTRUCT(oprtup);

		/* Validate strategy number */
		if (oprform->amopstrategy < 1 ||
			oprform->amopstrategy > IVFFLAT_NUM_STRATEGIES)
		{
			ereport(INFO,
					(errmsg("ivfflat opfamily %s contains unexpected strategy %d for type %s",
							opfamilyname,
							oprform->amopstrategy,
							format_type_be(oprform->amoplefttype))));
			result = false;
		}

		/* IVFFlat operators must be ORDER BY operators */
		if (oprform->amoppurpose != AMOP_ORDER)
		{
			ereport(INFO,
					(errmsg("ivfflat opfamily %s contains non-ordering operator for type %s",
							opfamilyname,
							format_type_be(oprform->amoplefttype))));
			result = false;
		}
	}

	ReleaseSysCacheList(oprlist);

	ReleaseSysCache(classtup);

	return result;
}
