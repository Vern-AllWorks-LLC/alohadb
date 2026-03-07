/*-------------------------------------------------------------------------
 *
 * hnswvalidate.c
 *	  Opclass validator for HNSW index access method.
 *
 * Copyright (c) 2025, AlohaDB Project
 *
 * IDENTIFICATION
 *	  src/backend/access/hnsw/hnswvalidate.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amvalidate.h"
#include "access/htup_details.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_type.h"
#include "hnsw.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/syscache.h"

/*
 * hnswvalidate - validate an HNSW opclass.
 *
 * Verifies that the opclass has a distance function registered as support
 * procedure 1 (HNSW_DISTANCE_PROC).
 */
bool
hnswvalidate(Oid opclassoid)
{
	bool		result = true;
	HeapTuple	classtup;
	Form_pg_opclass classform;
	Oid			opfamilyoid;
	Oid			opcintype;
	char	   *opclassname;
	char	   *opfamilyname;
	CatCList   *proclist;
	CatCList   *oprlist;
	List	   *grouplist;
	bool		found_distance_proc = false;
	int			i;
	ListCell   *lc;

	/* Fetch opclass information */
	classtup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassoid));
	if (!HeapTupleIsValid(classtup))
		elog(ERROR, "cache lookup failed for operator class %u", opclassoid);
	classform = (Form_pg_opclass) GETSTRUCT(classtup);

	opfamilyoid = classform->opcfamily;
	opcintype = classform->opcintype;
	opclassname = NameStr(classform->opcname);

	/* Fetch opfamily information */
	opfamilyname = get_opfamily_name(opfamilyoid, false);

	/* Fetch all operators and support functions of the opfamily */
	oprlist = SearchSysCacheList1(AMOPSTRATEGY, ObjectIdGetDatum(opfamilyoid));
	proclist = SearchSysCacheList1(AMPROCNUM, ObjectIdGetDatum(opfamilyoid));

	/* Check support functions */
	for (i = 0; i < proclist->n_members; i++)
	{
		HeapTuple	proctup = &proclist->members[i]->tuple;
		Form_pg_amproc procform = (Form_pg_amproc) GETSTRUCT(proctup);

		/* Check procedure numbers */
		switch (procform->amprocnum)
		{
			case HNSW_DISTANCE_PROC:
				/* Distance function should return float8 */
				if (!check_amproc_signature(procform->amproc, FLOAT8OID, true,
											2, 2,
											procform->amproclefttype,
											procform->amprocrighttype))
				{
					ereport(INFO,
							(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
							 errmsg("operator family \"%s\" of access method %s contains function %s with wrong signature for support number %d",
									opfamilyname, "hnsw",
									format_procedure(procform->amproc),
									procform->amprocnum)));
					result = false;
				}
				found_distance_proc = true;
				break;

			default:
				ereport(INFO,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("operator family \"%s\" of access method %s contains function %s with invalid support number %d",
								opfamilyname, "hnsw",
								format_procedure(procform->amproc),
								procform->amprocnum)));
				result = false;
				break;
		}
	}

	/* Verify that the required distance function is present */
	if (!found_distance_proc)
	{
		ereport(INFO,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("operator class \"%s\" of access method %s is missing support function %d",
						opclassname, "hnsw", HNSW_DISTANCE_PROC)));
		result = false;
	}

	/* Check operators */
	for (i = 0; i < oprlist->n_members; i++)
	{
		HeapTuple	oprtup = &oprlist->members[i]->tuple;
		Form_pg_amop oprform = (Form_pg_amop) GETSTRUCT(oprtup);

		/* HNSW only supports ORDER BY operators (amoppurpose = 'o') */
		if (oprform->amoppurpose != AMOP_ORDER)
		{
			ereport(INFO,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("operator family \"%s\" of access method %s contains non-ordering operator %s",
							opfamilyname, "hnsw",
							format_operator(oprform->amopopr))));
			result = false;
		}

		/* Verify operator signature: should return float8 for distance */
		if (!check_amop_signature(oprform->amopopr, FLOAT8OID,
								  oprform->amoplefttype,
								  oprform->amoprighttype))
		{
			ereport(INFO,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("operator family \"%s\" of access method %s contains operator %s with wrong signature",
							opfamilyname, "hnsw",
							format_operator(oprform->amopopr))));
			result = false;
		}
	}

	/* Identify opfamily groups and verify completeness */
	grouplist = identify_opfamily_groups(oprlist, proclist);
	foreach(lc, grouplist)
	{
		OpFamilyOpFuncGroup *grp = (OpFamilyOpFuncGroup *) lfirst(lc);

		/* Each group should have the distance support function */
		if (!(grp->functionset & (((uint64) 1) << HNSW_DISTANCE_PROC)))
		{
			ereport(INFO,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("operator family \"%s\" of access method %s is missing distance function for types %s and %s",
							opfamilyname, "hnsw",
							format_type_be(grp->lefttype),
							format_type_be(grp->righttype))));
			result = false;
		}
	}

	ReleaseSysCacheList(oprlist);
	ReleaseSysCacheList(proclist);
	ReleaseSysCache(classtup);

	return result;
}
