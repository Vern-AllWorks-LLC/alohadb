/*-------------------------------------------------------------------------
 *
 * alohadb_nosql.h
 *	  Shared declarations for the alohadb_nosql extension.
 *
 *	  Provides a document database API over PostgreSQL JSONB.
 *	  Collections are ordinary tables with (_id text, data jsonb).
 *	  All query optimization is delegated to PostgreSQL's native
 *	  planner via GIN indexes and jsonb_path_ops.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * contrib/alohadb_nosql/alohadb_nosql.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef ALOHADB_NOSQL_H
#define ALOHADB_NOSQL_H

#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"

/*
 * Metadata table that tracks all document collections.
 */
#define NOSQL_COLLECTIONS_TABLE		"alohadb_nosql_collections"

/*
 * Maximum length of a collection name.
 */
#define NOSQL_COLLECTION_MAXLEN		NAMEDATALEN

/* ----------------------------------------------------------------
 * Collection management functions (nosql_collection.c)
 * ---------------------------------------------------------------- */
extern Datum doc_create_collection(PG_FUNCTION_ARGS);
extern Datum doc_drop_collection(PG_FUNCTION_ARGS);
extern Datum doc_list_collections(PG_FUNCTION_ARGS);

/* ----------------------------------------------------------------
 * CRUD functions (nosql_crud.c)
 * ---------------------------------------------------------------- */
extern Datum doc_insert(PG_FUNCTION_ARGS);
extern Datum doc_insert_batch(PG_FUNCTION_ARGS);
extern Datum doc_get(PG_FUNCTION_ARGS);
extern Datum doc_put(PG_FUNCTION_ARGS);
extern Datum doc_remove(PG_FUNCTION_ARGS);
extern Datum doc_count(PG_FUNCTION_ARGS);
extern Datum doc_patch(PG_FUNCTION_ARGS);

/* ----------------------------------------------------------------
 * Query/search/analytics functions (nosql_query.c)
 * ---------------------------------------------------------------- */
extern Datum doc_search(PG_FUNCTION_ARGS);
extern Datum doc_query(PG_FUNCTION_ARGS);
extern Datum doc_group(PG_FUNCTION_ARGS);
extern Datum doc_array_append(PG_FUNCTION_ARGS);
extern Datum doc_array_remove(PG_FUNCTION_ARGS);
extern Datum doc_array_add_unique(PG_FUNCTION_ARGS);

/* ----------------------------------------------------------------
 * Internal helper: convert filter jsonb to SQL WHERE clause.
 * Defined in nosql_query.c, used by nosql_crud.c as well.
 * ---------------------------------------------------------------- */
extern void nosql_filter_to_where(StringInfo buf, const char *filter_str);

#endif							/* ALOHADB_NOSQL_H */
