/*-------------------------------------------------------------------------
 *
 * alohadb_distributed.c
 *	  Horizontal sharding using PG native PARTITION BY HASH and
 *	  postgres_fdw for multi-node support.
 *
 *	  Provides functions to distribute tables across hash partitions,
 *	  manage cluster node metadata, query shard info, and plan
 *	  rebalancing operations.
 *
 * Copyright (c) 2025, AlohaDB
 *
 * IDENTIFICATION
 *	  contrib/alohadb_distributed/alohadb_distributed.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"

PG_MODULE_MAGIC_EXT(
					.name = "alohadb_distributed",
					.version = "1.0"
);

/* Function declarations */
PG_FUNCTION_INFO_V1(alohadb_distributed_distribute_table);
PG_FUNCTION_INFO_V1(alohadb_distributed_undistribute_table);
PG_FUNCTION_INFO_V1(alohadb_distributed_create_reference_table);
PG_FUNCTION_INFO_V1(alohadb_distributed_add_node);
PG_FUNCTION_INFO_V1(alohadb_distributed_remove_node);
PG_FUNCTION_INFO_V1(alohadb_distributed_table_info);
PG_FUNCTION_INFO_V1(alohadb_distributed_rebalance_shards);
PG_FUNCTION_INFO_V1(alohadb_distributed_run_on_all_nodes);

/* ----------------------------------------------------------------
 * Helper: execute SPI command, ereport on failure
 * ---------------------------------------------------------------- */
static void
exec_spi_command(const char *cmd, bool read_only)
{
	int		ret;

	ret = SPI_execute(cmd, read_only, 0);
	if (ret < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("SPI_execute failed: %s", SPI_result_code_string(ret)),
				 errdetail("Command was: %s", cmd)));
}

/* ----------------------------------------------------------------
 * distribute_table(tbl_name text, dist_column text, shard_count int)
 *
 * Creates hash partitions of the given table using PG native
 * PARTITION BY HASH.  The original table data is migrated into the
 * new partitioned structure.
 * ---------------------------------------------------------------- */
Datum
alohadb_distributed_distribute_table(PG_FUNCTION_ARGS)
{
	text	   *tbl_name_txt = PG_GETARG_TEXT_PP(0);
	text	   *dist_col_txt = PG_GETARG_TEXT_PP(1);
	int32		shard_count = PG_GETARG_INT32(2);
	char	   *tbl_name = text_to_cstring(tbl_name_txt);
	char	   *dist_col = text_to_cstring(dist_col_txt);
	StringInfoData buf;
	int			i;

	if (shard_count < 1 || shard_count > 1024)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("shard_count must be between 1 and 1024")));

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* 1. Rename original table */
	initStringInfo(&buf);
	appendStringInfo(&buf, "ALTER TABLE %s RENAME TO %s_original",
					 tbl_name, tbl_name);
	exec_spi_command(buf.data, false);

	/* 2. Create partitioned table with same schema */
	resetStringInfo(&buf);
	appendStringInfo(&buf,
					 "CREATE TABLE %s (LIKE %s_original INCLUDING ALL) "
					 "PARTITION BY HASH (%s)",
					 tbl_name, tbl_name, dist_col);
	exec_spi_command(buf.data, false);

	/* 3. Create each shard partition */
	for (i = 0; i < shard_count; i++)
	{
		resetStringInfo(&buf);
		appendStringInfo(&buf,
						 "CREATE TABLE %s_shard_%d PARTITION OF %s "
						 "FOR VALUES WITH (MODULUS %d, REMAINDER %d)",
						 tbl_name, i, tbl_name, shard_count, i);
		exec_spi_command(buf.data, false);
	}

	/* 4. Migrate data */
	resetStringInfo(&buf);
	appendStringInfo(&buf,
					 "INSERT INTO %s SELECT * FROM %s_original",
					 tbl_name, tbl_name);
	exec_spi_command(buf.data, false);

	/* 5. Drop original */
	resetStringInfo(&buf);
	appendStringInfo(&buf, "DROP TABLE %s_original CASCADE", tbl_name);
	exec_spi_command(buf.data, false);

	/* 6. Record in alohadb_dist_tables */
	resetStringInfo(&buf);
	appendStringInfo(&buf,
					 "INSERT INTO alohadb_dist_tables "
					 "(table_name, dist_column, dist_method, shard_count) "
					 "VALUES ('%s', '%s', 'hash', %d)",
					 tbl_name, dist_col, shard_count);
	exec_spi_command(buf.data, false);

	/* 7. Record each shard in alohadb_dist_shards */
	for (i = 0; i < shard_count; i++)
	{
		resetStringInfo(&buf);
		appendStringInfo(&buf,
						 "INSERT INTO alohadb_dist_shards "
						 "(table_name, shard_index, status) "
						 "VALUES ('%s', %d, 'active')",
						 tbl_name, i);
		exec_spi_command(buf.data, false);
	}

	pfree(buf.data);

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * undistribute_table(tbl_name text)
 *
 * Merges all shards back into a regular (non-partitioned) table.
 * ---------------------------------------------------------------- */
Datum
alohadb_distributed_undistribute_table(PG_FUNCTION_ARGS)
{
	text	   *tbl_name_txt = PG_GETARG_TEXT_PP(0);
	char	   *tbl_name = text_to_cstring(tbl_name_txt);
	StringInfoData buf;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* 1. Save data into a temp table */
	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "CREATE TEMP TABLE _alohadb_undist_tmp AS "
					 "SELECT * FROM %s",
					 tbl_name);
	exec_spi_command(buf.data, false);

	/* 2. Drop the partitioned table (cascades to partitions) */
	resetStringInfo(&buf);
	appendStringInfo(&buf, "DROP TABLE %s CASCADE", tbl_name);
	exec_spi_command(buf.data, false);

	/* 3. Create a regular table from the temp data */
	resetStringInfo(&buf);
	appendStringInfo(&buf,
					 "CREATE TABLE %s AS SELECT * FROM _alohadb_undist_tmp",
					 tbl_name);
	exec_spi_command(buf.data, false);

	/* 4. Drop temp table */
	exec_spi_command("DROP TABLE _alohadb_undist_tmp", false);

	/* 5. Delete metadata */
	resetStringInfo(&buf);
	appendStringInfo(&buf,
					 "DELETE FROM alohadb_dist_shards WHERE table_name = '%s'",
					 tbl_name);
	exec_spi_command(buf.data, false);

	resetStringInfo(&buf);
	appendStringInfo(&buf,
					 "DELETE FROM alohadb_dist_tables WHERE table_name = '%s'",
					 tbl_name);
	exec_spi_command(buf.data, false);

	pfree(buf.data);

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * create_reference_table(tbl_name text)
 *
 * Marks a table as a reference table (replicated to all nodes).
 * Currently just records metadata; actual replication would use
 * postgres_fdw foreign tables on remote nodes.
 * ---------------------------------------------------------------- */
Datum
alohadb_distributed_create_reference_table(PG_FUNCTION_ARGS)
{
	text	   *tbl_name_txt = PG_GETARG_TEXT_PP(0);
	char	   *tbl_name = text_to_cstring(tbl_name_txt);
	StringInfoData buf;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "INSERT INTO alohadb_dist_tables "
					 "(table_name, dist_column, dist_method, shard_count) "
					 "VALUES ('%s', '*', 'reference', 1) "
					 "ON CONFLICT (table_name) DO UPDATE "
					 "SET dist_method = 'reference', shard_count = 1",
					 tbl_name);
	exec_spi_command(buf.data, false);

	pfree(buf.data);

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * add_node(node_name text, host text, port int, dbname text) → int
 *
 * Adds a node to the cluster metadata and returns the node_id.
 * ---------------------------------------------------------------- */
Datum
alohadb_distributed_add_node(PG_FUNCTION_ARGS)
{
	text	   *node_name_txt = PG_GETARG_TEXT_PP(0);
	text	   *host_txt = PG_GETARG_TEXT_PP(1);
	int32		port = PG_GETARG_INT32(2);
	text	   *dbname_txt = PG_GETARG_TEXT_PP(3);
	char	   *node_name = text_to_cstring(node_name_txt);
	char	   *host = text_to_cstring(host_txt);
	char	   *dbname = text_to_cstring(dbname_txt);
	StringInfoData buf;
	int32		node_id;
	bool		isnull;
	int			ret;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "INSERT INTO alohadb_dist_nodes "
					 "(node_name, host, port, dbname) "
					 "VALUES ('%s', '%s', %d, '%s') "
					 "RETURNING node_id",
					 node_name, host, port, dbname);

	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_INSERT_RETURNING || SPI_processed != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to insert node")));

	node_id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
										  SPI_tuptable->tupdesc,
										  1, &isnull));

	pfree(buf.data);

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_INT32(node_id);
}

/* ----------------------------------------------------------------
 * remove_node(node_name text)
 *
 * Removes a node from the cluster metadata.
 * ---------------------------------------------------------------- */
Datum
alohadb_distributed_remove_node(PG_FUNCTION_ARGS)
{
	text	   *node_name_txt = PG_GETARG_TEXT_PP(0);
	char	   *node_name = text_to_cstring(node_name_txt);
	StringInfoData buf;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* Unassign shards from this node first */
	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "UPDATE alohadb_dist_shards SET node_id = NULL "
					 "WHERE node_id = (SELECT node_id FROM alohadb_dist_nodes "
					 "WHERE node_name = '%s')",
					 node_name);
	exec_spi_command(buf.data, false);

	resetStringInfo(&buf);
	appendStringInfo(&buf,
					 "DELETE FROM alohadb_dist_nodes WHERE node_name = '%s'",
					 node_name);
	exec_spi_command(buf.data, false);

	pfree(buf.data);

	PopActiveSnapshot();
	SPI_finish();

	PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * dist_table_info(tbl_name text)
 *   → TABLE(shard_id int, shard_index int, node_name text,
 *           status text, row_count bigint)
 *
 * Shows shard information with estimated row counts.
 * ---------------------------------------------------------------- */
Datum
alohadb_distributed_table_info(PG_FUNCTION_ARGS)
{
	text	   *tbl_name_txt = PG_GETARG_TEXT_PP(0);
	char	   *tbl_name = text_to_cstring(tbl_name_txt);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	StringInfoData buf;
	int			ret;
	uint64		proc;
	uint64		i;
	Datum		values[5];
	bool		nulls[5];

	InitMaterializedSRF(fcinfo, 0);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT s.shard_id, s.shard_index, n.node_name, "
					 "s.status, "
					 "COALESCE((SELECT c.reltuples::bigint "
					 "  FROM pg_class c "
					 "  WHERE c.relname = '%s_shard_' || s.shard_index), 0) "
					 "  AS row_count "
					 "FROM alohadb_dist_shards s "
					 "LEFT JOIN alohadb_dist_nodes n ON s.node_id = n.node_id "
					 "WHERE s.table_name = '%s' "
					 "ORDER BY s.shard_index",
					 tbl_name, tbl_name);

	ret = SPI_execute(buf.data, true, 0);
	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to query shard info")));

	proc = SPI_processed;

	for (i = 0; i < proc; i++)
	{
		HeapTuple	spi_tuple = SPI_tuptable->vals[i];
		TupleDesc	spi_tupdesc = SPI_tuptable->tupdesc;
		bool		isnull;

		memset(nulls, 0, sizeof(nulls));

		/* shard_id (int4) */
		values[0] = SPI_getbinval(spi_tuple, spi_tupdesc, 1, &isnull);
		nulls[0] = isnull;

		/* shard_index (int4) */
		values[1] = SPI_getbinval(spi_tuple, spi_tupdesc, 2, &isnull);
		nulls[1] = isnull;

		/* node_name (text) */
		values[2] = SPI_getbinval(spi_tuple, spi_tupdesc, 3, &isnull);
		if (isnull)
			nulls[2] = true;
		else
			values[2] = SPI_getbinval(spi_tuple, spi_tupdesc, 3, &isnull);

		/* status (text) */
		values[3] = SPI_getbinval(spi_tuple, spi_tupdesc, 4, &isnull);
		nulls[3] = isnull;

		/* row_count (int8) */
		values[4] = SPI_getbinval(spi_tuple, spi_tupdesc, 5, &isnull);
		nulls[4] = isnull;

		tuplestore_putvalues(rsinfo->setResult,
							 rsinfo->setDesc,
							 values, nulls);
	}

	pfree(buf.data);

	PopActiveSnapshot();
	SPI_finish();

	return (Datum) 0;
}

/* ----------------------------------------------------------------
 * rebalance_shards()
 *   → TABLE(shard_id int, from_node text, to_node text, status text)
 *
 * Plans shard rebalancing across nodes. Returns the plan without
 * executing moves.  Shards on NULL (unassigned) nodes are assigned
 * to the least-loaded node.
 * ---------------------------------------------------------------- */
Datum
alohadb_distributed_rebalance_shards(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			ret;
	uint64		node_count;
	uint64		shard_count;
	uint64		i;
	int		   *node_ids = NULL;
	char	  **node_names = NULL;
	int		   *node_shard_counts = NULL;
	uint64		ideal_per_node;

	InitMaterializedSRF(fcinfo, 0);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* Get all active nodes */
	ret = SPI_execute(
		"SELECT node_id, node_name FROM alohadb_dist_nodes "
		"WHERE status = 'active' ORDER BY node_id",
		true, 0);
	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to query nodes")));

	node_count = SPI_processed;
	if (node_count == 0)
	{
		PopActiveSnapshot();
		SPI_finish();
		return (Datum) 0;
	}

	/* Copy node info to local memory */
	node_ids = palloc(sizeof(int) * node_count);
	node_names = palloc(sizeof(char *) * node_count);
	node_shard_counts = palloc0(sizeof(int) * node_count);

	for (i = 0; i < node_count; i++)
	{
		bool	isnull;

		node_ids[i] = DatumGetInt32(
			SPI_getbinval(SPI_tuptable->vals[i],
						  SPI_tuptable->tupdesc, 1, &isnull));
		node_names[i] = pstrdup(SPI_getvalue(SPI_tuptable->vals[i],
											 SPI_tuptable->tupdesc, 2));
	}

	/* Get all shards */
	ret = SPI_execute(
		"SELECT s.shard_id, s.node_id, n.node_name "
		"FROM alohadb_dist_shards s "
		"LEFT JOIN alohadb_dist_nodes n ON s.node_id = n.node_id "
		"ORDER BY s.shard_id",
		true, 0);
	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to query shards")));

	shard_count = SPI_processed;
	ideal_per_node = (shard_count + node_count - 1) / node_count;

	/* Count current assignments */
	for (i = 0; i < shard_count; i++)
	{
		bool	isnull;
		int		nid;
		uint64	j;

		nid = DatumGetInt32(
			SPI_getbinval(SPI_tuptable->vals[i],
						  SPI_tuptable->tupdesc, 2, &isnull));
		if (!isnull)
		{
			for (j = 0; j < node_count; j++)
			{
				if (node_ids[j] == nid)
				{
					node_shard_counts[j]++;
					break;
				}
			}
		}
	}

	/* Plan moves: for each shard, if unassigned or on overloaded node,
	 * assign to least-loaded node */
	for (i = 0; i < shard_count; i++)
	{
		bool		isnull;
		int			shard_id;
		char	   *from_name;
		int			cur_node_id;
		bool		needs_move = false;
		uint64		min_idx = 0;
		uint64		j;
		Datum		values[4];
		bool		nulls[4];

		shard_id = DatumGetInt32(
			SPI_getbinval(SPI_tuptable->vals[i],
						  SPI_tuptable->tupdesc, 1, &isnull));

		cur_node_id = DatumGetInt32(
			SPI_getbinval(SPI_tuptable->vals[i],
						  SPI_tuptable->tupdesc, 2, &isnull));

		if (isnull)
		{
			needs_move = true;
			from_name = "(unassigned)";
		}
		else
		{
			from_name = SPI_getvalue(SPI_tuptable->vals[i],
									 SPI_tuptable->tupdesc, 3);
			if (from_name == NULL)
				from_name = "(unknown)";

			/* Check if current node is overloaded */
			for (j = 0; j < node_count; j++)
			{
				if (node_ids[j] == cur_node_id)
				{
					if ((uint64) node_shard_counts[j] > ideal_per_node)
						needs_move = true;
					break;
				}
			}
		}

		if (!needs_move)
			continue;

		/* Find least-loaded node */
		for (j = 1; j < node_count; j++)
		{
			if (node_shard_counts[j] < node_shard_counts[min_idx])
				min_idx = j;
		}

		memset(nulls, 0, sizeof(nulls));
		values[0] = Int32GetDatum(shard_id);
		values[1] = CStringGetTextDatum(from_name);
		values[2] = CStringGetTextDatum(node_names[min_idx]);
		values[3] = CStringGetTextDatum("planned");

		tuplestore_putvalues(rsinfo->setResult,
							 rsinfo->setDesc,
							 values, nulls);

		node_shard_counts[min_idx]++;
	}

	pfree(node_ids);
	pfree(node_names);
	pfree(node_shard_counts);

	PopActiveSnapshot();
	SPI_finish();

	return (Datum) 0;
}

/* ----------------------------------------------------------------
 * run_on_all_nodes(command text)
 *   → TABLE(node_name text, result text, success bool)
 *
 * Stub that shows what would be executed on each node.
 * Does not actually connect to remote nodes.
 * ---------------------------------------------------------------- */
Datum
alohadb_distributed_run_on_all_nodes(PG_FUNCTION_ARGS)
{
	text	   *cmd_txt = PG_GETARG_TEXT_PP(0);
	char	   *command = text_to_cstring(cmd_txt);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			ret;
	uint64		proc;
	uint64		i;
	StringInfoData result_buf;

	InitMaterializedSRF(fcinfo, 0);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(
		"SELECT node_name, host, port, dbname "
		"FROM alohadb_dist_nodes WHERE status = 'active' "
		"ORDER BY node_id",
		true, 0);
	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to query nodes")));

	proc = SPI_processed;
	initStringInfo(&result_buf);

	for (i = 0; i < proc; i++)
	{
		HeapTuple	spi_tuple = SPI_tuptable->vals[i];
		TupleDesc	spi_tupdesc = SPI_tuptable->tupdesc;
		char	   *nname;
		char	   *host;
		char	   *port;
		char	   *dbname;
		Datum		values[3];
		bool		nulls[3];

		nname = SPI_getvalue(spi_tuple, spi_tupdesc, 1);
		host = SPI_getvalue(spi_tuple, spi_tupdesc, 2);
		port = SPI_getvalue(spi_tuple, spi_tupdesc, 3);
		dbname = SPI_getvalue(spi_tuple, spi_tupdesc, 4);

		resetStringInfo(&result_buf);
		appendStringInfo(&result_buf,
						 "would execute on %s:%s/%s: %s",
						 host, port, dbname, command);

		memset(nulls, 0, sizeof(nulls));
		values[0] = CStringGetTextDatum(nname);
		values[1] = CStringGetTextDatum(result_buf.data);
		values[2] = BoolGetDatum(true);

		tuplestore_putvalues(rsinfo->setResult,
							 rsinfo->setDesc,
							 values, nulls);
	}

	pfree(result_buf.data);

	PopActiveSnapshot();
	SPI_finish();

	return (Datum) 0;
}
