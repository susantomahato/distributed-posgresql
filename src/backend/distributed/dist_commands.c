/*-------------------------------------------------------------------------
 *
 * dist_commands.c
 *	  DDL commands for creating distributed tables and status views
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/distributed/dist_commands.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distributed/dist_commands.h"
#include "distributed/dist_connection.h"
#include "distributed/dist_guc.h"
#include "distributed/dist_node.h"
#include "distributed/dist_shmem.h"
#include "distributed/placement.h"
#include "distributed/raft.h"
#include "distributed/rebalancer.h"
#include "distributed/shard.h"
#include "access/amapi.h"
#include "access/genam.h"
#include "access/hash.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "commands/defrem.h"
#include "catalog/pg_shard_map.h"
#include "catalog/pg_sharded_table.h"
#include "catalog/pg_dist_placement.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

PG_FUNCTION_INFO_V1(create_distributed_table);
PG_FUNCTION_INFO_V1(dist_rebalance_shards);
PG_FUNCTION_INFO_V1(dist_shard_status);
PG_FUNCTION_INFO_V1(dist_raft_status);

/*
 * create_distributed_table(table_name regclass, shard_key text,
 *                          shard_count int DEFAULT 32,
 *                          replication_factor int DEFAULT 3)
 *
 * Creates a distributed table:
 *   1. Validate table exists, shard key column is hashable
 *   2. Insert into pg_sharded_table
 *   3. Create N shard entries in pg_shard_map with hash ranges
 *   4. For each shard, create replication_factor placements in pg_dist_placement
 *   5. First placement = leader, rest = followers
 */
Datum
create_distributed_table(PG_FUNCTION_ARGS)
{
	Oid			table_oid = PG_GETARG_OID(0);
	char	   *shard_key_col = text_to_cstring(PG_GETARG_TEXT_PP(1));
	int			shard_count = PG_GETARG_INT32(2);
	int			replication_factor = PG_GETARG_INT32(3);

	CreateDistributedTableInternal(table_oid, shard_key_col,
								   shard_count, replication_factor);

	PG_RETURN_VOID();
}

/*
 * CreateDistributedTableInternal
 *		Core logic for creating a distributed table.
 */
void
CreateDistributedTableInternal(Oid table_oid, const char *shard_key_col,
							   int shard_count, int replication_factor)
{
	Relation	rel;
	HeapTuple	tuple;
	Datum		values[Natts_pg_sharded_table];
	bool		nulls[Natts_pg_sharded_table];
	List	   *nodes;
	int			num_nodes;
	int			node_idx;
	AttrNumber	attnum;
	Oid			key_type;
	int32		hash_range_size;
	ListCell   *lc;
	int			i;
	char	  **node_names;

	/* Validate table exists */
	if (!OidIsValid(table_oid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("table does not exist")));

	/* Check table is not already distributed */
	if (IsShardedTable(table_oid))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("table is already distributed")));

	/* Validate shard key column */
	attnum = get_attnum(table_oid, shard_key_col);
	if (attnum == InvalidAttrNumber)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" does not exist", shard_key_col)));

	key_type = get_atttype(table_oid, attnum);

	/* Verify the type is hashable */
	{
		Oid			hash_proc;

		hash_proc = get_opfamily_proc(get_opclass_family(
										  get_opclass_for_type(key_type)),
									  key_type, key_type,
									  HASHSTANDARD_PROC);
		if (!OidIsValid(hash_proc))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("shard key column type %s is not hashable",
							format_type_be(key_type))));
	}

	/* Get available nodes */
	nodes = GetAllDistNodes();
	num_nodes = list_length(nodes);

	if (num_nodes == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("no nodes available in the cluster"),
				 errhint("Use dist_add_node() to add nodes first.")));

	if (replication_factor > num_nodes)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("replication_factor %d exceeds number of nodes %d",
						replication_factor, num_nodes)));

	/* Build node names array for round-robin */
	node_names = palloc(num_nodes * sizeof(char *));
	i = 0;
	foreach(lc, nodes)
	{
		node_names[i++] = (char *) lfirst(lc);
	}

	/* Step 1: Insert into pg_sharded_table */
	rel = table_open(ShardedTableRelationId, RowExclusiveLock);

	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_sharded_table_relid - 1] =
		ObjectIdGetDatum(table_oid);

	/* Build shard key array (single column for now) */
	{
		Datum		key_datum;
		ArrayType  *key_array;

		key_datum = CStringGetTextDatum(shard_key_col);
		key_array = construct_array_builtin(&key_datum, 1, TEXTOID);
		values[Anum_pg_sharded_table_shardkey - 1] =
			PointerGetDatum(key_array);
	}

	values[Anum_pg_sharded_table_shardmethod - 1] =
		CharGetDatum(SHARD_METHOD_CHAR_HASH);
	values[Anum_pg_sharded_table_shardcount - 1] =
		Int32GetDatum(shard_count);
	values[Anum_pg_sharded_table_createdat - 1] =
		TimestampTzGetDatum(GetCurrentTimestamp());

	tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
	CatalogTupleInsert(rel, tuple);
	heap_freetuple(tuple);
	table_close(rel, RowExclusiveLock);

	CommandCounterIncrement();

	/* Step 2: Create shard entries in pg_shard_map */
	hash_range_size = (int32) (((int64) 0xFFFFFFFF + 1) / shard_count);
	node_idx = 0;

	for (i = 0; i < shard_count; i++)
	{
		int32		hash_min = (int32) ((int64) INT32_MIN +
									   (int64) i * hash_range_size);
		int32		hash_max;
		int32		shard_id = i + 1;
		int32		raft_group_id;
		int			j;
		Relation	shard_rel;
		Datum		shard_values[Natts_pg_shard_map];
		bool		shard_nulls[Natts_pg_shard_map];

		if (i == shard_count - 1)
			hash_max = INT32_MAX;
		else
			hash_max = hash_min + hash_range_size - 1;

		/* The "primary" node for this shard (leader) */
		char	   *leader_node = node_names[node_idx % num_nodes];

		/* Insert shard map entry */
		shard_rel = table_open(ShardMapRelationId, RowExclusiveLock);

		memset(shard_values, 0, sizeof(shard_values));
		memset(shard_nulls, false, sizeof(shard_nulls));

		shard_values[Anum_pg_shard_map_shardid - 1] =
			Int32GetDatum(shard_id);
		shard_values[Anum_pg_shard_map_relid - 1] =
			ObjectIdGetDatum(table_oid);
		shard_values[Anum_pg_shard_map_nodename - 1] =
			DirectFunctionCall1(namein, CStringGetDatum(leader_node));
		shard_values[Anum_pg_shard_map_shardmethod - 1] =
			CharGetDatum(SHARD_METHOD_CHAR_HASH);
		shard_nulls[Anum_pg_shard_map_rangemin - 1] = true;
		shard_nulls[Anum_pg_shard_map_rangemax - 1] = true;
		shard_values[Anum_pg_shard_map_hashmin - 1] =
			Int32GetDatum(hash_min);
		shard_values[Anum_pg_shard_map_hashmax - 1] =
			Int32GetDatum(hash_max);
		shard_values[Anum_pg_shard_map_shardstate - 1] =
			CharGetDatum(SHARD_STATE_CHAR_ACTIVE);
		shard_values[Anum_pg_shard_map_createdat - 1] =
			TimestampTzGetDatum(GetCurrentTimestamp());

		tuple = heap_form_tuple(RelationGetDescr(shard_rel),
								shard_values, shard_nulls);
		CatalogTupleInsert(shard_rel, tuple);
		heap_freetuple(tuple);
		table_close(shard_rel, RowExclusiveLock);

		/* Step 3: Create placements (replication_factor replicas) */
		raft_group_id = GetNextRaftGroupId();

		for (j = 0; j < replication_factor; j++)
		{
			char	   *placement_node =
				node_names[(node_idx + j) % num_nodes];
			char		role = (j == 0) ? PLACEMENT_RAFT_LEADER : PLACEMENT_RAFT_FOLLOWER;

			InsertPlacement(shard_id, placement_node, raft_group_id,
							role, PLACEMENT_STATE_ACTIVE);
		}

		node_idx++;

		/* Initialize Raft group in shared memory */
		{
			RaftGroupState *group;
			const char *peers[RAFT_MAX_PEERS];
			int			num_peers = 0;

			for (j = 0; j < replication_factor && j < RAFT_MAX_PEERS; j++)
			{
				peers[j] = node_names[(node_idx - 1 + j) % num_nodes];
				num_peers++;
			}

			group = DistShmemAllocRaftGroup(raft_group_id, shard_id);
			if (group != NULL)
				RaftGroupInit(group, raft_group_id, shard_id,
							  peers, num_peers);
		}
	}

	CommandCounterIncrement();

	elog(LOG, "distributed: created distributed table (oid=%u) with "
		 "%d shards, replication_factor=%d",
		 table_oid, shard_count, replication_factor);
}

/*
 * dist_rebalance_shards() — SQL function
 *
 * Triggers shard rebalancing across nodes.
 */
Datum
dist_rebalance_shards(PG_FUNCTION_ARGS)
{
	RebalancePlan *plan;

	plan = ComputeRebalancePlan();
	if (plan != NULL && plan->num_moves > 0)
	{
		elog(LOG, "distributed: rebalancing %d shard moves", plan->num_moves);
		ExecuteRebalancePlan(plan);
		FreeRebalancePlan(plan);
	}
	else
	{
		elog(LOG, "distributed: cluster is already balanced");
	}

	PG_RETURN_VOID();
}

/*
 * dist_shard_status() — SQL function returning shard placement info
 */
Datum
dist_shard_status(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	Relation	rel;
	SysScanDesc scan;
	HeapTuple	tuple;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/* Scan pg_shard_map */
	rel = table_open(ShardMapRelationId, AccessShareLock);
	scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_shard_map form = (Form_pg_shard_map) GETSTRUCT(tuple);
		TupleDesc	reldesc = RelationGetDescr(rel);
		Datum		values[6];
		bool		nulls[6] = {false};
		Datum		d_hashmin,
					d_hashmax,
					d_shardstate;
		bool		n_hashmin,
					n_hashmax,
					n_shardstate;

		/* shardid, relid, nodename are before varlena — safe via form */
		values[0] = Int32GetDatum(form->shardid);
		values[1] = ObjectIdGetDatum(form->relid);
		values[2] = NameGetDatum(&form->nodename);

		/*
		 * shardstate, hashmin, hashmax are after variable-length
		 * rangemin/rangemax text fields — must use heap_getattr.
		 */
		d_shardstate = heap_getattr(tuple, Anum_pg_shard_map_shardstate,
									reldesc, &n_shardstate);
		d_hashmin = heap_getattr(tuple, Anum_pg_shard_map_hashmin,
								 reldesc, &n_hashmin);
		d_hashmax = heap_getattr(tuple, Anum_pg_shard_map_hashmax,
								 reldesc, &n_hashmax);

		values[3] = n_shardstate ? CharGetDatum('\0') : d_shardstate;
		nulls[3] = n_shardstate;
		values[4] = n_hashmin ? Int32GetDatum(0) : d_hashmin;
		nulls[4] = n_hashmin;
		values[5] = n_hashmax ? Int32GetDatum(0) : d_hashmax;
		nulls[5] = n_hashmax;

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	return (Datum) 0;
}

/*
 * dist_raft_status() — SQL function returning Raft group state
 */
Datum
dist_raft_status(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/* Report Raft groups from shared memory */
	if (DistShmem != NULL)
	{
		for (int i = 0; i < MAX_RAFT_GROUPS; i++)
		{
			RaftGroupState *group = &DistShmem->raft_groups[i];
			Datum		values[7];
			bool		nulls[7] = {false};

			if (!group->in_use)
				continue;

			values[0] = Int32GetDatum(group->raft_group_id);
			values[1] = Int32GetDatum(group->shard_id);
			values[2] = CStringGetTextDatum(RaftRoleToString(group->role));
			values[3] = Int64GetDatum(group->current_term);
			values[4] = Int64GetDatum(group->commit_index);
			values[5] = Int64GetDatum(group->last_applied);
			values[6] = Int32GetDatum(group->num_peers);

			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}
	}

	MemoryContextSwitchTo(oldcontext);

	return (Datum) 0;
}
