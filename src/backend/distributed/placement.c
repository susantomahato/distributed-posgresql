/*-------------------------------------------------------------------------
 *
 * placement.c
 *	  Placement catalog CRUD operations for pg_dist_placement
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/distributed/placement.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distributed/placement.h"
#include "distributed/dist_guc.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/indexing.h"
#include "catalog/pg_dist_placement.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/syscache.h"

/* Sequence counter for placement IDs (simple in-memory counter) */
static int64 nextPlacementId = 1;

/* Sequence counter for Raft group IDs */
static int32 nextRaftGroupId = 1;

/*
 * InsertPlacement
 *		Insert a new row into pg_dist_placement.
 *
 * Returns the assigned placement ID.
 */
int64
InsertPlacement(int32 shard_id, const char *node_name,
				int32 raft_group_id, char raft_role,
				char placement_state)
{
	Relation	rel;
	HeapTuple	tuple;
	Datum		values[Natts_pg_dist_placement];
	bool		nulls[Natts_pg_dist_placement];
	int64		placement_id;

	rel = table_open(DistPlacementRelationId, RowExclusiveLock);

	placement_id = nextPlacementId++;

	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_dist_placement_placementid - 1] =
		Int64GetDatum(placement_id);
	values[Anum_pg_dist_placement_shardid - 1] =
		Int32GetDatum(shard_id);
	values[Anum_pg_dist_placement_nodename - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(node_name));
	values[Anum_pg_dist_placement_raftgroupid - 1] =
		Int32GetDatum(raft_group_id);
	values[Anum_pg_dist_placement_raftrole - 1] =
		CharGetDatum(raft_role);
	values[Anum_pg_dist_placement_placementstate - 1] =
		CharGetDatum(placement_state);
	values[Anum_pg_dist_placement_raftterm - 1] =
		Int64GetDatum(0);
	values[Anum_pg_dist_placement_createdat - 1] =
		TimestampTzGetDatum(GetCurrentTimestamp());

	tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
	CatalogTupleInsert(rel, tuple);
	heap_freetuple(tuple);

	table_close(rel, RowExclusiveLock);

	return placement_id;
}

/*
 * UpdatePlacementRole
 *		Update the Raft role for a placement.
 */
void
UpdatePlacementRole(int64 placement_id, char new_role)
{
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData skey[1];
	HeapTuple	tuple;
	HeapTuple	newtuple;
	Datum		values[Natts_pg_dist_placement];
	bool		nulls[Natts_pg_dist_placement];
	bool		replaces[Natts_pg_dist_placement];

	rel = table_open(DistPlacementRelationId, RowExclusiveLock);

	ScanKeyInit(&skey[0],
				Anum_pg_dist_placement_placementid,
				BTEqualStrategyNumber, F_INT8EQ,
				Int64GetDatum(placement_id));

	scan = systable_beginscan(rel, DistPlacementPlacementidIndexId,
							  true, NULL, 1, skey);

	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("placement %lld does not exist",
						(long long) placement_id)));

	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));
	memset(replaces, false, sizeof(replaces));

	replaces[Anum_pg_dist_placement_raftrole - 1] = true;
	values[Anum_pg_dist_placement_raftrole - 1] = CharGetDatum(new_role);

	newtuple = heap_modify_tuple(tuple, RelationGetDescr(rel),
								 values, nulls, replaces);
	CatalogTupleUpdate(rel, &newtuple->t_self, newtuple);
	heap_freetuple(newtuple);

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);
}

/*
 * UpdatePlacementState
 *		Update the placement state.
 */
void
UpdatePlacementState(int64 placement_id, char new_state)
{
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData skey[1];
	HeapTuple	tuple;
	HeapTuple	newtuple;
	Datum		values[Natts_pg_dist_placement];
	bool		nulls[Natts_pg_dist_placement];
	bool		replaces[Natts_pg_dist_placement];

	rel = table_open(DistPlacementRelationId, RowExclusiveLock);

	ScanKeyInit(&skey[0],
				Anum_pg_dist_placement_placementid,
				BTEqualStrategyNumber, F_INT8EQ,
				Int64GetDatum(placement_id));

	scan = systable_beginscan(rel, DistPlacementPlacementidIndexId,
							  true, NULL, 1, skey);

	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("placement %lld does not exist",
						(long long) placement_id)));

	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));
	memset(replaces, false, sizeof(replaces));

	replaces[Anum_pg_dist_placement_placementstate - 1] = true;
	values[Anum_pg_dist_placement_placementstate - 1] =
		CharGetDatum(new_state);

	newtuple = heap_modify_tuple(tuple, RelationGetDescr(rel),
								 values, nulls, replaces);
	CatalogTupleUpdate(rel, &newtuple->t_self, newtuple);
	heap_freetuple(newtuple);

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);
}

/*
 * UpdatePlacementTerm
 *		Update the Raft term for a placement.
 */
void
UpdatePlacementTerm(int64 placement_id, int64 new_term)
{
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData skey[1];
	HeapTuple	tuple;
	HeapTuple	newtuple;
	Datum		values[Natts_pg_dist_placement];
	bool		nulls[Natts_pg_dist_placement];
	bool		replaces[Natts_pg_dist_placement];

	rel = table_open(DistPlacementRelationId, RowExclusiveLock);

	ScanKeyInit(&skey[0],
				Anum_pg_dist_placement_placementid,
				BTEqualStrategyNumber, F_INT8EQ,
				Int64GetDatum(placement_id));

	scan = systable_beginscan(rel, DistPlacementPlacementidIndexId,
							  true, NULL, 1, skey);

	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("placement %lld does not exist",
						(long long) placement_id)));

	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));
	memset(replaces, false, sizeof(replaces));

	replaces[Anum_pg_dist_placement_raftterm - 1] = true;
	values[Anum_pg_dist_placement_raftterm - 1] = Int64GetDatum(new_term);

	newtuple = heap_modify_tuple(tuple, RelationGetDescr(rel),
								 values, nulls, replaces);
	CatalogTupleUpdate(rel, &newtuple->t_self, newtuple);
	heap_freetuple(newtuple);

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);
}

/*
 * DeletePlacement
 *		Remove a placement from pg_dist_placement.
 */
void
DeletePlacement(int64 placement_id)
{
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData skey[1];
	HeapTuple	tuple;

	rel = table_open(DistPlacementRelationId, RowExclusiveLock);

	ScanKeyInit(&skey[0],
				Anum_pg_dist_placement_placementid,
				BTEqualStrategyNumber, F_INT8EQ,
				Int64GetDatum(placement_id));

	scan = systable_beginscan(rel, DistPlacementPlacementidIndexId,
							  true, NULL, 1, skey);

	tuple = systable_getnext(scan);
	if (HeapTupleIsValid(tuple))
		CatalogTupleDelete(rel, &tuple->t_self);

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);
}

/*
 * GetPlacementsForShard
 *		Get all placements for a given shard.
 */
List *
GetPlacementsForShard(int32 shard_id)
{
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData skey[1];
	HeapTuple	tuple;
	List	   *result = NIL;

	rel = table_open(DistPlacementRelationId, AccessShareLock);

	ScanKeyInit(&skey[0],
				Anum_pg_dist_placement_shardid,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(shard_id));

	scan = systable_beginscan(rel, DistPlacementShardidIndexId,
							  true, NULL, 1, skey);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_dist_placement form =
			(Form_pg_dist_placement) GETSTRUCT(tuple);
		PlacementInfo *info = palloc(sizeof(PlacementInfo));

		info->placementid = form->placementid;
		info->shardid = form->shardid;
		strlcpy(info->nodename, NameStr(form->nodename), NAMEDATALEN);
		info->raftgroupid = form->raftgroupid;
		info->raftrole = form->raftrole;
		info->placementstate = form->placementstate;
		info->raftterm = form->raftterm;
		info->createdat = form->createdat;

		result = lappend(result, info);
	}

	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	return result;
}

/*
 * GetPlacementsForNode
 *		Get all placements on a given node.
 */
List *
GetPlacementsForNode(const char *node_name)
{
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData skey[1];
	HeapTuple	tuple;
	List	   *result = NIL;

	rel = table_open(DistPlacementRelationId, AccessShareLock);

	ScanKeyInit(&skey[0],
				Anum_pg_dist_placement_nodename,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(node_name));

	scan = systable_beginscan(rel, DistPlacementNodenameIndexId,
							  true, NULL, 1, skey);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_dist_placement form =
			(Form_pg_dist_placement) GETSTRUCT(tuple);
		PlacementInfo *info = palloc(sizeof(PlacementInfo));

		info->placementid = form->placementid;
		info->shardid = form->shardid;
		strlcpy(info->nodename, NameStr(form->nodename), NAMEDATALEN);
		info->raftgroupid = form->raftgroupid;
		info->raftrole = form->raftrole;
		info->placementstate = form->placementstate;
		info->raftterm = form->raftterm;
		info->createdat = form->createdat;

		result = lappend(result, info);
	}

	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	return result;
}

/*
 * GetLeaderPlacement
 *		Get the leader placement for a given shard.
 *
 * Returns NULL if no leader found.
 */
PlacementInfo *
GetLeaderPlacement(int32 shard_id)
{
	List	   *placements;
	ListCell   *lc;

	placements = GetPlacementsForShard(shard_id);

	foreach(lc, placements)
	{
		PlacementInfo *info = (PlacementInfo *) lfirst(lc);

		if (info->raftrole == PLACEMENT_RAFT_LEADER &&
			info->placementstate == PLACEMENT_STATE_ACTIVE)
			return info;
	}

	return NULL;
}

/*
 * GetPlacementById
 *		Look up a placement by its ID.
 */
PlacementInfo *
GetPlacementById(int64 placement_id)
{
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData skey[1];
	HeapTuple	tuple;
	PlacementInfo *info = NULL;

	rel = table_open(DistPlacementRelationId, AccessShareLock);

	ScanKeyInit(&skey[0],
				Anum_pg_dist_placement_placementid,
				BTEqualStrategyNumber, F_INT8EQ,
				Int64GetDatum(placement_id));

	scan = systable_beginscan(rel, DistPlacementPlacementidIndexId,
							  true, NULL, 1, skey);

	tuple = systable_getnext(scan);
	if (HeapTupleIsValid(tuple))
	{
		Form_pg_dist_placement form =
			(Form_pg_dist_placement) GETSTRUCT(tuple);

		info = palloc(sizeof(PlacementInfo));
		info->placementid = form->placementid;
		info->shardid = form->shardid;
		strlcpy(info->nodename, NameStr(form->nodename), NAMEDATALEN);
		info->raftgroupid = form->raftgroupid;
		info->raftrole = form->raftrole;
		info->placementstate = form->placementstate;
		info->raftterm = form->raftterm;
		info->createdat = form->createdat;
	}

	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	return info;
}

/*
 * GetPlacementsForRaftGroup
 *		Get all placements in a Raft group.
 */
List *
GetPlacementsForRaftGroup(int32 raft_group_id)
{
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData skey[1];
	HeapTuple	tuple;
	List	   *result = NIL;

	rel = table_open(DistPlacementRelationId, AccessShareLock);

	ScanKeyInit(&skey[0],
				Anum_pg_dist_placement_raftgroupid,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(raft_group_id));

	scan = systable_beginscan(rel, DistPlacementRaftgroupidIndexId,
							  true, NULL, 1, skey);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_dist_placement form =
			(Form_pg_dist_placement) GETSTRUCT(tuple);
		PlacementInfo *info = palloc(sizeof(PlacementInfo));

		info->placementid = form->placementid;
		info->shardid = form->shardid;
		strlcpy(info->nodename, NameStr(form->nodename), NAMEDATALEN);
		info->raftgroupid = form->raftgroupid;
		info->raftrole = form->raftrole;
		info->placementstate = form->placementstate;
		info->raftterm = form->raftterm;
		info->createdat = form->createdat;

		result = lappend(result, info);
	}

	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	return result;
}

/*
 * GetNextRaftGroupId
 *		Return a new unique Raft group ID.
 */
int32
GetNextRaftGroupId(void)
{
	return nextRaftGroupId++;
}

/*
 * FreePlacementInfo
 *		Free a PlacementInfo structure.
 */
void
FreePlacementInfo(PlacementInfo *info)
{
	if (info != NULL)
		pfree(info);
}
