/*-------------------------------------------------------------------------
 *
 * placement.h
 *	  Placement catalog CRUD operations
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/include/distributed/placement.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PLACEMENT_H
#define PLACEMENT_H

#include "catalog/pg_dist_placement.h"
#include "nodes/pg_list.h"

/*
 * PlacementInfo — in-memory representation of a placement
 */
typedef struct PlacementInfo
{
	int64		placementid;
	int32		shardid;
	char		nodename[NAMEDATALEN];
	int32		raftgroupid;
	char		raftrole;
	char		placementstate;
	int64		raftterm;
	TimestampTz createdat;
} PlacementInfo;

/* CRUD operations */
extern int64 InsertPlacement(int32 shard_id, const char *node_name,
							 int32 raft_group_id, char raft_role,
							 char placement_state);
extern void UpdatePlacementRole(int64 placement_id, char new_role);
extern void UpdatePlacementState(int64 placement_id, char new_state);
extern void UpdatePlacementTerm(int64 placement_id, int64 new_term);
extern void DeletePlacement(int64 placement_id);

/* Lookup operations */
extern List *GetPlacementsForShard(int32 shard_id);
extern List *GetPlacementsForNode(const char *node_name);
extern PlacementInfo *GetLeaderPlacement(int32 shard_id);
extern PlacementInfo *GetPlacementById(int64 placement_id);
extern List *GetPlacementsForRaftGroup(int32 raft_group_id);

/* Utility */
extern int32 GetNextRaftGroupId(void);
extern void FreePlacementInfo(PlacementInfo *info);

#endif							/* PLACEMENT_H */
