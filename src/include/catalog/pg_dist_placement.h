/*-------------------------------------------------------------------------
 *
 * pg_dist_placement.h
 *	  definition of the "distributed placement" system catalog
 *	  (pg_dist_placement)
 *
 * This catalog tracks the placement of shard replicas across nodes,
 * including their Raft group membership and role.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/include/catalog/pg_dist_placement.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_DIST_PLACEMENT_H
#define PG_DIST_PLACEMENT_H

#include "catalog/genbki.h"
#include "datatype/timestamp.h"
#include "catalog/pg_dist_placement_d.h"

/* ----------------
 *		pg_dist_placement definition.  cpp turns this into
 *		typedef struct FormData_pg_dist_placement
 * ----------------
 */
CATALOG(pg_dist_placement,9040,DistPlacementRelationId)
{
	/* unique placement identifier */
	int32		placementid;

	/* shard identifier (FK to pg_shard_map.shardid) */
	int32		shardid;

	/* node name (FK to pg_shard_node.nodename) */
	NameData	nodename;

	/* Raft group identifier (same for all replicas of a shard) */
	int32		raftgroupid;

	/* Raft role: 'l'=leader, 'f'=follower, 'c'=candidate, 'n'=learner */
	char		raftrole BKI_DEFAULT(f);

	/* placement state: 'a'=active, 's'=syncing, 'd'=decommissioning */
	char		placementstate BKI_DEFAULT(a);

	/* current Raft term */
	int64		raftterm BKI_DEFAULT(0);

	/* placement creation timestamp */
	TimestampTz	createdat BKI_DEFAULT(now);
} FormData_pg_dist_placement;

/* ----------------
 *		Form_pg_dist_placement corresponds to a pointer to a tuple with
 *		the format of pg_dist_placement relation.
 * ----------------
 */
typedef FormData_pg_dist_placement *Form_pg_dist_placement;

DECLARE_UNIQUE_INDEX_PKEY(pg_dist_placement_placementid_index, 9141, DistPlacementPlacementidIndexId, pg_dist_placement, btree(placementid int4_ops));
DECLARE_INDEX(pg_dist_placement_shardid_index, 9142, DistPlacementShardidIndexId, pg_dist_placement, btree(shardid int4_ops));
DECLARE_INDEX(pg_dist_placement_nodename_index, 9143, DistPlacementNodenameIndexId, pg_dist_placement, btree(nodename name_ops));
DECLARE_INDEX(pg_dist_placement_raftgroupid_index, 9144, DistPlacementRaftgroupidIndexId, pg_dist_placement, btree(raftgroupid int4_ops));

MAKE_SYSCACHE(DISTPLACEMENTID, pg_dist_placement_placementid_index, 8);

/*
 * Raft role constants
 */
#define PLACEMENT_RAFT_LEADER	'l'
#define PLACEMENT_RAFT_FOLLOWER	'f'
#define PLACEMENT_RAFT_CANDIDATE	'c'
#define PLACEMENT_RAFT_LEARNER	'n'

/*
 * Placement state constants
 */
#define PLACEMENT_STATE_ACTIVE			'a'
#define PLACEMENT_STATE_SYNCING			's'
#define PLACEMENT_STATE_DECOMMISSIONING	'd'

#endif							/* PG_DIST_PLACEMENT_H */
