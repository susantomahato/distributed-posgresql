/*-------------------------------------------------------------------------
 *
 * rebalancer.c
 *	  Shard redistribution when nodes join or leave
 *
 * The rebalancer computes a plan to move shard placements so that
 * each node has approximately the same number of placements.
 * Moves are executed online — reads/writes continue during migration.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/distributed/rebalancer.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distributed/dist_connection.h"
#include "distributed/dist_guc.h"
#include "distributed/dist_node.h"
#include "distributed/dist_shmem.h"
#include "distributed/failure_detector.h"
#include "distributed/placement.h"
#include "distributed/rebalancer.h"
#include "distributed/raft.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

/*
 * NodePlacementCount — helper for sorting nodes by placement count
 */
typedef struct NodePlacementCount
{
	char		node_name[NAMEDATALEN];
	int			count;
} NodePlacementCount;

/* Comparison function for sorting */
static int
node_count_cmp(const void *a, const void *b)
{
	const NodePlacementCount *na = (const NodePlacementCount *) a;
	const NodePlacementCount *nb = (const NodePlacementCount *) b;

	return na->count - nb->count;
}

/*
 * ComputeRebalancePlan
 *		Compute a plan to balance placements across nodes.
 *
 * Target: total_placements / num_nodes placements per node (±1).
 * Moves placements from overloaded nodes to underloaded nodes.
 */
RebalancePlan *
ComputeRebalancePlan(void)
{
	List	   *all_nodes;
	int			num_nodes;
	int			total_placements = 0;
	int			target_per_node;
	NodePlacementCount *node_counts;
	ListCell   *lc;
	int			i;
	RebalancePlan *plan;
	int			max_moves;
	int			move_count = 0;
	ShardMove  *moves;

	all_nodes = GetAllDistNodes();
	num_nodes = list_length(all_nodes);

	if (num_nodes <= 1)
	{
		list_free_deep(all_nodes);
		return NULL;
	}

	/* Count placements per node */
	node_counts = palloc0(num_nodes * sizeof(NodePlacementCount));
	i = 0;
	foreach(lc, all_nodes)
	{
		char	   *name = (char *) lfirst(lc);
		List	   *placements;

		strlcpy(node_counts[i].node_name, name, NAMEDATALEN);
		placements = GetPlacementsForNode(name);
		node_counts[i].count = list_length(placements);
		total_placements += node_counts[i].count;
		list_free(placements);
		i++;
	}

	target_per_node = total_placements / num_nodes;

	/* Sort by count (ascending) */
	qsort(node_counts, num_nodes, sizeof(NodePlacementCount),
		  node_count_cmp);

	/* Compute moves */
	max_moves = total_placements;	/* upper bound */
	moves = palloc0(max_moves * sizeof(ShardMove));

	{
		int			lo = 0;
		int			hi = num_nodes - 1;

		while (lo < hi)
		{
			if (node_counts[lo].count >= target_per_node)
				break;			/* everyone at or above target */

			if (node_counts[hi].count <= target_per_node + 1)
			{
				hi--;
				continue;
			}

			/* Move one placement from hi to lo */
			{
				List	   *hi_placements;
				PlacementInfo *p;

				hi_placements = GetPlacementsForNode(
					node_counts[hi].node_name);
				if (hi_placements == NIL)
				{
					hi--;
					continue;
				}

				p = (PlacementInfo *) linitial(hi_placements);

				moves[move_count].shard_id = p->shardid;
				moves[move_count].raft_group_id = p->raftgroupid;
				strlcpy(moves[move_count].from_node,
						node_counts[hi].node_name, NAMEDATALEN);
				strlcpy(moves[move_count].to_node,
						node_counts[lo].node_name, NAMEDATALEN);

				move_count++;
				node_counts[lo].count++;
				node_counts[hi].count--;

				list_free(hi_placements);
			}

			if (node_counts[lo].count >= target_per_node)
				lo++;
		}
	}

	list_free_deep(all_nodes);
	pfree(node_counts);

	if (move_count == 0)
	{
		pfree(moves);
		return NULL;
	}

	plan = palloc(sizeof(RebalancePlan));
	plan->num_moves = move_count;
	plan->moves = moves;

	elog(LOG, "distributed: rebalance plan computed with %d moves",
		 move_count);

	return plan;
}

/*
 * MoveShard
 *		Move a shard placement from one node to another.
 *
 * The move is online: reads/writes continue during migration.
 *
 * Steps:
 *   1. Add to_node to Raft group as LEARNER
 *   2. Wait for LEARNER to catch up
 *   3. Promote LEARNER to FOLLOWER
 *   4. Remove from_node from Raft group
 *   5. Update pg_dist_placement
 */
void
MoveShard(int32 shard_id, const char *from_node, const char *to_node)
{
	PlacementInfo *from_placement = NULL;
	List	   *placements;
	ListCell   *lc;
	int32		raft_group_id = 0;

	elog(LOG, "distributed: moving shard %d from \"%s\" to \"%s\"",
		 shard_id, from_node, to_node);

	/* Find the placement to move */
	placements = GetPlacementsForShard(shard_id);
	foreach(lc, placements)
	{
		PlacementInfo *p = (PlacementInfo *) lfirst(lc);

		if (strcmp(p->nodename, from_node) == 0 &&
			p->placementstate == PLACEMENT_STATE_ACTIVE)
		{
			from_placement = p;
			raft_group_id = p->raftgroupid;
			break;
		}
	}

	if (from_placement == NULL)
	{
		elog(WARNING, "distributed: no active placement for shard %d "
			 "on node \"%s\"",
			 shard_id, from_node);
		list_free(placements);
		return;
	}

	/* Step 1: Create new placement as LEARNER */
	InsertPlacement(shard_id, to_node, raft_group_id,
					PLACEMENT_RAFT_LEARNER, PLACEMENT_STATE_SYNCING);

	CommandCounterIncrement();

	/* Step 2: Initiate data transfer (via re-replication mechanism) */
	InitiateReReplication(raft_group_id, to_node);

	/*
	 * Steps 3-5 happen asynchronously in the re-replication worker:
	 *   - LEARNER catches up
	 *   - LEARNER promoted to FOLLOWER
	 *   - Old placement removed
	 *
	 * For now, mark the old placement for decommissioning.
	 */
	UpdatePlacementState(from_placement->placementid,
						 PLACEMENT_STATE_DECOMMISSIONING);

	list_free(placements);

	elog(LOG, "distributed: shard %d move initiated from \"%s\" to \"%s\"",
		 shard_id, from_node, to_node);
}

/*
 * ExecuteRebalancePlan
 *		Execute all moves in a rebalance plan.
 */
void
ExecuteRebalancePlan(RebalancePlan *plan)
{
	int			i;

	if (plan == NULL || plan->num_moves == 0)
		return;

	elog(LOG, "distributed: executing rebalance plan with %d moves",
		 plan->num_moves);

	for (i = 0; i < plan->num_moves; i++)
	{
		ShardMove  *move = &plan->moves[i];

		elog(LOG, "distributed: rebalance move %d/%d: shard %d "
			 "\"%s\" -> \"%s\"",
			 i + 1, plan->num_moves,
			 move->shard_id, move->from_node, move->to_node);

		PG_TRY();
		{
			MoveShard(move->shard_id, move->from_node, move->to_node);
		}
		PG_CATCH();
		{
			elog(WARNING, "distributed: failed to move shard %d, "
				 "skipping",
				 move->shard_id);
			FlushErrorState();
		}
		PG_END_TRY();
	}

	elog(LOG, "distributed: rebalance plan execution complete");
}

/*
 * FreeRebalancePlan
 *		Free a rebalance plan.
 */
void
FreeRebalancePlan(RebalancePlan *plan)
{
	if (plan == NULL)
		return;

	if (plan->moves)
		pfree(plan->moves);
	pfree(plan);
}
