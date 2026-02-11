/*-------------------------------------------------------------------------
 *
 * rebalancer.h
 *	  Shard rebalancing API
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/include/distributed/rebalancer.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef REBALANCER_H
#define REBALANCER_H

#include "fmgr.h"

/*
 * ShardMove — describes moving a shard placement between nodes
 */
typedef struct ShardMove
{
	int32		shard_id;
	int32		raft_group_id;
	char		from_node[NAMEDATALEN];
	char		to_node[NAMEDATALEN];
} ShardMove;

/*
 * RebalancePlan — a set of shard moves to achieve balance
 */
typedef struct RebalancePlan
{
	int			num_moves;
	ShardMove  *moves;
} RebalancePlan;

/* Compute a rebalance plan */
extern RebalancePlan *ComputeRebalancePlan(void);

/* Execute a single shard move */
extern void MoveShard(int32 shard_id, const char *from_node,
					  const char *to_node);

/* Execute a full rebalance */
extern void ExecuteRebalancePlan(RebalancePlan *plan);

/* Free a rebalance plan */
extern void FreeRebalancePlan(RebalancePlan *plan);

#endif							/* REBALANCER_H */
