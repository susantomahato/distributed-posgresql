/*-------------------------------------------------------------------------
 *
 * query_router.h
 *	  Query routing infrastructure for distributed tables
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/include/distributed/query_router.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef QUERY_ROUTER_H
#define QUERY_ROUTER_H

#include "nodes/pathnodes.h"
#include "nodes/plannodes.h"
#include "optimizer/planner.h"

/*
 * RouteType — how to execute a distributed query
 */
typedef enum RouteType
{
	ROUTE_NONE = 0,			/* not a distributed query */
	ROUTE_LOCAL_LEADER,		/* single-shard, this node is leader */
	ROUTE_REMOTE_FORWARD,	/* single-shard, remote node is leader */
	ROUTE_SCATTER_GATHER	/* multi-shard query */
} RouteType;

/*
 * ShardRouteInfo — routing decision for a query
 */
typedef struct ShardRouteInfo
{
	RouteType	route_type;
	bool		is_single_shard;
	int32		shard_id;
	char		leader_node[NAMEDATALEN];
	Oid			table_oid;
	int			num_shards;
	int32	   *shard_ids;		/* for scatter-gather */
	char	  **leader_nodes;	/* for scatter-gather */
} ShardRouteInfo;

/* Planner hook */
extern PlannedStmt *DistPlannerHook(Query *parse,
									const char *query_string,
									int cursorOptions,
									ParamListInfo boundParams,
									ExplainState *es);

/* Shard key extraction */
extern ShardRouteInfo *ExtractShardRoute(Query *parse);

/* Check if a query involves distributed tables */
extern bool QueryInvolvesDistributedTables(Query *parse);

/* Free a ShardRouteInfo */
extern void FreeShardRouteInfo(ShardRouteInfo *info);

#endif							/* QUERY_ROUTER_H */
