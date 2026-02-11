/*-------------------------------------------------------------------------
 *
 * query_router.c
 *	  Shard key extraction and query routing for distributed tables
 *
 * The planner hook intercepts queries involving distributed tables,
 * extracts the shard key from WHERE clause equality constraints,
 * determines which shard the query should go to, and which node
 * is the leader for that shard.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/distributed/query_router.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distributed/dist_guc.h"
#include "distributed/dist_node.h"
#include "distributed/placement.h"
#include "distributed/query_router.h"
#include "distributed/shard.h"
#include "catalog/pg_shard_map.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

/* Custom plan node tag for distributed plans */
#define T_DistributedPlan		1000

/*
 * ShardKeyExtractContext — walker context for finding shard key = const
 */
typedef struct ShardKeyExtractContext
{
	Oid			table_oid;
	AttrNumber	shard_key_attnum;
	bool		found;
	Datum		shard_key_value;
	Oid			shard_key_type;
} ShardKeyExtractContext;

/* Forward declarations */
static bool extract_shard_key_walker(Node *node,
									 ShardKeyExtractContext *context);

/*
 * DistPlannerHook
 *		Planner hook for distributed query routing.
 *
 * Returns a PlannedStmt if we handle the query, NULL to fall through
 * to the standard planner.
 */
PlannedStmt *
DistPlannerHook(Query *parse, const char *query_string,
				int cursorOptions, ParamListInfo boundParams,
				ExplainState *es)
{
	ShardRouteInfo *route;

	/* Only handle SELECT/INSERT/UPDATE/DELETE */
	if (parse->commandType != CMD_SELECT &&
		parse->commandType != CMD_INSERT &&
		parse->commandType != CMD_UPDATE &&
		parse->commandType != CMD_DELETE)
		return NULL;

	/* Check if any tables in the query are distributed */
	if (!QueryInvolvesDistributedTables(parse))
		return NULL;

	/* Extract routing information */
	route = ExtractShardRoute(parse);
	if (route == NULL || route->route_type == ROUTE_NONE)
		return NULL;

	/*
	 * For local leader queries, let the standard planner handle it.
	 * The executor hook will handle Raft proposal for writes.
	 */
	if (route->route_type == ROUTE_LOCAL_LEADER)
	{
		/* Store route info for the executor to find */
		FreeShardRouteInfo(route);
		return NULL;
	}

	/*
	 * For remote forward and scatter-gather, we create a standard plan
	 * and attach routing info. The executor hooks will detect the
	 * distributed plan and handle it accordingly.
	 *
	 * For now, fall through to standard planner; the executor hooks
	 * will re-extract routing info and handle distribution.
	 */
	FreeShardRouteInfo(route);
	return NULL;
}

/*
 * QueryInvolvesDistributedTables
 *		Check if any range table entry in the query is a distributed table.
 */
bool
QueryInvolvesDistributedTables(Query *parse)
{
	ListCell   *lc;

	foreach(lc, parse->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		if (rte->rtekind == RTE_RELATION && IsShardedTable(rte->relid))
			return true;
	}

	return false;
}

/*
 * ExtractShardRoute
 *		Analyze a query to determine routing.
 *
 * Walks the WHERE clause looking for shard_key = <const> patterns.
 * If found, computes the target shard and looks up the leader node.
 */
ShardRouteInfo *
ExtractShardRoute(Query *parse)
{
	ShardRouteInfo *info;
	ListCell   *lc;
	Oid			dist_table_oid = InvalidOid;
	ShardedTableInfo *table_info;
	AttrNumber	shard_key_attnum;
	ShardKeyExtractContext ctx;

	info = palloc0(sizeof(ShardRouteInfo));
	info->route_type = ROUTE_NONE;

	/* Find the first distributed table in the query */
	foreach(lc, parse->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		if (rte->rtekind == RTE_RELATION && IsShardedTable(rte->relid))
		{
			dist_table_oid = rte->relid;
			break;
		}
	}

	if (!OidIsValid(dist_table_oid))
		return info;

	info->table_oid = dist_table_oid;

	/* Get sharded table metadata */
	table_info = GetShardedTableInfo(dist_table_oid);
	if (table_info == NULL)
		return info;

	/* Get the shard key attribute number */
	shard_key_attnum = linitial_int(table_info->shard_key_attnums);

	/* Try to extract shard key value from WHERE clause */
	ctx.table_oid = dist_table_oid;
	ctx.shard_key_attnum = shard_key_attnum;
	ctx.found = false;
	ctx.shard_key_value = (Datum) 0;
	ctx.shard_key_type = get_atttype(dist_table_oid, shard_key_attnum);

	if (parse->jointree != NULL && parse->jointree->quals != NULL)
		extract_shard_key_walker(parse->jointree->quals, &ctx);

	/* Also check for INSERT ... VALUES */
	if (parse->commandType == CMD_INSERT)
	{
		ListCell   *tlc;

		foreach(tlc, parse->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(tlc);

			if (tle->resno == shard_key_attnum && IsA(tle->expr, Const))
			{
				Const	   *c = (Const *) tle->expr;

				if (!c->constisnull)
				{
					ctx.found = true;
					ctx.shard_key_value = c->constvalue;
					ctx.shard_key_type = c->consttype;
				}
				break;
			}
		}
	}

	if (ctx.found)
	{
		/* Single-shard query */
		ShardMapEntry *shard_entry;
		PlacementInfo *leader;

		shard_entry = GetShardForKey(dist_table_oid, ctx.shard_key_value);
		if (shard_entry == NULL)
		{
			FreeShardedTableInfo(table_info);
			return info;
		}

		info->is_single_shard = true;
		info->shard_id = shard_entry->shard_id;

		/* Find the leader placement */
		leader = GetLeaderPlacement(shard_entry->shard_id);
		if (leader != NULL)
		{
			strlcpy(info->leader_node, leader->nodename, NAMEDATALEN);

			if (IsLocalNode(leader->nodename))
				info->route_type = ROUTE_LOCAL_LEADER;
			else
				info->route_type = ROUTE_REMOTE_FORWARD;

			FreePlacementInfo(leader);
		}

		FreeShardMapEntry(shard_entry);
	}
	else
	{
		/* Multi-shard query (scatter-gather) */
		List	   *all_shards;
		int			num_shards;
		int			i;

		all_shards = GetAllShardsForTable(dist_table_oid);
		num_shards = list_length(all_shards);

		if (num_shards > 0)
		{
			info->route_type = ROUTE_SCATTER_GATHER;
			info->is_single_shard = false;
			info->num_shards = num_shards;
			info->shard_ids = palloc(num_shards * sizeof(int32));
			info->leader_nodes = palloc(num_shards * sizeof(char *));

			i = 0;
			foreach(lc, all_shards)
			{
				ShardMapEntry *entry = (ShardMapEntry *) lfirst(lc);
				PlacementInfo *leader;

				info->shard_ids[i] = entry->shard_id;

				leader = GetLeaderPlacement(entry->shard_id);
				if (leader != NULL)
				{
					info->leader_nodes[i] = pstrdup(leader->nodename);
					FreePlacementInfo(leader);
				}
				else
				{
					info->leader_nodes[i] = pstrdup("");
				}

				FreeShardMapEntry(entry);
				i++;
			}
		}
	}

	FreeShardedTableInfo(table_info);

	return info;
}

/*
 * extract_shard_key_walker
 *		Walk expression tree looking for shard_key = <const>.
 */
static bool
extract_shard_key_walker(Node *node, ShardKeyExtractContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, OpExpr))
	{
		OpExpr	   *op = (OpExpr *) node;
		Node	   *left;
		Node	   *right;

		/* Look for equality operator with exactly 2 args */
		if (list_length(op->args) != 2)
			return false;

		/* Check if this is an equality operator */
		if (!op_mergejoinable(op->opno, context->shard_key_type))
		{
			/* Try simpler check — look for = operator */
			char	   *opname = get_opname(op->opno);

			if (opname == NULL || strcmp(opname, "=") != 0)
				return expression_tree_walker(node,
											  extract_shard_key_walker,
											  context);
		}

		left = (Node *) linitial(op->args);
		right = (Node *) lsecond(op->args);

		/* Check for Var = Const pattern */
		if (IsA(left, Var) && IsA(right, Const))
		{
			Var		   *var = (Var *) left;
			Const	   *c = (Const *) right;

			if (var->varattno == context->shard_key_attnum &&
				!c->constisnull)
			{
				context->found = true;
				context->shard_key_value = c->constvalue;
				context->shard_key_type = c->consttype;
				return true;
			}
		}

		/* Check for Const = Var pattern */
		if (IsA(left, Const) && IsA(right, Var))
		{
			Const	   *c = (Const *) left;
			Var		   *var = (Var *) right;

			if (var->varattno == context->shard_key_attnum &&
				!c->constisnull)
			{
				context->found = true;
				context->shard_key_value = c->constvalue;
				context->shard_key_type = c->consttype;
				return true;
			}
		}
	}

	return expression_tree_walker(node, extract_shard_key_walker, context);
}

/*
 * FreeShardRouteInfo
 *		Free a ShardRouteInfo structure.
 */
void
FreeShardRouteInfo(ShardRouteInfo *info)
{
	if (info == NULL)
		return;

	if (info->shard_ids)
		pfree(info->shard_ids);

	if (info->leader_nodes)
	{
		for (int i = 0; i < info->num_shards; i++)
		{
			if (info->leader_nodes[i])
				pfree(info->leader_nodes[i]);
		}
		pfree(info->leader_nodes);
	}

	pfree(info);
}
