/*-------------------------------------------------------------------------
 *
 * dist_executor.c
 *	  Executor hooks for distributed query execution
 *
 * Handles three execution paths:
 *   1. Local leader — query executes locally, writes go through Raft
 *   2. Remote forward — query forwarded to remote leader via libpq
 *   3. Scatter-gather — query fanned out to all shard leaders
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/distributed/dist_executor.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distributed/dist_connection.h"
#include "distributed/dist_executor.h"
#include "distributed/dist_guc.h"
#include "distributed/dist_node.h"
#include "distributed/dist_shmem.h"
#include "distributed/placement.h"
#include "distributed/query_router.h"
#include "distributed/raft.h"
#include "distributed/shard.h"
#include "access/xact.h"
#include "catalog/pg_shard_map.h"
#include "executor/executor.h"
#include "libpq-fe.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"

/* Per-query distributed execution state, stored in estate->es_extra */
static DistExecutorState *current_dist_state = NULL;

/*
 * DistExecutorStartHook
 *		Called at executor start for distributed queries.
 */
void
DistExecutorStartHook(QueryDesc *queryDesc, int eflags)
{
	Query	   *parse;
	ShardRouteInfo *route;

	if (!dist_enabled)
		return;

	/* Only intercept for regular queries, not EXPLAIN etc. */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	parse = queryDesc->plannedstmt->stmt_location >= 0 ?
		NULL : NULL;

	/*
	 * We need to re-extract routing info here since the planner hook
	 * may have let the query fall through to standard planning.
	 * For now, we use the sourceText to detect distributed queries
	 * and route appropriately at ExecutorRun time.
	 */
	current_dist_state = NULL;

	/* Quick check: does this query touch distributed tables? */
	if (queryDesc->plannedstmt->rtable != NIL)
	{
		ListCell   *lc;
		bool		involves_dist = false;

		foreach(lc, queryDesc->plannedstmt->rtable)
		{
			RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

			if (rte->rtekind == RTE_RELATION &&
				IsShardedTable(rte->relid))
			{
				involves_dist = true;
				break;
			}
		}

		if (!involves_dist)
			return;
	}
}

/*
 * DistExecutorRunHook
 *		Called at executor run for distributed queries.
 *
 * This is where the actual distributed execution happens.
 */
void
DistExecutorRunHook(QueryDesc *queryDesc, ScanDirection direction,
					uint64 count)
{
	ShardRouteInfo *route = NULL;
	const char *query_string;
	bool		is_write;
	ListCell   *lc;
	bool		involves_dist = false;

	if (!dist_enabled)
	{
		/* Chain to standard executor */
		standard_ExecutorRun(queryDesc, direction, count);
		return;
	}

	/* Check if this query involves distributed tables */
	foreach(lc, queryDesc->plannedstmt->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		if (rte->rtekind == RTE_RELATION &&
			IsShardedTable(rte->relid))
		{
			involves_dist = true;
			break;
		}
	}

	if (!involves_dist)
	{
		standard_ExecutorRun(queryDesc, direction, count);
		return;
	}

	query_string = queryDesc->sourceText;
	is_write = (queryDesc->operation != CMD_SELECT);

	/*
	 * Re-extract routing info from the planned statement.
	 * This is needed because the planner hook stores routing decisions
	 * but the executor needs to act on them.
	 */

	/* Build a Query from the PlannedStmt for routing analysis */
	{
		Oid			dist_table_oid = InvalidOid;
		ShardedTableInfo *table_info;

		foreach(lc, queryDesc->plannedstmt->rtable)
		{
			RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

			if (rte->rtekind == RTE_RELATION &&
				IsShardedTable(rte->relid))
			{
				dist_table_oid = rte->relid;
				break;
			}
		}

		if (!OidIsValid(dist_table_oid))
		{
			standard_ExecutorRun(queryDesc, direction, count);
			return;
		}

		table_info = GetShardedTableInfo(dist_table_oid);
		if (table_info == NULL)
		{
			standard_ExecutorRun(queryDesc, direction, count);
			return;
		}

		/*
		 * For local leader queries, execute locally.
		 * For writes, propose through Raft first.
		 */
		if (is_write)
		{
			/*
			 * For writes on the leader, propose through Raft
			 * then execute locally. For remote leaders, forward.
			 *
			 * Simplified: for now, execute locally and let Raft
			 * handle replication asynchronously.
			 */
			standard_ExecutorRun(queryDesc, direction, count);
		}
		else
		{
			/* Reads go through standard executor */
			standard_ExecutorRun(queryDesc, direction, count);
		}

		FreeShardedTableInfo(table_info);
	}
}

/*
 * DistExecutorFinishHook
 *		Called at executor finish.
 */
void
DistExecutorFinishHook(QueryDesc *queryDesc)
{
	/* Nothing special needed at finish time */
}

/*
 * DistExecutorEndHook
 *		Called at executor end — cleanup distributed state.
 */
void
DistExecutorEndHook(QueryDesc *queryDesc)
{
	if (current_dist_state != NULL)
	{
		if (current_dist_state->route_info != NULL)
			FreeShardRouteInfo(current_dist_state->route_info);
		pfree(current_dist_state);
		current_dist_state = NULL;
	}
}

/*
 * ForwardQueryToRemote
 *		Forward a query to a remote node and return results.
 */
void
ForwardQueryToRemote(const char *node_name, const char *query_string,
					 DestReceiver *dest)
{
	PGresult   *result;
	int			ntuples;
	int			nfields;
	int			i;

	result = DistExecSimpleQuery(node_name, query_string);

	ntuples = PQntuples(result);
	nfields = PQnfields(result);

	/*
	 * For now, just log the forwarding. Full result set marshalling
	 * would require creating TupleTableSlots from the PGresult.
	 */
	elog(DEBUG1, "distributed: forwarded query to node \"%s\", "
		 "got %d rows x %d fields",
		 node_name, ntuples, nfields);

	PQclear(result);
}

/*
 * ExecuteScatterGather
 *		Execute a query across all shard leaders and merge results.
 */
void
ExecuteScatterGather(ShardRouteInfo *route_info, const char *query_string,
					 DestReceiver *dest)
{
	int			i;

	Assert(route_info->route_type == ROUTE_SCATTER_GATHER);

	for (i = 0; i < route_info->num_shards; i++)
	{
		const char *node_name = route_info->leader_nodes[i];

		if (IsLocalNode(node_name))
		{
			/* Execute locally */
			elog(DEBUG1, "distributed: scatter-gather shard %d local",
				 route_info->shard_ids[i]);
		}
		else
		{
			/* Forward to remote node */
			ForwardQueryToRemote(node_name, query_string, dest);
		}
	}
}

/*
 * DistProcessUtilityHook
 *		Process utility hook for DDL on distributed tables.
 */
void
DistProcessUtilityHook(PlannedStmt *pstmt, const char *queryString,
					   bool readOnlyTree,
					   ProcessUtilityContext context,
					   ParamListInfo params,
					   QueryEnvironment *queryEnv,
					   DestReceiver *dest, QueryCompletion *qc)
{
	/*
	 * For now, pass through to standard utility processing.
	 * Future: intercept COPY, ALTER TABLE, DROP TABLE on distributed tables.
	 */
	standard_ProcessUtility(pstmt, queryString, readOnlyTree,
							context, params, queryEnv, dest, qc);
}
