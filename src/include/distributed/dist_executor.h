/*-------------------------------------------------------------------------
 *
 * dist_executor.h
 *	  Distributed executor state and hooks
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/include/distributed/dist_executor.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DIST_EXECUTOR_H
#define DIST_EXECUTOR_H

#include "distributed/query_router.h"
#include "executor/executor.h"
#include "tcop/utility.h"

/*
 * DistExecutorState — per-query distributed execution state
 */
typedef struct DistExecutorState
{
	ShardRouteInfo *route_info;
	bool		is_distributed_write;
	bool		raft_proposed;
	bool		completed;
} DistExecutorState;

/* Executor hooks */
extern void DistExecutorStartHook(QueryDesc *queryDesc, int eflags);
extern void DistExecutorRunHook(QueryDesc *queryDesc,
								ScanDirection direction,
								uint64 count);
extern void DistExecutorFinishHook(QueryDesc *queryDesc);
extern void DistExecutorEndHook(QueryDesc *queryDesc);

/* Remote query forwarding */
extern void ForwardQueryToRemote(const char *node_name,
								 const char *query_string,
								 DestReceiver *dest);

/* Scatter-gather execution */
extern void ExecuteScatterGather(ShardRouteInfo *route_info,
								 const char *query_string,
								 DestReceiver *dest);

/* ProcessUtility hook for DDL */
extern void DistProcessUtilityHook(PlannedStmt *pstmt,
								   const char *queryString,
								   bool readOnlyTree,
								   ProcessUtilityContext context,
								   ParamListInfo params,
								   QueryEnvironment *queryEnv,
								   DestReceiver *dest,
								   QueryCompletion *qc);

#endif							/* DIST_EXECUTOR_H */
