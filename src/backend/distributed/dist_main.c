/*-------------------------------------------------------------------------
 *
 * dist_main.c
 *	  Module initialization and hook registration for the distributed
 *	  subsystem.
 *
 * DistributedInit() is called during postmaster startup. It:
 *   1. Loads GUC parameters
 *   2. Installs planner, executor, and utility hooks
 *   3. Registers shared memory request/startup hooks
 *   4. Registers background workers (Raft ticker, failure detector)
 *   5. Registers XactCallback for connection cleanup
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/distributed/dist_main.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distributed/dist_connection.h"
#include "distributed/dist_executor.h"
#include "distributed/dist_guc.h"
#include "distributed/dist_shmem.h"
#include "distributed/dist_worker.h"
#include "distributed/query_router.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "optimizer/planner.h"
#include "storage/ipc.h"
#include "tcop/utility.h"
#include "access/xact.h"

/* Saved hook values */
static planner_hook_type prev_planner_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart_hook = NULL;
static ExecutorRun_hook_type prev_ExecutorRun_hook = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;
static ProcessUtility_hook_type prev_ProcessUtility_hook = NULL;
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* Forward declarations for wrapper hooks */
static PlannedStmt *dist_planner_wrapper(Query *parse,
										 const char *query_string,
										 int cursorOptions,
										 ParamListInfo boundParams,
										 ExplainState *es);
static void dist_executor_start_wrapper(QueryDesc *queryDesc, int eflags);
static void dist_executor_run_wrapper(QueryDesc *queryDesc,
									  ScanDirection direction,
									  uint64 count);
static void dist_executor_finish_wrapper(QueryDesc *queryDesc);
static void dist_executor_end_wrapper(QueryDesc *queryDesc);
static void dist_process_utility_wrapper(PlannedStmt *pstmt,
										 const char *queryString,
										 bool readOnlyTree,
										 ProcessUtilityContext context,
										 ParamListInfo params,
										 QueryEnvironment *queryEnv,
										 DestReceiver *dest,
										 QueryCompletion *qc);
static void dist_shmem_request_wrapper(void);
static void dist_shmem_startup_wrapper(void);

/*
 * DistributedInit
 *		Initialize the distributed subsystem.
 *
 * Called during postmaster startup, after shared_preload_libraries
 * processing and before accepting connections.
 */
void
DistributedInit(void)
{
	/* Register GUC parameters first */
	DistributedGucInit();

	if (!dist_enabled)
		return;

	elog(LOG, "distributed: initializing distributed subsystem");

	/* Chain shared memory hooks */
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = dist_shmem_request_wrapper;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = dist_shmem_startup_wrapper;

	/* Chain planner hook */
	prev_planner_hook = planner_hook;
	planner_hook = dist_planner_wrapper;

	/* Chain executor hooks */
	prev_ExecutorStart_hook = ExecutorStart_hook;
	ExecutorStart_hook = dist_executor_start_wrapper;

	prev_ExecutorRun_hook = ExecutorRun_hook;
	ExecutorRun_hook = dist_executor_run_wrapper;

	prev_ExecutorFinish_hook = ExecutorFinish_hook;
	ExecutorFinish_hook = dist_executor_finish_wrapper;

	prev_ExecutorEnd_hook = ExecutorEnd_hook;
	ExecutorEnd_hook = dist_executor_end_wrapper;

	/* Chain ProcessUtility hook */
	prev_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = dist_process_utility_wrapper;

	/* Register transaction callback for connection cleanup */
	RegisterXactCallback(DistConnectionXactCallback, NULL);

	/* Register background workers */
	RegisterDistributedWorkers();

	elog(LOG, "distributed: initialization complete, node='%s'",
		 dist_node_name ? dist_node_name : "(unset)");
}

/*
 * Hook wrappers — each checks dist_enabled, then delegates or chains.
 */

static PlannedStmt *
dist_planner_wrapper(Query *parse,
					 const char *query_string,
					 int cursorOptions,
					 ParamListInfo boundParams,
					 ExplainState *es)
{
	if (dist_enabled)
	{
		PlannedStmt *result;

		result = DistPlannerHook(parse, query_string, cursorOptions,
								boundParams, es);
		if (result != NULL)
			return result;
	}

	/* Fall through to previous hook or standard planner */
	if (prev_planner_hook)
		return prev_planner_hook(parse, query_string, cursorOptions,
								boundParams, es);
	return standard_planner(parse, query_string, cursorOptions,
							boundParams, es);
}

static void
dist_executor_start_wrapper(QueryDesc *queryDesc, int eflags)
{
	if (dist_enabled)
		DistExecutorStartHook(queryDesc, eflags);

	if (prev_ExecutorStart_hook)
		prev_ExecutorStart_hook(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
}

static void
dist_executor_run_wrapper(QueryDesc *queryDesc,
						  ScanDirection direction,
						  uint64 count)
{
	if (dist_enabled)
	{
		DistExecutorRunHook(queryDesc, direction, count);
		/* If the hook handled the query completely, don't run standard */
		return;
	}

	if (prev_ExecutorRun_hook)
		prev_ExecutorRun_hook(queryDesc, direction, count);
	else
		standard_ExecutorRun(queryDesc, direction, count);
}

static void
dist_executor_finish_wrapper(QueryDesc *queryDesc)
{
	if (dist_enabled)
		DistExecutorFinishHook(queryDesc);

	if (prev_ExecutorFinish_hook)
		prev_ExecutorFinish_hook(queryDesc);
	else
		standard_ExecutorFinish(queryDesc);
}

static void
dist_executor_end_wrapper(QueryDesc *queryDesc)
{
	if (dist_enabled)
		DistExecutorEndHook(queryDesc);

	if (prev_ExecutorEnd_hook)
		prev_ExecutorEnd_hook(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

static void
dist_process_utility_wrapper(PlannedStmt *pstmt,
							 const char *queryString,
							 bool readOnlyTree,
							 ProcessUtilityContext context,
							 ParamListInfo params,
							 QueryEnvironment *queryEnv,
							 DestReceiver *dest,
							 QueryCompletion *qc)
{
	if (dist_enabled)
	{
		DistProcessUtilityHook(pstmt, queryString, readOnlyTree,
							   context, params, queryEnv, dest, qc);
		return;
	}

	if (prev_ProcessUtility_hook)
		prev_ProcessUtility_hook(pstmt, queryString, readOnlyTree,
								context, params, queryEnv, dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString, readOnlyTree,
								context, params, queryEnv, dest, qc);
}

static void
dist_shmem_request_wrapper(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	DistShmemRequestHook();
}

static void
dist_shmem_startup_wrapper(void)
{
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	DistShmemStartupHook();
}
