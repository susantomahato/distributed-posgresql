/*-------------------------------------------------------------------------
 *
 * raft_worker.c
 *	  Background worker for Raft consensus tick loop
 *
 * This worker runs continuously after recovery is finished, ticking
 * all active Raft groups on this node every raft_tick_interval_ms.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/distributed/raft_worker.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distributed/dist_guc.h"
#include "distributed/dist_node.h"
#include "distributed/dist_shmem.h"
#include "distributed/dist_worker.h"
#include "distributed/raft.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/snapmgr.h"

/*
 * RegisterDistributedWorkers
 *		Register the Raft ticker and failure detector background workers.
 */
void
RegisterDistributedWorkers(void)
{
	BackgroundWorker worker;

	if (!dist_enabled)
		return;

	/* Register Raft ticker worker */
	memset(&worker, 0, sizeof(BackgroundWorker));
	snprintf(worker.bgw_name, BGW_MAXLEN, "distributed raft ticker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "distributed raft ticker");
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 5;	/* restart after 5 seconds */
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "postgres");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "RaftWorkerMain");
	worker.bgw_main_arg = Int32GetDatum(0);
	worker.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&worker);

	/* Register failure detector worker */
	memset(&worker, 0, sizeof(BackgroundWorker));
	snprintf(worker.bgw_name, BGW_MAXLEN, "distributed failure detector");
	snprintf(worker.bgw_type, BGW_MAXLEN, "distributed failure detector");
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 10;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "postgres");
	snprintf(worker.bgw_function_name, BGW_MAXLEN,
			 "FailureDetectorWorkerMain");
	worker.bgw_main_arg = Int32GetDatum(0);
	worker.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&worker);

	elog(LOG, "distributed: registered background workers");
}

/*
 * RaftWorkerMain
 *		Main loop for the Raft ticker background worker.
 *
 * Ticks all active Raft groups every raft_tick_interval_ms (default 50ms).
 */
void
RaftWorkerMain(Datum main_arg)
{
	elog(LOG, "distributed: raft worker starting");

	/* Set up signal handlers */
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	BackgroundWorkerUnblockSignals();

	/* Connect to a database for SPI access */
	BackgroundWorkerInitializeConnection("postgres", NULL, 0);

	/* Main tick loop */
	while (!ShutdownRequestPending)
	{
		int			rc;

		CHECK_FOR_INTERRUPTS();

		/* Wait for tick interval */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   dist_raft_tick_interval_ms,
					   PG_WAIT_EXTENSION);

		ResetLatch(MyLatch);

		if (rc & WL_POSTMASTER_DEATH)
			break;

		/* Skip if shared memory not initialized */
		if (DistShmem == NULL || !DistShmem->initialized)
			continue;

		/* Tick all active Raft groups on this node */
		for (int i = 0; i < MAX_RAFT_GROUPS; i++)
		{
			RaftGroupState *group = &DistShmem->raft_groups[i];

			if (!group->in_use)
				continue;

			/* Only tick groups where this node is a member */
			{
				bool		is_member = false;

				for (int j = 0; j < group->num_peers; j++)
				{
					if (IsLocalNode(group->peer_names[j]))
					{
						is_member = true;
						break;
					}
				}

				if (!is_member)
					continue;
			}

			PG_TRY();
			{
				RaftGroupTick(group);
			}
			PG_CATCH();
			{
				elog(WARNING, "distributed: error ticking raft group %d",
					 group->raft_group_id);
				FlushErrorState();
			}
			PG_END_TRY();
		}
	}

	elog(LOG, "distributed: raft worker shutting down");
	proc_exit(0);
}
