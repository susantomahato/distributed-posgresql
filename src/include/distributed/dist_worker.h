/*-------------------------------------------------------------------------
 *
 * dist_worker.h
 *	  Background worker registration for the distributed subsystem
 *
 * Two background workers are registered:
 *   1. Raft ticker — ticks all local Raft groups every 50ms
 *   2. Failure detector — heartbeats all nodes every 1s
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/include/distributed/dist_worker.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DIST_WORKER_H
#define DIST_WORKER_H

#include "postmaster/bgworker.h"

/* Background worker main entry points */
extern PGDLLEXPORT void RaftWorkerMain(Datum main_arg);
extern PGDLLEXPORT void FailureDetectorWorkerMain(Datum main_arg);
extern PGDLLEXPORT void ReReplicationWorkerMain(Datum main_arg);

/* Registration */
extern void RegisterDistributedWorkers(void);

#endif							/* DIST_WORKER_H */
