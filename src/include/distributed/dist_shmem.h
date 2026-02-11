/*-------------------------------------------------------------------------
 *
 * dist_shmem.h
 *	  Shared memory structures for the distributed subsystem
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/include/distributed/dist_shmem.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DIST_SHMEM_H
#define DIST_SHMEM_H

#include "distributed/raft.h"
#include "storage/lwlock.h"
#include "storage/s_lock.h"
#include "utils/timestamp.h"

/*
 * Maximum number of Raft groups tracked in shared memory.
 * Each shard has one Raft group.
 */
#define MAX_RAFT_GROUPS		1024

/*
 * Maximum number of nodes in the cluster
 */
#define MAX_DIST_NODES		64

/*
 * Maximum number of peers per Raft group
 */
#define MAX_RAFT_PEERS		5

/*
 * NodeHealthState — tracks heartbeat state for each node
 */
typedef struct NodeHealthState
{
	NameData	node_name;
	TimestampTz last_heartbeat;
	int			consecutive_failures;
	bool		is_marked_failed;
	bool		in_use;
} NodeHealthState;

/*
 * DistributedShmemState — top-level shared memory structure
 */
typedef struct DistributedShmemState
{
	LWLock		lock;				/* protects global state updates */
	bool		initialized;		/* true after first initialization */
	int			num_raft_groups;	/* active Raft groups */
	int			num_nodes;			/* registered nodes */
	RaftGroupState raft_groups[MAX_RAFT_GROUPS];
	NodeHealthState node_health[MAX_DIST_NODES];
} DistributedShmemState;

/* Global pointer to shared memory state */
extern PGDLLIMPORT DistributedShmemState *DistShmem;

/* Initialization functions */
extern Size DistributedShmemSize(void);
extern void DistributedShmemInit(void);

/* Shared memory hooks */
extern void DistShmemRequestHook(void);
extern void DistShmemStartupHook(void);

/* Raft group management */
extern RaftGroupState *DistShmemGetRaftGroup(int raft_group_id);
extern RaftGroupState *DistShmemAllocRaftGroup(int raft_group_id, int shard_id);
extern void DistShmemRemoveRaftGroup(int raft_group_id);

/* Node health management */
extern NodeHealthState *DistShmemGetNodeHealth(const char *node_name);
extern void DistShmemUpdateNodeHealth(const char *node_name,
									  TimestampTz heartbeat_time);

#endif							/* DIST_SHMEM_H */
