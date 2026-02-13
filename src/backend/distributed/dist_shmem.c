/*-------------------------------------------------------------------------
 *
 * dist_shmem.c
 *	  Shared memory management for the distributed subsystem
 *
 * Allocates and initializes a DistributedShmemState structure in shared
 * memory, containing RaftGroupState slots and NodeHealthState slots.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/distributed/dist_shmem.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distributed/dist_guc.h"
#include "distributed/dist_shmem.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/memutils.h"

/* Global pointer */
DistributedShmemState *DistShmem = NULL;

/* Tranche IDs for our LWLocks */
static int	dist_shmem_tranche_id = 0;
static int	raft_group_tranche_id = 0;

/*
 * DistributedShmemSize
 *		Calculate shared memory needed for the distributed subsystem.
 */
Size
DistributedShmemSize(void)
{
	return MAXALIGN(sizeof(DistributedShmemState));
}

/*
 * DistShmemRequestHook
 *		Request shared memory space during postmaster startup.
 */
void
DistShmemRequestHook(void)
{
	if (dist_enabled)
		RequestAddinShmemSpace(DistributedShmemSize());
}

/*
 * DistShmemStartupHook
 *		Initialize shared memory structures.
 */
void
DistShmemStartupHook(void)
{
	bool		found;

	if (!dist_enabled)
		return;

	/* Allocate and register our LWLock tranches */
	if (dist_shmem_tranche_id == 0)
		dist_shmem_tranche_id = LWLockNewTrancheId("DistributedShmemLock");
	if (raft_group_tranche_id == 0)
		raft_group_tranche_id = LWLockNewTrancheId("RaftGroupLock");

	DistShmem = (DistributedShmemState *)
		ShmemInitStruct("Distributed Shmem State",
						DistributedShmemSize(),
						&found);

	if (!found)
	{
		/* First time — initialize everything */
		memset(DistShmem, 0, sizeof(DistributedShmemState));
		LWLockInitialize(&DistShmem->lock, dist_shmem_tranche_id);
		DistShmem->initialized = true;
		DistShmem->num_raft_groups = 0;
		DistShmem->num_nodes = 0;

		/* Initialize all Raft group slots */
		for (int i = 0; i < MAX_RAFT_GROUPS; i++)
		{
			DistShmem->raft_groups[i].in_use = false;
			SpinLockInit(&DistShmem->raft_groups[i].mutex);
			LWLockInitialize(&DistShmem->raft_groups[i].raft_lock,
							 raft_group_tranche_id);
		}

		/* Initialize node health slots */
		for (int i = 0; i < MAX_DIST_NODES; i++)
		{
			DistShmem->node_health[i].in_use = false;
			DistShmem->node_health[i].consecutive_failures = 0;
			DistShmem->node_health[i].is_marked_failed = false;
		}

		elog(LOG, "distributed: shared memory initialized (%zu bytes)",
			 DistributedShmemSize());
	}
}

/*
 * DistributedShmemInit
 *		Full initialization (called from DistributedInit).
 */
void
DistributedShmemInit(void)
{
	/* The actual work is done in the shmem hooks */
}

/*
 * DistShmemGetRaftGroup
 *		Find a Raft group by ID in shared memory.
 *
 * Returns NULL if not found.
 */
RaftGroupState *
DistShmemGetRaftGroup(int raft_group_id)
{
	if (DistShmem == NULL)
		return NULL;

	for (int i = 0; i < MAX_RAFT_GROUPS; i++)
	{
		if (DistShmem->raft_groups[i].in_use &&
			DistShmem->raft_groups[i].raft_group_id == raft_group_id)
			return &DistShmem->raft_groups[i];
	}

	return NULL;
}

/*
 * DistShmemAllocRaftGroup
 *		Allocate and initialize a new Raft group slot.
 *
 * Returns NULL if no slots available.
 */
RaftGroupState *
DistShmemAllocRaftGroup(int raft_group_id, int shard_id)
{
	if (DistShmem == NULL)
		return NULL;

	LWLockAcquire(&DistShmem->lock, LW_EXCLUSIVE);

	for (int i = 0; i < MAX_RAFT_GROUPS; i++)
	{
		if (!DistShmem->raft_groups[i].in_use)
		{
			RaftGroupState *group = &DistShmem->raft_groups[i];

			memset(group, 0, sizeof(RaftGroupState));
			group->in_use = true;
			group->raft_group_id = raft_group_id;
			group->shard_id = shard_id;
			group->role = RAFT_ROLE_FOLLOWER;
			group->current_term = 0;
			group->commit_index = 0;
			group->last_applied = 0;
			group->last_log_index = 0;
			group->last_log_term = 0;
			group->num_peers = 0;
			group->last_heartbeat = GetCurrentTimestamp();
			SpinLockInit(&group->mutex);
			LWLockInitialize(&group->raft_lock,
							 raft_group_tranche_id);

			DistShmem->num_raft_groups++;

			LWLockRelease(&DistShmem->lock);
			return group;
		}
	}

	LWLockRelease(&DistShmem->lock);

	ereport(WARNING,
			(errmsg("distributed: no free Raft group slots (max %d)",
					MAX_RAFT_GROUPS)));
	return NULL;
}

/*
 * DistShmemRemoveRaftGroup
 *		Mark a Raft group slot as free.
 */
void
DistShmemRemoveRaftGroup(int raft_group_id)
{
	if (DistShmem == NULL)
		return;

	LWLockAcquire(&DistShmem->lock, LW_EXCLUSIVE);

	for (int i = 0; i < MAX_RAFT_GROUPS; i++)
	{
		if (DistShmem->raft_groups[i].in_use &&
			DistShmem->raft_groups[i].raft_group_id == raft_group_id)
		{
			DistShmem->raft_groups[i].in_use = false;
			DistShmem->num_raft_groups--;
			break;
		}
	}

	LWLockRelease(&DistShmem->lock);
}

/*
 * DistShmemGetNodeHealth
 *		Find node health state by name.
 */
NodeHealthState *
DistShmemGetNodeHealth(const char *node_name)
{
	if (DistShmem == NULL)
		return NULL;

	for (int i = 0; i < MAX_DIST_NODES; i++)
	{
		if (DistShmem->node_health[i].in_use &&
			strcmp(NameStr(DistShmem->node_health[i].node_name),
				   node_name) == 0)
			return &DistShmem->node_health[i];
	}

	return NULL;
}

/*
 * DistShmemUpdateNodeHealth
 *		Update the last heartbeat time for a node.
 *		Creates a new entry if the node is not yet tracked.
 */
void
DistShmemUpdateNodeHealth(const char *node_name, TimestampTz heartbeat_time)
{
	NodeHealthState *health;

	if (DistShmem == NULL)
		return;

	LWLockAcquire(&DistShmem->lock, LW_EXCLUSIVE);

	/* Try to find existing entry */
	health = NULL;
	for (int i = 0; i < MAX_DIST_NODES; i++)
	{
		if (DistShmem->node_health[i].in_use &&
			strcmp(NameStr(DistShmem->node_health[i].node_name),
				   node_name) == 0)
		{
			health = &DistShmem->node_health[i];
			break;
		}
	}

	/* If not found, allocate a new slot */
	if (health == NULL)
	{
		for (int i = 0; i < MAX_DIST_NODES; i++)
		{
			if (!DistShmem->node_health[i].in_use)
			{
				health = &DistShmem->node_health[i];
				health->in_use = true;
				namestrcpy(&health->node_name, node_name);
				health->consecutive_failures = 0;
				health->is_marked_failed = false;
				DistShmem->num_nodes++;
				break;
			}
		}
	}

	if (health != NULL)
	{
		health->last_heartbeat = heartbeat_time;
		health->consecutive_failures = 0;
	}

	LWLockRelease(&DistShmem->lock);
}
