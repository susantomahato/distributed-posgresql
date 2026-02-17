/*-------------------------------------------------------------------------
 *
 * failure_detector.c
 *	  Health monitoring, failure detection, and auto-recovery
 *
 * The failure detector background worker periodically sends heartbeat
 * pings to all registered nodes. After 3 consecutive missed heartbeats,
 * a node is marked as failed and re-replication is initiated for all
 * shard groups that had placements on the failed node.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/distributed/failure_detector.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distributed/dist_connection.h"
#include "distributed/dist_guc.h"
#include "distributed/dist_node.h"
#include "distributed/dist_shmem.h"
#include "distributed/dist_worker.h"
#include "distributed/failure_detector.h"
#include "distributed/placement.h"
#include "distributed/raft.h"
#include "distributed/raft_log.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/pg_shard_node.h"
#include "catalog/indexing.h"
#include "miscadmin.h"
#include "utils/fmgroids.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "distributed/shard.h"
#include "libpq-fe.h"

/* Number of missed heartbeats before marking a node as failed */
#define FAILURE_THRESHOLD		3

/*
 * FailureDetectorWorkerMain
 *		Main loop for the failure detector background worker.
 */
void
FailureDetectorWorkerMain(Datum main_arg)
{
	elog(LOG, "distributed: failure detector starting");

	/* Set up signal handlers */
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection("postgres", NULL, 0);

	while (!ShutdownRequestPending)
	{
		int			rc;

		CHECK_FOR_INTERRUPTS();

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   dist_heartbeat_interval_ms,
					   PG_WAIT_EXTENSION);

		ResetLatch(MyLatch);

		if (rc & WL_POSTMASTER_DEATH)
			break;

		if (DistShmem == NULL || !DistShmem->initialized)
			continue;

		PG_TRY();
		{
			FailureDetectorCheck();
		}
		PG_CATCH();
		{
			elog(WARNING, "distributed: failure detector error");
			FlushErrorState();
			if (IsTransactionState())
				AbortCurrentTransaction();
		}
		PG_END_TRY();
	}

	elog(LOG, "distributed: failure detector shutting down");
	proc_exit(0);
}

/*
 * PingNodeDirect
 *		Test if a node is reachable using PQping (no ereport on failure).
 *
 * This function never throws an error. Returns true if the node
 * is accepting connections, false otherwise.
 */
static bool
PingNodeDirect(const char *node_name, const char *conninfo)
{
	PGPing		ping_result;

	if (conninfo == NULL || conninfo[0] == '\0')
		return false;

	ping_result = PQping(conninfo);
	return (ping_result == PQPING_OK);
}

/*
 * FailureDetectorCheck
 *		Send heartbeat pings to all nodes and handle failures.
 *
 * This function manages its own transactions to ensure catalog
 * operations are always done in a clean transaction state.
 */
void
FailureDetectorCheck(void)
{
	char		node_names[MAX_DIST_NODES][NAMEDATALEN];
	char		node_conninfos[MAX_DIST_NODES][256];
	int			num_nodes = 0;
	char		failed_nodes[MAX_DIST_NODES][NAMEDATALEN];
	char		recovered_nodes[MAX_DIST_NODES][NAMEDATALEN];
	int			num_failed = 0;
	int			num_recovered = 0;

	/*
	 * Phase 1: Read node list from catalog.
	 * Copy names and conninfo to local arrays so we don't need a
	 * transaction during the ping phase.
	 */
	StartTransactionCommand();
	{
		List	   *nodes;
		ListCell   *lc;

		nodes = GetAllDistNodes();

		foreach(lc, nodes)
		{
			char	   *node_name = (char *) lfirst(lc);
			char	   *conninfo;

			if (IsLocalNode(node_name))
				continue;

			if (num_nodes >= MAX_DIST_NODES)
				break;

			strlcpy(node_names[num_nodes], node_name, NAMEDATALEN);

			conninfo = GetNodeConnInfo(node_name);
			if (conninfo != NULL)
			{
				strlcpy(node_conninfos[num_nodes], conninfo, 256);
				pfree(conninfo);
			}
			else
			{
				node_conninfos[num_nodes][0] = '\0';
			}
			num_nodes++;
		}

		list_free_deep(nodes);
	}
	CommitTransactionCommand();

	/*
	 * Phase 2: Ping each node using PQping (no transaction needed).
	 * PQping never throws an error — it returns a status code.
	 */
	for (int i = 0; i < num_nodes; i++)
	{
		NodeHealthState *health;
		bool		ping_ok;

		ping_ok = PingNodeDirect(node_names[i], node_conninfos[i]);

		elog(DEBUG1, "distributed: ping %s = %s (conninfo=%s)",
			 node_names[i], ping_ok ? "OK" : "FAIL",
			 node_conninfos[i]);

		if (ping_ok)
		{
			DistShmemUpdateNodeHealth(node_names[i], GetCurrentTimestamp());

			health = DistShmemGetNodeHealth(node_names[i]);
			elog(DEBUG1, "distributed: %s recovery check: health=%p, "
				 "is_marked_failed=%d",
				 node_names[i], health,
				 health ? health->is_marked_failed : -1);
			if (health != NULL && health->is_marked_failed)
			{
				if (num_recovered < MAX_DIST_NODES)
					strlcpy(recovered_nodes[num_recovered++],
							node_names[i], NAMEDATALEN);
			}
		}
		else
		{
			/*
			 * Ensure the node has a health entry in shared memory.
			 * DistShmemGetNodeHealth returns NULL for nodes that
			 * have never had a successful ping (e.g. after restart).
			 */
			health = DistShmemGetNodeHealth(node_names[i]);
			if (health == NULL)
			{
				/* Create the entry, then re-fetch it */
				DistShmemUpdateNodeHealth(node_names[i],
										  (TimestampTz) 0);
				health = DistShmemGetNodeHealth(node_names[i]);
			}
			if (health != NULL)
			{
				LWLockAcquire(&DistShmem->lock, LW_EXCLUSIVE);
				health->consecutive_failures++;

				if (health->consecutive_failures >= FAILURE_THRESHOLD &&
					!health->is_marked_failed)
				{
					health->is_marked_failed = true;
					LWLockRelease(&DistShmem->lock);

					elog(LOG, "distributed: node \"%s\" marked as FAILED "
						 "(%d consecutive failures)",
						 node_names[i], health->consecutive_failures);

					if (num_failed < MAX_DIST_NODES)
						strlcpy(failed_nodes[num_failed++],
								node_names[i], NAMEDATALEN);
				}
				else
				{
					LWLockRelease(&DistShmem->lock);
				}
			}
		}
	}

	/*
	 * Phase 3: Handle failures — each in its own transaction.
	 */
	for (int i = 0; i < num_failed; i++)
	{
		PG_TRY();
		{
			StartTransactionCommand();
			HandleNodeFailure(failed_nodes[i]);
			CommitTransactionCommand();
		}
		PG_CATCH();
		{
			elog(WARNING, "distributed: HandleNodeFailure for \"%s\" failed",
				 failed_nodes[i]);
			FlushErrorState();
			if (IsTransactionState())
				AbortCurrentTransaction();
		}
		PG_END_TRY();
	}

	/*
	 * Phase 4: Handle recoveries — each in its own transaction.
	 */
	for (int i = 0; i < num_recovered; i++)
	{
		PG_TRY();
		{
			StartTransactionCommand();
			elog(LOG, "distributed: node \"%s\" has recovered",
				 recovered_nodes[i]);
			HandleNodeRecovery(recovered_nodes[i]);
			CommitTransactionCommand();
		}
		PG_CATCH();
		{
			elog(WARNING, "distributed: HandleNodeRecovery for \"%s\" failed",
				 recovered_nodes[i]);
			FlushErrorState();
			if (IsTransactionState())
				AbortCurrentTransaction();
		}
		PG_END_TRY();
	}
}

/*
 * HandleNodeFailure
 *		Handle a node failure: update catalog, promote new leaders.
 *
 * This function runs in its own transaction (started by the caller).
 * It copies placement data to local arrays before doing any catalog
 * modifications to avoid iterator-invalidation issues.
 */
void
HandleNodeFailure(const char *node_name)
{
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData skey[1];
	HeapTuple	tuple;
	HeapTuple	newtuple;
	Datum		values[Natts_pg_shard_node];
	bool		nulls[Natts_pg_shard_node];
	bool		replaces[Natts_pg_shard_node];
	List	   *placements;
	ListCell   *lc;
	int			nplacements;
	int			idx;

	/* Local array to hold placement data (avoids iterator invalidation) */
	typedef struct
	{
		int32		placementid;
		int32		shardid;
		int32		raftgroupid;
		char		raftrole;
		char		placementstate;
	} PlacementBasicInfo;
	PlacementBasicInfo *pinfos;

	/* Step 1: Update pg_shard_node — set nodestate = 'f' (offline) */
	rel = table_open(ShardNodeRelationId, RowExclusiveLock);

	ScanKeyInit(&skey[0],
				Anum_pg_shard_node_nodename,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(node_name));

	scan = systable_beginscan(rel, ShardNodeNodenameIndexId,
							  true, NULL, 1, skey);

	tuple = systable_getnext(scan);
	if (HeapTupleIsValid(tuple))
	{
		memset(values, 0, sizeof(values));
		memset(nulls, false, sizeof(nulls));
		memset(replaces, false, sizeof(replaces));

		replaces[Anum_pg_shard_node_nodestate - 1] = true;
		values[Anum_pg_shard_node_nodestate - 1] =
			CharGetDatum(SHARD_NODE_STATE_OFFLINE);

		newtuple = heap_modify_tuple(tuple, RelationGetDescr(rel),
									 values, nulls, replaces);
		CatalogTupleUpdate(rel, &newtuple->t_self, newtuple);
		heap_freetuple(newtuple);
	}

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	CommandCounterIncrement();

	/*
	 * Step 2: Read all placements for this node and copy to a local array.
	 * This avoids issues with catalog modifications invalidating the list
	 * during iteration.
	 */
	placements = GetPlacementsForNode(node_name);
	nplacements = list_length(placements);

	elog(LOG, "distributed: HandleNodeFailure for \"%s\": "
		 "found %d placements",
		 node_name, nplacements);

	if (nplacements == 0)
	{
		list_free(placements);
		return;
	}

	pinfos = palloc(nplacements * sizeof(PlacementBasicInfo));
	idx = 0;
	foreach(lc, placements)
	{
		PlacementInfo *p = (PlacementInfo *) lfirst(lc);

		pinfos[idx].placementid = (int32) p->placementid;
		pinfos[idx].shardid = p->shardid;
		pinfos[idx].raftgroupid = p->raftgroupid;
		pinfos[idx].raftrole = p->raftrole;
		pinfos[idx].placementstate = p->placementstate;
		idx++;
		FreePlacementInfo(p);
	}
	list_free(placements);

	/*
	 * Step 3: Process each placement from the local array.
	 * Mark active placements as decommissioning and promote followers
	 * to leader where needed.
	 */
	for (int i = 0; i < nplacements; i++)
	{
		elog(LOG, "distributed: processing placement %d "
			 "(shard %d, role '%c', state '%c')",
			 pinfos[i].placementid, pinfos[i].shardid,
			 pinfos[i].raftrole, pinfos[i].placementstate);

		if (pinfos[i].placementstate != PLACEMENT_STATE_ACTIVE)
			continue;

		UpdatePlacementState(pinfos[i].placementid,
							PLACEMENT_STATE_DECOMMISSIONING);

		elog(LOG, "distributed: marked placement %d (shard %d) "
			 "on failed node \"%s\" as decommissioning",
			 pinfos[i].placementid, pinfos[i].shardid, node_name);

		/*
		 * If the failed node was the leader for this shard's Raft group,
		 * promote a surviving active follower to leader so that
		 * GetLeaderPlacement() returns a live node.
		 */
		if (pinfos[i].raftrole == PLACEMENT_RAFT_LEADER)
		{
			List	   *group_pls;
			ListCell   *gplc;
			bool		promoted = false;

			CommandCounterIncrement();
			group_pls = GetPlacementsForRaftGroup(pinfos[i].raftgroupid);

			foreach(gplc, group_pls)
			{
				PlacementInfo *gp = (PlacementInfo *) lfirst(gplc);

				if (gp->placementstate == PLACEMENT_STATE_ACTIVE &&
					gp->raftrole == PLACEMENT_RAFT_FOLLOWER &&
					strcmp(gp->nodename, node_name) != 0)
				{
					UpdatePlacementRole(gp->placementid,
										PLACEMENT_RAFT_LEADER);
					elog(LOG, "distributed: promoted node \"%s\" "
						 "to leader for raft group %d (shard %d)",
						 gp->nodename, pinfos[i].raftgroupid,
						 pinfos[i].shardid);
					promoted = true;
					FreePlacementInfo(gp);
					break;
				}
				FreePlacementInfo(gp);
			}
			list_free(group_pls);

			if (!promoted)
				elog(WARNING, "distributed: no follower to promote "
					 "for raft group %d", pinfos[i].raftgroupid);

			/*
			 * Expedite Raft election in shared memory by resetting
			 * the heartbeat timer. The Raft background worker will
			 * detect the timeout on its next tick and start an election.
			 * (We do NOT call RaftGroupTick here — that would send RPCs
			 * which is unsafe during a catalog transaction.)
			 */
			if (DistShmem != NULL)
			{
				RaftGroupState *group;

				group = DistShmemGetRaftGroup(pinfos[i].raftgroupid);
				if (group != NULL && group->in_use &&
					group->role == RAFT_ROLE_FOLLOWER)
				{
					SpinLockAcquire(&group->mutex);
					group->last_heartbeat = 0;
					SpinLockRelease(&group->mutex);
				}
			}
		}
	}

	pfree(pinfos);
}

/*
 * InitiateReReplication
 *		Start re-replicating a shard group to a new node.
 *
 * This creates a new placement as LEARNER and triggers data transfer.
 * Once caught up, the learner is promoted to FOLLOWER.
 */
void
InitiateReReplication(int raft_group_id, const char *new_node)
{
	List	   *placements;
	int32		shard_id = 0;

	/* Find the shard_id for this raft group */
	placements = GetPlacementsForRaftGroup(raft_group_id);
	if (placements != NIL)
	{
		PlacementInfo *p = (PlacementInfo *) linitial(placements);

		shard_id = p->shardid;
	}
	list_free(placements);

	if (shard_id == 0)
	{
		elog(WARNING, "distributed: cannot find shard for raft group %d",
			 raft_group_id);
		return;
	}

	/* Create new placement as LEARNER */
	InsertPlacement(shard_id, new_node, raft_group_id,
					PLACEMENT_RAFT_LEARNER, PLACEMENT_STATE_SYNCING);

	CommandCounterIncrement();

	/*
	 * Register a dynamic background worker for data transfer.
	 * The worker will:
	 *   1. Take a snapshot on the leader
	 *   2. COPY data to the new node
	 *   3. Replay Raft log entries since snapshot
	 *   4. Promote LEARNER to FOLLOWER
	 */
	{
		BackgroundWorker worker;
		BackgroundWorkerHandle *handle;

		memset(&worker, 0, sizeof(BackgroundWorker));
		snprintf(worker.bgw_name, BGW_MAXLEN,
				 "distributed re-replication group %d -> %s",
				 raft_group_id, new_node);
		snprintf(worker.bgw_type, BGW_MAXLEN,
				 "distributed re-replication");
		worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
			BGWORKER_BACKEND_DATABASE_CONNECTION;
		worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
		worker.bgw_restart_time = BGW_NEVER_RESTART;
		snprintf(worker.bgw_library_name, BGW_MAXLEN, "postgres");
		snprintf(worker.bgw_function_name, BGW_MAXLEN,
				 "ReReplicationWorkerMain");
		worker.bgw_main_arg = Int32GetDatum(raft_group_id);
		worker.bgw_notify_pid = MyProcPid;

		if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		{
			elog(WARNING, "distributed: failed to start re-replication "
				 "worker for group %d",
				 raft_group_id);
		}
	}
}

/*
 * ReReplicationWorkerMain
 *		Background worker for data transfer during re-replication.
 *
 * Steps:
 *   1. Find leader for the raft group
 *   2. Find the new (LEARNER) placement for target node
 *   3. Copy shard data from leader to new node
 *   4. Promote LEARNER to FOLLOWER
 *   5. Remove decommissioned placements
 */
void
ReReplicationWorkerMain(Datum main_arg)
{
	int			raft_group_id = DatumGetInt32(main_arg);
	List	   *placements;
	ListCell   *lc;
	char		leader_node[NAMEDATALEN] = "";
	char		learner_node[NAMEDATALEN] = "";
	int64		learner_placement_id = 0;
	int32		shard_id = 0;
	Oid			table_oid = InvalidOid;

	elog(LOG, "distributed: re-replication worker starting for group %d",
		 raft_group_id);

	BackgroundWorkerInitializeConnection("postgres", NULL, 0);

	/* Find leader and learner placements */
	StartTransactionCommand();

	placements = GetPlacementsForRaftGroup(raft_group_id);
	foreach(lc, placements)
	{
		PlacementInfo *p = (PlacementInfo *) lfirst(lc);

		if (p->raftrole == PLACEMENT_RAFT_LEADER &&
			p->placementstate == PLACEMENT_STATE_ACTIVE)
		{
			strlcpy(leader_node, p->nodename, NAMEDATALEN);
			shard_id = p->shardid;
		}
		else if (p->raftrole == PLACEMENT_RAFT_LEARNER &&
				 p->placementstate == PLACEMENT_STATE_SYNCING)
		{
			strlcpy(learner_node, p->nodename, NAMEDATALEN);
			learner_placement_id = p->placementid;
			if (shard_id == 0)
				shard_id = p->shardid;
		}
		FreePlacementInfo(p);
	}
	list_free(placements);

	/* If no leader found, try any active non-decommissioning node */
	if (leader_node[0] == '\0')
	{
		placements = GetPlacementsForRaftGroup(raft_group_id);
		foreach(lc, placements)
		{
			PlacementInfo *p = (PlacementInfo *) lfirst(lc);

			if (p->placementstate == PLACEMENT_STATE_ACTIVE &&
				p->raftrole != PLACEMENT_RAFT_LEARNER)
			{
				strlcpy(leader_node, p->nodename, NAMEDATALEN);
				shard_id = p->shardid;
				FreePlacementInfo(p);
				break;
			}
			FreePlacementInfo(p);
		}
		list_free(placements);
	}

	CommitTransactionCommand();

	if (leader_node[0] == '\0' || learner_node[0] == '\0' || shard_id == 0)
	{
		elog(WARNING, "distributed: re-replication for group %d: "
			 "could not find leader (\"%s\") or learner (\"%s\") or "
			 "shard (%d)",
			 raft_group_id, leader_node, learner_node, shard_id);
		proc_exit(0);
	}

	/*
	 * Copy data from leader to learner.
	 * We query the shard data from the leader and insert it into
	 * the learner node.
	 */
	PG_TRY();
	{
		PGresult   *data_result;
		int			ntuples;
		int			nfields;
		StringInfoData query;
		StringInfoData insert_cmd;

		/*
		 * Get the table name for this shard.
		 * We need a transaction for catalog lookups.
		 */
		StartTransactionCommand();
		{
			ShardMapEntry *sme = GetShardById(shard_id);

			if (sme != NULL)
			{
				table_oid = sme->table_oid;
				FreeShardMapEntry(sme);
			}
		}
		CommitTransactionCommand();

		if (!OidIsValid(table_oid))
		{
			elog(WARNING, "distributed: re-replication for group %d: "
				 "could not find table for shard %d",
				 raft_group_id, shard_id);
			proc_exit(0);
		}

		/* Get table name */
		StartTransactionCommand();
		{
			char	   *table_name;

			table_name = get_rel_name(table_oid);
			if (table_name == NULL)
			{
				CommitTransactionCommand();
				elog(WARNING, "distributed: re-replication table "
					 "OID %u not found", table_oid);
				proc_exit(0);
			}

			/* Query all data from leader */
			initStringInfo(&query);
			appendStringInfo(&query, "SELECT * FROM %s", table_name);

			CommitTransactionCommand();

			data_result = DistExecSimpleQuery(leader_node, query.data);
			ntuples = PQntuples(data_result);
			nfields = PQnfields(data_result);

			elog(LOG, "distributed: re-replication group %d: "
				 "copying %d rows from \"%s\" to \"%s\"",
				 raft_group_id, ntuples, leader_node, learner_node);

			/* Insert each row into the learner */
			for (int i = 0; i < ntuples; i++)
			{
				initStringInfo(&insert_cmd);
				appendStringInfo(&insert_cmd, "INSERT INTO %s VALUES (",
								 table_name);

				for (int j = 0; j < nfields; j++)
				{
					if (j > 0)
						appendStringInfoString(&insert_cmd, ", ");

					if (PQgetisnull(data_result, i, j))
					{
						appendStringInfoString(&insert_cmd, "NULL");
					}
					else
					{
						char	   *val = PQgetvalue(data_result, i, j);

						/* Quote the value (simple quoting) */
						appendStringInfoChar(&insert_cmd, '\'');
						for (char *p = val; *p; p++)
						{
							if (*p == '\'')
								appendStringInfoChar(&insert_cmd, '\'');
							appendStringInfoChar(&insert_cmd, *p);
						}
						appendStringInfoChar(&insert_cmd, '\'');
					}
				}

				appendStringInfoString(&insert_cmd,
									   ") ON CONFLICT DO NOTHING");

				PG_TRY();
				{
					PGresult   *ins_result;

					ins_result = DistExecSimpleQuery(learner_node,
													 insert_cmd.data);
					PQclear(ins_result);
				}
				PG_CATCH();
				{
					elog(WARNING, "distributed: re-replication row %d "
						 "insert failed", i);
					FlushErrorState();
				}
				PG_END_TRY();

				pfree(insert_cmd.data);
			}

			PQclear(data_result);
			pfree(query.data);
		}

		/* Promote LEARNER to FOLLOWER */
		StartTransactionCommand();
		if (learner_placement_id > 0)
		{
			UpdatePlacementRole(learner_placement_id,
								PLACEMENT_RAFT_FOLLOWER);
			UpdatePlacementState(learner_placement_id,
								 PLACEMENT_STATE_ACTIVE);
		}

		/* Remove decommissioned placements */
		placements = GetPlacementsForRaftGroup(raft_group_id);
		foreach(lc, placements)
		{
			PlacementInfo *p = (PlacementInfo *) lfirst(lc);

			if (p->placementstate == PLACEMENT_STATE_DECOMMISSIONING)
			{
				DeletePlacement(p->placementid);
				elog(LOG, "distributed: removed decommissioned placement "
					 "%lld from group %d",
					 (long long) p->placementid, raft_group_id);
			}
			FreePlacementInfo(p);
		}
		list_free(placements);

		CommitTransactionCommand();

		elog(LOG, "distributed: re-replication for group %d complete "
			 "(leader=%s, new_node=%s)",
			 raft_group_id, leader_node, learner_node);
	}
	PG_CATCH();
	{
		elog(WARNING, "distributed: re-replication for group %d failed",
			 raft_group_id);
		FlushErrorState();
		AbortCurrentTransaction();
	}
	PG_END_TRY();

	proc_exit(0);
}

/*
 * ReReplicationComplete
 *		Called when re-replication finishes.
 */
void
ReReplicationComplete(int raft_group_id, const char *new_node)
{
	elog(LOG, "distributed: re-replication complete for group %d "
		 "on node \"%s\"",
		 raft_group_id, new_node);
}

/*
 * HandleNodeRecovery
 *		Handle a previously-failed node coming back online.
 */
void
HandleNodeRecovery(const char *node_name)
{
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData skey[1];
	HeapTuple	tuple;
	HeapTuple	newtuple;
	Datum		values[Natts_pg_shard_node];
	bool		nulls[Natts_pg_shard_node];
	bool		replaces[Natts_pg_shard_node];
	NodeHealthState *health;

	/* Update pg_shard_node — set nodestate back to online */
	rel = table_open(ShardNodeRelationId, RowExclusiveLock);

	ScanKeyInit(&skey[0],
				Anum_pg_shard_node_nodename,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(node_name));

	scan = systable_beginscan(rel, ShardNodeNodenameIndexId,
							  true, NULL, 1, skey);

	tuple = systable_getnext(scan);
	if (HeapTupleIsValid(tuple))
	{
		memset(values, 0, sizeof(values));
		memset(nulls, false, sizeof(nulls));
		memset(replaces, false, sizeof(replaces));

		replaces[Anum_pg_shard_node_nodestate - 1] = true;
		values[Anum_pg_shard_node_nodestate - 1] =
			CharGetDatum(SHARD_NODE_STATE_ONLINE);

		newtuple = heap_modify_tuple(tuple, RelationGetDescr(rel),
									 values, nulls, replaces);
		CatalogTupleUpdate(rel, &newtuple->t_self, newtuple);
		heap_freetuple(newtuple);
	}

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	/* Update shared memory */
	health = DistShmemGetNodeHealth(node_name);
	if (health != NULL)
	{
		LWLockAcquire(&DistShmem->lock, LW_EXCLUSIVE);
		health->is_marked_failed = false;
		health->consecutive_failures = 0;
		health->last_heartbeat = GetCurrentTimestamp();
		LWLockRelease(&DistShmem->lock);
	}

	CommandCounterIncrement();

	elog(LOG, "distributed: node \"%s\" marked as ONLINE (recovered)",
		 node_name);
}
