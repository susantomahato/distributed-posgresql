/*-------------------------------------------------------------------------
 *
 * raft.c
 *	  Raft consensus protocol state machine
 *
 * Implements the core Raft algorithm for per-shard-group consensus.
 * Each shard has a Raft group of replication_factor replicas (default 3).
 * A write is committed only after a majority of replicas acknowledge it.
 *
 * The state machine is ticked periodically by the Raft background worker.
 * Leaders send heartbeats, followers check election timeouts, and
 * committed entries are applied via SPI.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/distributed/raft.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distributed/dist_guc.h"
#include "distributed/dist_node.h"
#include "distributed/dist_shmem.h"
#include "distributed/placement.h"
#include "distributed/raft.h"
#include "distributed/raft_log.h"
#include "distributed/raft_rpc.h"
#include "common/pg_prng.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/s_lock.h"
#include "storage/spin.h"
#include "utils/timestamp.h"

/* Local helper declarations */
static void RaftBecomeLeader(RaftGroupState *group);
static void RaftBecomeFollower(RaftGroupState *group, int64 term);
static void RaftSendAppendEntriesToPeer(RaftGroupState *group, int peer_idx);
static void RaftAdvanceCommitIndex(RaftGroupState *group);
static int	RaftFindPeerIndex(RaftGroupState *group, const char *name);
static bool RaftIsLogUpToDate(RaftGroupState *group,
							  int64 last_log_index, int64 last_log_term);
static TimestampTz RaftGetElapsedMs(TimestampTz since);

/*
 * RaftGroupInit
 *		Initialize a Raft group with peers.
 *
 * Called when a new distributed table is created and shard placements
 * are assigned to nodes.
 */
void
RaftGroupInit(RaftGroupState *group, int group_id, int shard_id,
			  const char **peer_names, int num_peers)
{
	Assert(group != NULL);
	Assert(num_peers > 0 && num_peers <= RAFT_MAX_PEERS);

	SpinLockAcquire(&group->mutex);

	group->raft_group_id = group_id;
	group->shard_id = shard_id;
	group->in_use = true;
	group->role = RAFT_ROLE_FOLLOWER;
	group->current_term = 0;
	group->voted_for[0] = '\0';
	group->commit_index = 0;
	group->last_applied = 0;
	group->last_log_index = 0;
	group->last_log_term = 0;
	group->num_peers = num_peers;
	group->last_heartbeat = GetCurrentTimestamp();

	/* Randomize election timeout */
	group->election_timeout_ms =
		dist_election_timeout_min_ms +
		(pg_prng_uint32(&pg_global_prng_state) %
		 (dist_election_timeout_max_ms - dist_election_timeout_min_ms));

	for (int i = 0; i < num_peers; i++)
	{
		strlcpy(group->peer_names[i], peer_names[i], NAMEDATALEN);
		group->next_index[i] = 1;
		group->match_index[i] = 0;
	}

	/* Clear remaining peer slots */
	for (int i = num_peers; i < RAFT_MAX_PEERS; i++)
	{
		group->peer_names[i][0] = '\0';
		group->next_index[i] = 0;
		group->match_index[i] = 0;
	}

	SpinLockRelease(&group->mutex);

	elog(DEBUG1, "raft: initialized group %d for shard %d with %d peers",
		 group_id, shard_id, num_peers);
}

/*
 * RaftGroupTick
 *		Periodic processing for a Raft group.
 *
 * Called by the background worker every raft_tick_interval_ms.
 * Leaders send heartbeats, followers check election timeouts.
 */
void
RaftGroupTick(RaftGroupState *group)
{
	TimestampTz now = GetCurrentTimestamp();
	long		elapsed_ms;

	if (!group->in_use)
		return;

	elapsed_ms = RaftGetElapsedMs(group->last_heartbeat);

	switch (group->role)
	{
		case RAFT_ROLE_LEADER:
			/* Send heartbeats if interval elapsed */
			if (elapsed_ms >= dist_heartbeat_interval_ms)
			{
				for (int i = 0; i < group->num_peers; i++)
				{
					if (!IsLocalNode(group->peer_names[i]))
						RaftSendHeartbeat(group, group->peer_names[i]);
				}
				group->last_heartbeat = now;
			}

			/* Try to advance commit index */
			RaftAdvanceCommitIndex(group);
			break;

		case RAFT_ROLE_FOLLOWER:
			/* Check election timeout */
			if (elapsed_ms >= group->election_timeout_ms)
			{
				elog(LOG, "raft: group %d election timeout (%ld ms), "
					 "starting election",
					 group->raft_group_id, elapsed_ms);
				RaftStartElection(group);
			}
			break;

		case RAFT_ROLE_CANDIDATE:
			/* Re-check election timeout (retry if no winner) */
			if (elapsed_ms >= group->election_timeout_ms)
			{
				elog(LOG, "raft: group %d election timeout as candidate, "
					 "restarting election",
					 group->raft_group_id);
				RaftStartElection(group);
			}
			break;

		case RAFT_ROLE_LEARNER:
			/* Learners don't participate in elections */
			break;
	}

	/* Apply committed entries */
	RaftApplyCommitted(group);
}

/*
 * RaftPropose
 *		Propose a new log entry to the Raft group.
 *
 * Only the leader can propose entries. The entry is appended to the
 * leader's log and replicated to followers. This function blocks
 * until the entry is committed (majority ack) or times out.
 *
 * Returns true if the entry was committed, false otherwise.
 */
bool
RaftPropose(RaftGroupState *group, RaftLogEntry *entry)
{
	TimestampTz start_time;
	int			timeout_ms = 5000;	/* 5 second timeout */

	if (group->role != RAFT_ROLE_LEADER)
	{
		ereport(WARNING,
				(errmsg("raft: cannot propose on non-leader (group %d, "
						"role %s)",
						group->raft_group_id,
						RaftRoleToString(group->role))));
		return false;
	}

	/* Assign log index and term */
	LWLockAcquire(&group->raft_lock, LW_EXCLUSIVE);

	group->last_log_index++;
	entry->log_index = group->last_log_index;
	entry->log_term = group->current_term;
	group->last_log_term = group->current_term;

	LWLockRelease(&group->raft_lock);

	/* Append to local log */
	RaftLogAppend(group->raft_group_id, entry);

	/* Send AppendEntries to all followers */
	for (int i = 0; i < group->num_peers; i++)
	{
		if (!IsLocalNode(group->peer_names[i]))
			RaftSendAppendEntriesToPeer(group, i);
	}

	/* Wait for majority to acknowledge */
	start_time = GetCurrentTimestamp();
	while (group->commit_index < entry->log_index)
	{
		long		elapsed;

		CHECK_FOR_INTERRUPTS();

		elapsed = RaftGetElapsedMs(start_time);
		if (elapsed >= timeout_ms)
		{
			ereport(WARNING,
					(errmsg("raft: propose timeout for group %d "
							"(index %lld)",
							group->raft_group_id,
							(long long) entry->log_index)));
			return false;
		}

		/* Brief sleep before retry */
		pg_usleep(1000);		/* 1ms */

		/* Try to advance commit index */
		RaftAdvanceCommitIndex(group);
	}

	elog(DEBUG1, "raft: entry committed (group %d, index %lld, term %lld)",
		 group->raft_group_id,
		 (long long) entry->log_index,
		 (long long) entry->log_term);

	return true;
}

/*
 * RaftStartElection
 *		Transition from follower/candidate to candidate and start election.
 */
void
RaftStartElection(RaftGroupState *group)
{
	int			votes_received = 1;	/* vote for self */
	int			majority;

	SpinLockAcquire(&group->mutex);

	group->current_term++;
	group->role = RAFT_ROLE_CANDIDATE;
	strlcpy(group->voted_for, dist_node_name, NAMEDATALEN);
	group->last_heartbeat = GetCurrentTimestamp();

	/* Randomize timeout for next round */
	RaftRandomizeElectionTimeout(group);

	SpinLockRelease(&group->mutex);

	majority = (group->num_peers / 2) + 1;

	elog(LOG, "raft: group %d starting election for term %lld "
		 "(need %d votes)",
		 group->raft_group_id,
		 (long long) group->current_term, majority);

	/* Send RequestVote to all peers */
	for (int i = 0; i < group->num_peers; i++)
	{
		RequestVoteRequest req;
		RequestVoteResponse resp;

		if (IsLocalNode(group->peer_names[i]))
			continue;

		req.group_id = group->raft_group_id;
		req.term = group->current_term;
		strlcpy(req.candidate_name, dist_node_name, NAMEDATALEN);
		req.last_log_index = group->last_log_index;
		req.last_log_term = group->last_log_term;

		PG_TRY();
		{
			resp = RaftSendRequestVote(group->peer_names[i], &req);

			if (resp.term > group->current_term)
			{
				/* Higher term seen — step down */
				RaftStepDown(group, resp.term);
				return;
			}

			if (resp.vote_granted)
				votes_received++;
		}
		PG_CATCH();
		{
			/* Peer unreachable — skip */
			elog(DEBUG1, "raft: could not reach peer %s for vote",
				 group->peer_names[i]);
			FlushErrorState();
		}
		PG_END_TRY();
	}

	if (votes_received >= majority)
	{
		elog(LOG, "raft: group %d won election for term %lld "
			 "(%d votes received)",
			 group->raft_group_id,
			 (long long) group->current_term, votes_received);
		RaftBecomeLeader(group);
	}
	else
	{
		elog(LOG, "raft: group %d election failed for term %lld "
			 "(%d votes, need %d)",
			 group->raft_group_id,
			 (long long) group->current_term,
			 votes_received, majority);
	}
}

/*
 * RaftStepDown
 *		Transition to follower on seeing a higher term.
 */
void
RaftStepDown(RaftGroupState *group, int64 new_term)
{
	SpinLockAcquire(&group->mutex);

	elog(LOG, "raft: group %d stepping down from %s (term %lld -> %lld)",
		 group->raft_group_id,
		 RaftRoleToString(group->role),
		 (long long) group->current_term,
		 (long long) new_term);

	group->current_term = new_term;
	group->role = RAFT_ROLE_FOLLOWER;
	group->voted_for[0] = '\0';
	group->last_heartbeat = GetCurrentTimestamp();
	RaftRandomizeElectionTimeout(group);

	SpinLockRelease(&group->mutex);

	/* Update placement catalog */
	{
		List	   *placements = GetPlacementsForRaftGroup(group->raft_group_id);
		ListCell   *lc;

		foreach(lc, placements)
		{
			PlacementInfo *p = (PlacementInfo *) lfirst(lc);

			if (IsLocalNode(p->nodename) &&
				p->raftrole == PLACEMENT_RAFT_LEADER)
			{
				UpdatePlacementRole(p->placementid, PLACEMENT_RAFT_FOLLOWER);
				UpdatePlacementTerm(p->placementid, new_term);
			}
			FreePlacementInfo(p);
		}
		list_free(placements);
	}
}

/*
 * RaftApplyCommitted
 *		Apply all committed but not yet applied log entries.
 *
 * Entries are executed via SPI within the background worker.
 */
void
RaftApplyCommitted(RaftGroupState *group)
{
	while (group->last_applied < group->commit_index)
	{
		int64		apply_index = group->last_applied + 1;
		RaftLogEntry *entry;

		entry = RaftLogRead(group->raft_group_id, apply_index);
		if (entry == NULL)
		{
			elog(WARNING, "raft: group %d missing log entry at index %lld",
				 group->raft_group_id, (long long) apply_index);
			break;
		}

		/* Apply the entry */
		if (entry->cmd_type != 'n')	/* skip no-op entries */
		{
			elog(DEBUG2, "raft: group %d applying entry %lld "
				 "(type=%c, sql=%s)",
				 group->raft_group_id,
				 (long long) apply_index,
				 entry->cmd_type,
				 entry->sql_cmd ? entry->sql_cmd : "(null)");

			/*
			 * In a full implementation, we would execute the SQL command
			 * via SPI here. For now, we just advance last_applied.
			 *
			 * TODO: SPI_connect() + SPI_exec(entry->sql_cmd) + SPI_finish()
			 */
		}

		SpinLockAcquire(&group->mutex);
		group->last_applied = apply_index;
		SpinLockRelease(&group->mutex);

		RaftLogEntryFree(entry);
	}
}

/*
 * RaftHandleAppendEntries
 *		Process an incoming AppendEntries RPC from a leader.
 *
 * Returns true if the entries were accepted.
 */
bool
RaftHandleAppendEntries(RaftGroupState *group,
						int64 term,
						const char *leader_name,
						int64 prev_log_index,
						int64 prev_log_term,
						int64 leader_commit,
						RaftLogEntry *entries,
						int num_entries)
{
	/* Reply false if term < currentTerm */
	if (term < group->current_term)
	{
		elog(DEBUG1, "raft: group %d rejecting AppendEntries "
			 "(term %lld < %lld)",
			 group->raft_group_id,
			 (long long) term,
			 (long long) group->current_term);
		return false;
	}

	/* If term >= currentTerm, update and become follower */
	if (term > group->current_term)
		RaftBecomeFollower(group, term);

	/* Reset election timeout (we heard from leader) */
	group->last_heartbeat = GetCurrentTimestamp();

	/* Reply false if log doesn't contain an entry at prevLogIndex
	 * matching prevLogTerm */
	if (prev_log_index > 0)
	{
		RaftLogEntry *prev_entry;

		prev_entry = RaftLogRead(group->raft_group_id, prev_log_index);
		if (prev_entry == NULL || prev_entry->log_term != prev_log_term)
		{
			if (prev_entry)
				RaftLogEntryFree(prev_entry);
			elog(DEBUG1, "raft: group %d log mismatch at index %lld",
				 group->raft_group_id,
				 (long long) prev_log_index);
			return false;
		}
		RaftLogEntryFree(prev_entry);
	}

	/* If existing entry conflicts with new one, delete it and all after */
	for (int i = 0; i < num_entries; i++)
	{
		int64		entry_index = entries[i].log_index;
		RaftLogEntry *existing;

		existing = RaftLogRead(group->raft_group_id, entry_index);
		if (existing != NULL)
		{
			if (existing->log_term != entries[i].log_term)
			{
				RaftLogTruncateFrom(group->raft_group_id, entry_index);
				RaftLogEntryFree(existing);
				break;
			}
			RaftLogEntryFree(existing);
		}
	}

	/* Append new entries not already in the log */
	for (int i = 0; i < num_entries; i++)
	{
		if (entries[i].log_index > group->last_log_index)
		{
			RaftLogAppend(group->raft_group_id, &entries[i]);
			group->last_log_index = entries[i].log_index;
			group->last_log_term = entries[i].log_term;
		}
	}

	/* Update commit index */
	if (leader_commit > group->commit_index)
	{
		SpinLockAcquire(&group->mutex);
		group->commit_index = Min(leader_commit, group->last_log_index);
		SpinLockRelease(&group->mutex);
	}

	return true;
}

/*
 * RaftHandleRequestVote
 *		Process an incoming RequestVote RPC.
 *
 * Returns true if vote is granted.
 */
bool
RaftHandleRequestVote(RaftGroupState *group,
					  int64 term,
					  const char *candidate_name,
					  int64 last_log_index,
					  int64 last_log_term)
{
	/* Reply false if term < currentTerm */
	if (term < group->current_term)
	{
		elog(DEBUG1, "raft: group %d denying vote to %s "
			 "(term %lld < %lld)",
			 group->raft_group_id, candidate_name,
			 (long long) term,
			 (long long) group->current_term);
		return false;
	}

	/* If term > currentTerm, step down */
	if (term > group->current_term)
		RaftBecomeFollower(group, term);

	/* Grant vote if we haven't voted yet and candidate's log is up to date */
	SpinLockAcquire(&group->mutex);

	if ((group->voted_for[0] == '\0' ||
		 strcmp(group->voted_for, candidate_name) == 0) &&
		RaftIsLogUpToDate(group, last_log_index, last_log_term))
	{
		strlcpy(group->voted_for, candidate_name, NAMEDATALEN);
		group->last_heartbeat = GetCurrentTimestamp();
		SpinLockRelease(&group->mutex);

		elog(LOG, "raft: group %d granting vote to %s for term %lld",
			 group->raft_group_id, candidate_name,
			 (long long) term);
		return true;
	}

	SpinLockRelease(&group->mutex);

	elog(DEBUG1, "raft: group %d denying vote to %s "
		 "(already voted for %s)",
		 group->raft_group_id, candidate_name, group->voted_for);
	return false;
}

/*
 * RaftRoleToString
 *		Convert role enum to string.
 */
const char *
RaftRoleToString(RaftRole role)
{
	switch (role)
	{
		case RAFT_ROLE_FOLLOWER:
			return "follower";
		case RAFT_ROLE_CANDIDATE:
			return "candidate";
		case RAFT_ROLE_LEADER:
			return "leader";
		case RAFT_ROLE_LEARNER:
			return "learner";
		default:
			return "unknown";
	}
}

/*
 * RaftRandomizeElectionTimeout
 *		Set a random election timeout between min and max.
 */
void
RaftRandomizeElectionTimeout(RaftGroupState *group)
{
	group->election_timeout_ms =
		dist_election_timeout_min_ms +
		(pg_prng_uint32(&pg_global_prng_state) %
		 (dist_election_timeout_max_ms - dist_election_timeout_min_ms));
}

/* ---- Internal helpers ---- */

/*
 * RaftBecomeLeader
 *		Transition from candidate to leader.
 */
static void
RaftBecomeLeader(RaftGroupState *group)
{
	SpinLockAcquire(&group->mutex);

	group->role = RAFT_ROLE_LEADER;
	group->last_heartbeat = GetCurrentTimestamp();

	/* Initialize next_index and match_index for all peers */
	for (int i = 0; i < group->num_peers; i++)
	{
		group->next_index[i] = group->last_log_index + 1;
		group->match_index[i] = 0;

		/* Set our own match_index */
		if (IsLocalNode(group->peer_names[i]))
			group->match_index[i] = group->last_log_index;
	}

	SpinLockRelease(&group->mutex);

	/* Update placement catalog: mark this node as leader */
	{
		List	   *placements = GetPlacementsForRaftGroup(group->raft_group_id);
		ListCell   *lc;

		foreach(lc, placements)
		{
			PlacementInfo *p = (PlacementInfo *) lfirst(lc);

			if (IsLocalNode(p->nodename))
			{
				UpdatePlacementRole(p->placementid, PLACEMENT_RAFT_LEADER);
				UpdatePlacementTerm(p->placementid, group->current_term);
			}
			else if (p->raftrole == PLACEMENT_RAFT_LEADER)
			{
				/* Demote previous leader */
				UpdatePlacementRole(p->placementid, PLACEMENT_RAFT_FOLLOWER);
			}
			FreePlacementInfo(p);
		}
		list_free(placements);
	}

	/* Send immediate heartbeat to assert leadership */
	for (int i = 0; i < group->num_peers; i++)
	{
		if (!IsLocalNode(group->peer_names[i]))
			RaftSendHeartbeat(group, group->peer_names[i]);
	}

	elog(LOG, "raft: group %d became leader for term %lld",
		 group->raft_group_id,
		 (long long) group->current_term);
}

/*
 * RaftBecomeFollower
 *		Transition to follower with a new term.
 */
static void
RaftBecomeFollower(RaftGroupState *group, int64 term)
{
	SpinLockAcquire(&group->mutex);

	group->current_term = term;
	group->role = RAFT_ROLE_FOLLOWER;
	group->voted_for[0] = '\0';
	group->last_heartbeat = GetCurrentTimestamp();
	RaftRandomizeElectionTimeout(group);

	SpinLockRelease(&group->mutex);
}

/*
 * RaftSendAppendEntriesToPeer
 *		Send AppendEntries RPC to a specific peer.
 */
static void
RaftSendAppendEntriesToPeer(RaftGroupState *group, int peer_idx)
{
	AppendEntriesRequest req;
	AppendEntriesResponse resp;
	int64		next_idx;
	int64		prev_idx;
	List	   *entries_list;

	next_idx = group->next_index[peer_idx];
	prev_idx = next_idx - 1;

	req.group_id = group->raft_group_id;
	req.term = group->current_term;
	strlcpy(req.leader_name, dist_node_name, NAMEDATALEN);
	req.prev_log_index = prev_idx;
	req.leader_commit = group->commit_index;

	/* Get prev_log_term */
	if (prev_idx > 0)
	{
		RaftLogEntry *prev_entry;

		prev_entry = RaftLogRead(group->raft_group_id, prev_idx);
		req.prev_log_term = prev_entry ? prev_entry->log_term : 0;
		if (prev_entry)
			RaftLogEntryFree(prev_entry);
	}
	else
	{
		req.prev_log_term = 0;
	}

	/* Get entries to send */
	entries_list = RaftLogReadRange(group->raft_group_id,
								   next_idx,
								   group->last_log_index);

	req.num_entries = list_length(entries_list);
	if (req.num_entries > 0)
	{
		ListCell   *lc;
		int			i = 0;

		req.entries = palloc(req.num_entries * sizeof(RaftLogEntry));
		foreach(lc, entries_list)
		{
			RaftLogEntry *e = (RaftLogEntry *) lfirst(lc);

			memcpy(&req.entries[i], e, sizeof(RaftLogEntry));
			i++;
		}
	}
	else
	{
		req.entries = NULL;
	}

	PG_TRY();
	{
		resp = RaftSendAppendEntries(group->peer_names[peer_idx], &req);

		if (resp.term > group->current_term)
		{
			RaftStepDown(group, resp.term);
		}
		else if (resp.success)
		{
			/* Update next_index and match_index */
			SpinLockAcquire(&group->mutex);
			if (req.num_entries > 0)
			{
				group->next_index[peer_idx] =
					req.entries[req.num_entries - 1].log_index + 1;
				group->match_index[peer_idx] =
					req.entries[req.num_entries - 1].log_index;
			}
			SpinLockRelease(&group->mutex);
		}
		else
		{
			/* Decrement next_index and retry */
			SpinLockAcquire(&group->mutex);
			if (group->next_index[peer_idx] > 1)
				group->next_index[peer_idx]--;
			SpinLockRelease(&group->mutex);
		}
	}
	PG_CATCH();
	{
		/* Peer unreachable */
		elog(DEBUG1, "raft: group %d could not send AppendEntries to %s",
			 group->raft_group_id, group->peer_names[peer_idx]);
		FlushErrorState();
	}
	PG_END_TRY();

	if (req.entries)
		pfree(req.entries);
	list_free_deep(entries_list);
}

/*
 * RaftAdvanceCommitIndex
 *		Leader advances commit index based on majority match_index.
 */
static void
RaftAdvanceCommitIndex(RaftGroupState *group)
{
	int64		new_commit;
	int			i;

	if (group->role != RAFT_ROLE_LEADER)
		return;

	/*
	 * Find the highest index N such that a majority of match_index[i] >= N
	 * and log[N].term == currentTerm.
	 */
	for (new_commit = group->last_log_index;
		 new_commit > group->commit_index;
		 new_commit--)
	{
		int			count = 0;

		for (i = 0; i < group->num_peers; i++)
		{
			if (group->match_index[i] >= new_commit)
				count++;
		}

		if (count >= (group->num_peers / 2) + 1)
		{
			/* Verify the entry at new_commit has current term */
			RaftLogEntry *entry;

			entry = RaftLogRead(group->raft_group_id, new_commit);
			if (entry != NULL && entry->log_term == group->current_term)
			{
				SpinLockAcquire(&group->mutex);
				group->commit_index = new_commit;
				SpinLockRelease(&group->mutex);
				RaftLogEntryFree(entry);
				break;
			}
			if (entry)
				RaftLogEntryFree(entry);
		}
	}
}

/*
 * RaftFindPeerIndex
 *		Find the index of a peer by name.
 */
static int
RaftFindPeerIndex(RaftGroupState *group, const char *name)
{
	for (int i = 0; i < group->num_peers; i++)
	{
		if (strcmp(group->peer_names[i], name) == 0)
			return i;
	}
	return -1;
}

/*
 * RaftIsLogUpToDate
 *		Check if candidate's log is at least as up-to-date as ours.
 */
static bool
RaftIsLogUpToDate(RaftGroupState *group,
				  int64 last_log_index, int64 last_log_term)
{
	if (last_log_term != group->last_log_term)
		return last_log_term > group->last_log_term;

	return last_log_index >= group->last_log_index;
}

/*
 * RaftGetElapsedMs
 *		Get milliseconds elapsed since a timestamp.
 */
static TimestampTz
RaftGetElapsedMs(TimestampTz since)
{
	TimestampTz now = GetCurrentTimestamp();
	long		secs;
	int			usecs;

	TimestampDifference(since, now, &secs, &usecs);
	return secs * 1000 + usecs / 1000;
}
