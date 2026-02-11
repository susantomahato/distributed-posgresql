/*-------------------------------------------------------------------------
 *
 * raft.h
 *	  Raft consensus protocol types and API
 *
 * Each shard has a Raft group consisting of replication_factor replicas.
 * The Raft consensus protocol ensures strong consistency for writes:
 * a write is committed only after a majority of replicas acknowledge it.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/include/distributed/raft.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RAFT_H
#define RAFT_H

#include "postgres.h"
#include "storage/lwlock.h"
#include "storage/s_lock.h"
#include "utils/timestamp.h"

/*
 * Maximum peers per Raft group (same as MAX_RAFT_PEERS in dist_shmem.h)
 */
#define RAFT_MAX_PEERS		5

/*
 * RaftRole — state of this node within a Raft group
 */
typedef enum RaftRole
{
	RAFT_ROLE_FOLLOWER = 0,
	RAFT_ROLE_CANDIDATE,
	RAFT_ROLE_LEADER,
	RAFT_ROLE_LEARNER			/* non-voting replica being brought up to speed */
} RaftRole;

/*
 * RaftLogEntry — a single entry in the Raft log
 */
typedef struct RaftLogEntry
{
	int64		log_index;
	int64		log_term;
	char		cmd_type;		/* 'i'=insert, 'u'=update, 'd'=delete, 'n'=noop */
	Oid			table_oid;
	int32		shard_id;
	char	   *sql_cmd;		/* SQL command text */
	bytea	   *tuple_data;		/* serialized tuple data (optional) */
	TransactionId orig_xid;		/* original transaction ID */
} RaftLogEntry;

/*
 * RaftGroupState — shared-memory state for one Raft group
 *
 * This is kept in shared memory so the background worker can tick
 * all active groups efficiently.
 */
typedef struct RaftGroupState
{
	/* Identity */
	int			raft_group_id;
	int			shard_id;
	bool		in_use;

	/* Raft persistent state */
	RaftRole	role;
	int64		current_term;
	char		voted_for[NAMEDATALEN];
	int64		commit_index;
	int64		last_applied;
	int64		last_log_index;
	int64		last_log_term;

	/* Leader-only volatile state (per peer) */
	int64		next_index[RAFT_MAX_PEERS];
	int64		match_index[RAFT_MAX_PEERS];

	/* Cluster membership */
	int			num_peers;
	char		peer_names[RAFT_MAX_PEERS][NAMEDATALEN];

	/* Timing */
	TimestampTz last_heartbeat;
	int			election_timeout_ms;

	/* Synchronization */
	slock_t		mutex;
	LWLock		raft_lock;
} RaftGroupState;

/* Core Raft operations */
extern void RaftGroupInit(RaftGroupState *group, int group_id, int shard_id,
						  const char **peer_names, int num_peers);
extern void RaftGroupTick(RaftGroupState *group);
extern bool RaftPropose(RaftGroupState *group, RaftLogEntry *entry);
extern void RaftStartElection(RaftGroupState *group);
extern void RaftStepDown(RaftGroupState *group, int64 new_term);
extern void RaftApplyCommitted(RaftGroupState *group);

/* AppendEntries RPC handler */
extern bool RaftHandleAppendEntries(RaftGroupState *group,
									int64 term,
									const char *leader_name,
									int64 prev_log_index,
									int64 prev_log_term,
									int64 leader_commit,
									RaftLogEntry *entries,
									int num_entries);

/* RequestVote RPC handler */
extern bool RaftHandleRequestVote(RaftGroupState *group,
								  int64 term,
								  const char *candidate_name,
								  int64 last_log_index,
								  int64 last_log_term);

/* Utility */
extern const char *RaftRoleToString(RaftRole role);
extern void RaftRandomizeElectionTimeout(RaftGroupState *group);

#endif							/* RAFT_H */
