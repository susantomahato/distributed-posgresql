/*-------------------------------------------------------------------------
 *
 * raft_rpc.h
 *	  Raft RPC message types and transport API
 *
 * Raft messages are sent between nodes as SQL function calls over libpq.
 * This keeps the transport simple and reuses the existing connection pool.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/include/distributed/raft_rpc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RAFT_RPC_H
#define RAFT_RPC_H

#include "distributed/raft.h"
#include "fmgr.h"

/*
 * AppendEntries request/response
 */
typedef struct AppendEntriesRequest
{
	int			group_id;
	int64		term;
	char		leader_name[NAMEDATALEN];
	int64		prev_log_index;
	int64		prev_log_term;
	int64		leader_commit;
	int			num_entries;
	RaftLogEntry *entries;
} AppendEntriesRequest;

typedef struct AppendEntriesResponse
{
	int64		term;
	bool		success;
	int64		match_index;
} AppendEntriesResponse;

/*
 * RequestVote request/response
 */
typedef struct RequestVoteRequest
{
	int			group_id;
	int64		term;
	char		candidate_name[NAMEDATALEN];
	int64		last_log_index;
	int64		last_log_term;
} RequestVoteRequest;

typedef struct RequestVoteResponse
{
	int64		term;
	bool		vote_granted;
} RequestVoteResponse;

/* Send RPC messages to remote nodes */
extern AppendEntriesResponse RaftSendAppendEntries(const char *node_name,
												   AppendEntriesRequest *req);
extern RequestVoteResponse RaftSendRequestVote(const char *node_name,
											   RequestVoteRequest *req);

/* Heartbeat (empty AppendEntries) */
extern void RaftSendHeartbeat(RaftGroupState *group, const char *peer_name);

/* SQL-callable RPC endpoint functions */
extern Datum dist_raft_append_entries(PG_FUNCTION_ARGS);
extern Datum dist_raft_request_vote(PG_FUNCTION_ARGS);

/* Serialization helpers */
extern bytea *RaftSerializeEntries(RaftLogEntry *entries, int count);
extern RaftLogEntry *RaftDeserializeEntries(bytea *data, int *count);

#endif							/* RAFT_RPC_H */
