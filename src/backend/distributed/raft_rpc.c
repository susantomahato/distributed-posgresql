/*-------------------------------------------------------------------------
 *
 * raft_rpc.c
 *	  Raft RPC transport via libpq SQL function calls
 *
 * Raft messages are sent between nodes as SQL function calls:
 *   - dist_raft_append_entries() — AppendEntries RPC
 *   - dist_raft_request_vote() — RequestVote RPC
 *
 * These SQL functions are registered as C functions and serve as
 * RPC endpoints on the receiving side.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/distributed/raft_rpc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distributed/dist_connection.h"
#include "distributed/dist_guc.h"
#include "distributed/dist_shmem.h"
#include "distributed/raft.h"
#include "distributed/raft_log.h"
#include "distributed/raft_rpc.h"
#include "access/htup_details.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "utils/builtins.h"

PG_FUNCTION_INFO_V1(dist_raft_append_entries);
PG_FUNCTION_INFO_V1(dist_raft_request_vote);

/*
 * RaftSendAppendEntries
 *		Send AppendEntries RPC to a remote node via libpq.
 */
AppendEntriesResponse
RaftSendAppendEntries(const char *node_name, AppendEntriesRequest *req)
{
	AppendEntriesResponse resp;
	PGresult   *result;
	StringInfoData query;

	memset(&resp, 0, sizeof(resp));
	resp.success = false;

	initStringInfo(&query);

	if (req->num_entries > 0 && req->entries != NULL)
	{
		bytea	   *entries_data;

		entries_data = RaftSerializeEntries(req->entries, req->num_entries);

		appendStringInfo(&query,
						 "SELECT * FROM dist_raft_append_entries("
						 "%d, %lld, '%s', %lld, %lld, %lld, '%s'::bytea)",
						 req->group_id,
						 (long long) req->term,
						 req->leader_name,
						 (long long) req->prev_log_index,
						 (long long) req->prev_log_term,
						 (long long) req->leader_commit,
						 "\\x00");	/* placeholder */
	}
	else
	{
		/* Heartbeat — no entries */
		appendStringInfo(&query,
						 "SELECT * FROM dist_raft_append_entries("
						 "%d, %lld, '%s', %lld, %lld, %lld, NULL)",
						 req->group_id,
						 (long long) req->term,
						 req->leader_name,
						 (long long) req->prev_log_index,
						 (long long) req->prev_log_term,
						 (long long) req->leader_commit);
	}

	result = DistExecSimpleQuery(node_name, query.data);

	if (PQresultStatus(result) == PGRES_TUPLES_OK &&
		PQntuples(result) > 0 && PQnfields(result) >= 3)
	{
		resp.term = atoll(PQgetvalue(result, 0, 0));
		resp.success = (strcmp(PQgetvalue(result, 0, 1), "t") == 0);
		resp.match_index = atoll(PQgetvalue(result, 0, 2));
	}

	PQclear(result);
	pfree(query.data);

	return resp;
}

/*
 * RaftSendRequestVote
 *		Send RequestVote RPC to a remote node via libpq.
 */
RequestVoteResponse
RaftSendRequestVote(const char *node_name, RequestVoteRequest *req)
{
	RequestVoteResponse resp;
	PGresult   *result;
	StringInfoData query;

	memset(&resp, 0, sizeof(resp));
	resp.vote_granted = false;

	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT * FROM dist_raft_request_vote("
					 "%d, %lld, '%s', %lld, %lld)",
					 req->group_id,
					 (long long) req->term,
					 req->candidate_name,
					 (long long) req->last_log_index,
					 (long long) req->last_log_term);

	result = DistExecSimpleQuery(node_name, query.data);

	if (PQresultStatus(result) == PGRES_TUPLES_OK &&
		PQntuples(result) > 0 && PQnfields(result) >= 2)
	{
		resp.term = atoll(PQgetvalue(result, 0, 0));
		resp.vote_granted = (strcmp(PQgetvalue(result, 0, 1), "t") == 0);
	}

	PQclear(result);
	pfree(query.data);

	return resp;
}

/*
 * RaftSendHeartbeat
 *		Send an empty AppendEntries as heartbeat.
 */
void
RaftSendHeartbeat(RaftGroupState *group, const char *peer_name)
{
	AppendEntriesRequest req;
	AppendEntriesResponse resp;

	memset(&req, 0, sizeof(req));
	req.group_id = group->raft_group_id;
	req.term = group->current_term;
	strlcpy(req.leader_name, dist_node_name, NAMEDATALEN);
	req.prev_log_index = group->last_log_index;
	req.prev_log_term = group->last_log_term;
	req.leader_commit = group->commit_index;
	req.num_entries = 0;
	req.entries = NULL;

	PG_TRY();
	{
		resp = RaftSendAppendEntries(peer_name, &req);

		if (resp.term > group->current_term)
			RaftStepDown(group, resp.term);
	}
	PG_CATCH();
	{
		elog(DEBUG1, "raft: heartbeat to %s failed (group %d)",
			 peer_name, group->raft_group_id);
		FlushErrorState();
	}
	PG_END_TRY();
}

/*
 * dist_raft_append_entries — SQL endpoint for AppendEntries RPC
 *
 * Called on the receiving node. Processes the request and returns
 * (term, success, match_index).
 */
Datum
dist_raft_append_entries(PG_FUNCTION_ARGS)
{
	int			group_id = PG_GETARG_INT32(0);
	int64		term = PG_GETARG_INT64(1);
	char	   *leader_name = text_to_cstring(PG_GETARG_TEXT_PP(2));
	int64		prev_log_index = PG_GETARG_INT64(3);
	int64		prev_log_term = PG_GETARG_INT64(4);
	int64		leader_commit = PG_GETARG_INT64(5);
	RaftGroupState *group;
	bool		success;
	RaftLogEntry *entries = NULL;
	int			num_entries = 0;
	TupleDesc	tupdesc;
	Datum		values[3];
	bool		nulls[3] = {false};
	HeapTuple	result_tuple;

	/* Deserialize entries if provided */
	if (!PG_ARGISNULL(6))
	{
		bytea	   *data = PG_GETARG_BYTEA_PP(6);

		entries = RaftDeserializeEntries(data, &num_entries);
	}

	/* Find the Raft group */
	group = DistShmemGetRaftGroup(group_id);
	if (group == NULL)
	{
		ereport(WARNING,
				(errmsg("raft: unknown group %d in AppendEntries",
						group_id)));
		PG_RETURN_NULL();
	}

	/* Process the AppendEntries */
	success = RaftHandleAppendEntries(group, term, leader_name,
									  prev_log_index, prev_log_term,
									  leader_commit, entries, num_entries);

	/* Build result tuple */
	tupdesc = CreateTemplateTupleDesc(3);
	TupleDescInitEntry(tupdesc, 1, "term", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, 2, "success", BOOLOID, -1, 0);
	TupleDescInitEntry(tupdesc, 3, "match_index", INT8OID, -1, 0);
	tupdesc = BlessTupleDesc(tupdesc);

	values[0] = Int64GetDatum(group->current_term);
	values[1] = BoolGetDatum(success);
	values[2] = Int64GetDatum(group->last_log_index);

	result_tuple = heap_form_tuple(tupdesc, values, nulls);

	if (entries)
		pfree(entries);

	PG_RETURN_DATUM(HeapTupleGetDatum(result_tuple));
}

/*
 * dist_raft_request_vote — SQL endpoint for RequestVote RPC
 *
 * Called on the receiving node. Returns (term, vote_granted).
 */
Datum
dist_raft_request_vote(PG_FUNCTION_ARGS)
{
	int			group_id = PG_GETARG_INT32(0);
	int64		term = PG_GETARG_INT64(1);
	char	   *candidate_name = text_to_cstring(PG_GETARG_TEXT_PP(2));
	int64		last_log_index = PG_GETARG_INT64(3);
	int64		last_log_term = PG_GETARG_INT64(4);
	RaftGroupState *group;
	bool		vote_granted;
	TupleDesc	tupdesc;
	Datum		values[2];
	bool		nulls[2] = {false};
	HeapTuple	result_tuple;

	group = DistShmemGetRaftGroup(group_id);
	if (group == NULL)
	{
		ereport(WARNING,
				(errmsg("raft: unknown group %d in RequestVote",
						group_id)));
		PG_RETURN_NULL();
	}

	vote_granted = RaftHandleRequestVote(group, term, candidate_name,
										 last_log_index, last_log_term);

	tupdesc = CreateTemplateTupleDesc(2);
	TupleDescInitEntry(tupdesc, 1, "term", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, 2, "vote_granted", BOOLOID, -1, 0);
	tupdesc = BlessTupleDesc(tupdesc);

	values[0] = Int64GetDatum(group->current_term);
	values[1] = BoolGetDatum(vote_granted);

	result_tuple = heap_form_tuple(tupdesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(result_tuple));
}

/*
 * RaftSerializeEntries
 *		Serialize Raft log entries to a bytea for transport.
 *
 * Simple format: header (count:int32) + per-entry (index:int64, term:int64,
 * cmd_type:char, table_oid:oid, shard_id:int32, sql_len:int32, sql:bytes)
 */
bytea *
RaftSerializeEntries(RaftLogEntry *entries, int count)
{
	StringInfoData buf;
	int			i;

	initStringInfo(&buf);

	/* Header: entry count */
	appendBinaryStringInfo(&buf, (char *) &count, sizeof(int32));

	for (i = 0; i < count; i++)
	{
		int32		sql_len;

		appendBinaryStringInfo(&buf, (char *) &entries[i].log_index,
							   sizeof(int64));
		appendBinaryStringInfo(&buf, (char *) &entries[i].log_term,
							   sizeof(int64));
		appendBinaryStringInfo(&buf, &entries[i].cmd_type, sizeof(char));
		appendBinaryStringInfo(&buf, (char *) &entries[i].table_oid,
							   sizeof(Oid));
		appendBinaryStringInfo(&buf, (char *) &entries[i].shard_id,
							   sizeof(int32));

		sql_len = entries[i].sql_cmd ? strlen(entries[i].sql_cmd) : 0;
		appendBinaryStringInfo(&buf, (char *) &sql_len, sizeof(int32));
		if (sql_len > 0)
			appendBinaryStringInfo(&buf, entries[i].sql_cmd, sql_len);
	}

	{
		bytea	   *result;
		int			total_len = buf.len;

		result = (bytea *) palloc(total_len + VARHDRSZ);
		SET_VARSIZE(result, total_len + VARHDRSZ);
		memcpy(VARDATA(result), buf.data, total_len);
		pfree(buf.data);
		return result;
	}
}

/*
 * RaftDeserializeEntries
 *		Deserialize Raft log entries from bytea.
 */
RaftLogEntry *
RaftDeserializeEntries(bytea *data, int *count)
{
	char	   *ptr;
	int			remaining;
	RaftLogEntry *entries;
	int			i;

	ptr = VARDATA(data);
	remaining = VARSIZE(data) - VARHDRSZ;

	if (remaining < (int) sizeof(int32))
	{
		*count = 0;
		return NULL;
	}

	memcpy(count, ptr, sizeof(int32));
	ptr += sizeof(int32);
	remaining -= sizeof(int32);

	if (*count <= 0)
		return NULL;

	entries = palloc0(*count * sizeof(RaftLogEntry));

	for (i = 0; i < *count && remaining > 0; i++)
	{
		int32		sql_len;

		memcpy(&entries[i].log_index, ptr, sizeof(int64));
		ptr += sizeof(int64);
		remaining -= sizeof(int64);

		memcpy(&entries[i].log_term, ptr, sizeof(int64));
		ptr += sizeof(int64);
		remaining -= sizeof(int64);

		entries[i].cmd_type = *ptr;
		ptr += sizeof(char);
		remaining -= sizeof(char);

		memcpy(&entries[i].table_oid, ptr, sizeof(Oid));
		ptr += sizeof(Oid);
		remaining -= sizeof(Oid);

		memcpy(&entries[i].shard_id, ptr, sizeof(int32));
		ptr += sizeof(int32);
		remaining -= sizeof(int32);

		memcpy(&sql_len, ptr, sizeof(int32));
		ptr += sizeof(int32);
		remaining -= sizeof(int32);

		if (sql_len > 0 && remaining >= sql_len)
		{
			entries[i].sql_cmd = palloc(sql_len + 1);
			memcpy(entries[i].sql_cmd, ptr, sql_len);
			entries[i].sql_cmd[sql_len] = '\0';
			ptr += sql_len;
			remaining -= sql_len;
		}
		else
		{
			entries[i].sql_cmd = NULL;
		}

		entries[i].tuple_data = NULL;
		entries[i].orig_xid = InvalidTransactionId;
	}

	return entries;
}
