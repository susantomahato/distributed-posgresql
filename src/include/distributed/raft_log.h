/*-------------------------------------------------------------------------
 *
 * raft_log.h
 *	  Raft log storage API
 *
 * The Raft log is stored in per-group local tables named
 * pg_dist_raft_log_<group_id>. This module handles creation,
 * appending, reading, and truncation of log entries.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/include/distributed/raft_log.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RAFT_LOG_H
#define RAFT_LOG_H

#include "distributed/raft.h"
#include "nodes/pg_list.h"

/* Log table management */
extern void RaftLogCreateTable(int raft_group_id);
extern void RaftLogDropTable(int raft_group_id);

/* Log entry operations */
extern void RaftLogAppend(int raft_group_id, RaftLogEntry *entry);
extern RaftLogEntry *RaftLogRead(int raft_group_id, int64 log_index);
extern void RaftLogGetLast(int raft_group_id, int64 *last_index,
						   int64 *last_term);
extern void RaftLogTruncateFrom(int raft_group_id, int64 from_index);
extern void RaftLogCompact(int raft_group_id, int64 up_to_index);

/* Batch operations */
extern List *RaftLogReadRange(int raft_group_id, int64 from_index,
							  int64 to_index);
extern void RaftLogAppendBatch(int raft_group_id, List *entries);

/* Free a log entry */
extern void RaftLogEntryFree(RaftLogEntry *entry);

#endif							/* RAFT_LOG_H */
