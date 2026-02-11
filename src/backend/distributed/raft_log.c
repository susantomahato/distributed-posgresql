/*-------------------------------------------------------------------------
 *
 * raft_log.c
 *	  Raft log storage implementation
 *
 * The Raft log for each group is stored in a local table named
 * pg_dist_raft_log_<group_id>. This provides durable storage that
 * survives process restarts.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/distributed/raft_log.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distributed/raft_log.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

/*
 * RaftLogCreateTable
 *		Create the log table for a Raft group.
 *
 * Creates: pg_dist_raft_log_<gid>(
 *     log_index int8 PRIMARY KEY,
 *     log_term int8,
 *     cmd_type char,
 *     table_oid oid,
 *     shard_id int4,
 *     sql_cmd text,
 *     tuple_data bytea,
 *     orig_xid xid
 * )
 */
void
RaftLogCreateTable(int raft_group_id)
{
	StringInfoData buf;
	int			ret;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "CREATE TABLE IF NOT EXISTS pg_dist_raft_log_%d ("
					 "log_index int8 PRIMARY KEY, "
					 "log_term int8 NOT NULL, "
					 "cmd_type char NOT NULL DEFAULT 'n', "
					 "table_oid oid, "
					 "shard_id int4, "
					 "sql_cmd text, "
					 "tuple_data bytea, "
					 "orig_xid xid"
					 ")",
					 raft_group_id);

	SPI_connect();
	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_UTILITY)
		elog(WARNING, "raft_log: failed to create log table for group %d",
			 raft_group_id);
	SPI_finish();

	pfree(buf.data);
}

/*
 * RaftLogDropTable
 *		Drop the log table for a Raft group.
 */
void
RaftLogDropTable(int raft_group_id)
{
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "DROP TABLE IF EXISTS pg_dist_raft_log_%d",
					 raft_group_id);

	SPI_connect();
	SPI_execute(buf.data, false, 0);
	SPI_finish();

	pfree(buf.data);
}

/*
 * RaftLogAppend
 *		Append an entry to the Raft log.
 */
void
RaftLogAppend(int raft_group_id, RaftLogEntry *entry)
{
	StringInfoData buf;
	int			ret;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "INSERT INTO pg_dist_raft_log_%d "
					 "(log_index, log_term, cmd_type, table_oid, "
					 "shard_id, sql_cmd) "
					 "VALUES (%lld, %lld, '%c', %u, %d, ",
					 raft_group_id,
					 (long long) entry->log_index,
					 (long long) entry->log_term,
					 entry->cmd_type,
					 entry->table_oid,
					 entry->shard_id);

	if (entry->sql_cmd != NULL)
		appendStringInfo(&buf, "'%s'", entry->sql_cmd);
	else
		appendStringInfoString(&buf, "NULL");

	appendStringInfo(&buf,
					 ") ON CONFLICT (log_index) DO UPDATE SET "
					 "log_term = EXCLUDED.log_term, "
					 "cmd_type = EXCLUDED.cmd_type, "
					 "sql_cmd = EXCLUDED.sql_cmd");

	SPI_connect();
	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_INSERT)
		elog(WARNING, "raft_log: failed to append entry (group %d, "
			 "index %lld)",
			 raft_group_id, (long long) entry->log_index);
	SPI_finish();

	pfree(buf.data);
}

/*
 * RaftLogRead
 *		Read a single log entry by index.
 *
 * Returns NULL if not found. Caller must free with RaftLogEntryFree().
 */
RaftLogEntry *
RaftLogRead(int raft_group_id, int64 log_index)
{
	StringInfoData buf;
	RaftLogEntry *entry = NULL;
	int			ret;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT log_index, log_term, cmd_type, table_oid, "
					 "shard_id, sql_cmd "
					 "FROM pg_dist_raft_log_%d "
					 "WHERE log_index = %lld",
					 raft_group_id, (long long) log_index);

	SPI_connect();
	ret = SPI_execute(buf.data, true, 1);

	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		TupleDesc	tupdesc = SPI_tuptable->tupdesc;
		HeapTuple	tuple = SPI_tuptable->vals[0];
		bool		isnull;
		MemoryContext oldcxt;

		/* Allocate in caller's memory context */
		oldcxt = MemoryContextSwitchTo(TopMemoryContext);
		entry = palloc0(sizeof(RaftLogEntry));
		MemoryContextSwitchTo(oldcxt);

		entry->log_index = DatumGetInt64(
			SPI_getbinval(tuple, tupdesc, 1, &isnull));
		entry->log_term = DatumGetInt64(
			SPI_getbinval(tuple, tupdesc, 2, &isnull));

		{
			Datum		d = SPI_getbinval(tuple, tupdesc, 3, &isnull);

			entry->cmd_type = isnull ? 'n' : DatumGetChar(d);
		}

		{
			Datum		d = SPI_getbinval(tuple, tupdesc, 4, &isnull);

			entry->table_oid = isnull ? InvalidOid : DatumGetObjectId(d);
		}

		{
			Datum		d = SPI_getbinval(tuple, tupdesc, 5, &isnull);

			entry->shard_id = isnull ? 0 : DatumGetInt32(d);
		}

		{
			Datum		d = SPI_getbinval(tuple, tupdesc, 6, &isnull);

			if (!isnull)
			{
				oldcxt = MemoryContextSwitchTo(TopMemoryContext);
				entry->sql_cmd = TextDatumGetCString(d);
				MemoryContextSwitchTo(oldcxt);
			}
			else
			{
				entry->sql_cmd = NULL;
			}
		}

		entry->tuple_data = NULL;
		entry->orig_xid = InvalidTransactionId;
	}

	SPI_finish();
	pfree(buf.data);

	return entry;
}

/*
 * RaftLogGetLast
 *		Get the index and term of the last log entry.
 */
void
RaftLogGetLast(int raft_group_id, int64 *last_index, int64 *last_term)
{
	StringInfoData buf;
	int			ret;

	*last_index = 0;
	*last_term = 0;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT log_index, log_term "
					 "FROM pg_dist_raft_log_%d "
					 "ORDER BY log_index DESC LIMIT 1",
					 raft_group_id);

	SPI_connect();
	ret = SPI_execute(buf.data, true, 1);

	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		TupleDesc	tupdesc = SPI_tuptable->tupdesc;
		HeapTuple	tuple = SPI_tuptable->vals[0];
		bool		isnull;

		*last_index = DatumGetInt64(
			SPI_getbinval(tuple, tupdesc, 1, &isnull));
		*last_term = DatumGetInt64(
			SPI_getbinval(tuple, tupdesc, 2, &isnull));
	}

	SPI_finish();
	pfree(buf.data);
}

/*
 * RaftLogTruncateFrom
 *		Delete all log entries from a given index onward.
 */
void
RaftLogTruncateFrom(int raft_group_id, int64 from_index)
{
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "DELETE FROM pg_dist_raft_log_%d "
					 "WHERE log_index >= %lld",
					 raft_group_id, (long long) from_index);

	SPI_connect();
	SPI_execute(buf.data, false, 0);
	SPI_finish();

	pfree(buf.data);
}

/*
 * RaftLogCompact
 *		Delete all log entries up to and including a given index.
 */
void
RaftLogCompact(int raft_group_id, int64 up_to_index)
{
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "DELETE FROM pg_dist_raft_log_%d "
					 "WHERE log_index <= %lld",
					 raft_group_id, (long long) up_to_index);

	SPI_connect();
	SPI_execute(buf.data, false, 0);
	SPI_finish();

	pfree(buf.data);
}

/*
 * RaftLogReadRange
 *		Read a range of log entries.
 *
 * Returns a List of RaftLogEntry*.
 */
List *
RaftLogReadRange(int raft_group_id, int64 from_index, int64 to_index)
{
	StringInfoData buf;
	List	   *result = NIL;
	int			ret;

	if (from_index > to_index)
		return NIL;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT log_index, log_term, cmd_type, table_oid, "
					 "shard_id, sql_cmd "
					 "FROM pg_dist_raft_log_%d "
					 "WHERE log_index >= %lld AND log_index <= %lld "
					 "ORDER BY log_index",
					 raft_group_id,
					 (long long) from_index,
					 (long long) to_index);

	SPI_connect();
	ret = SPI_execute(buf.data, true, 0);

	if (ret == SPI_OK_SELECT)
	{
		MemoryContext oldcxt;

		for (uint64 i = 0; i < SPI_processed; i++)
		{
			TupleDesc	tupdesc = SPI_tuptable->tupdesc;
			HeapTuple	tuple = SPI_tuptable->vals[i];
			RaftLogEntry *entry;
			bool		isnull;

			oldcxt = MemoryContextSwitchTo(TopMemoryContext);
			entry = palloc0(sizeof(RaftLogEntry));
			MemoryContextSwitchTo(oldcxt);

			entry->log_index = DatumGetInt64(
				SPI_getbinval(tuple, tupdesc, 1, &isnull));
			entry->log_term = DatumGetInt64(
				SPI_getbinval(tuple, tupdesc, 2, &isnull));
			entry->cmd_type = DatumGetChar(
				SPI_getbinval(tuple, tupdesc, 3, &isnull));

			{
				Datum		d = SPI_getbinval(tuple, tupdesc, 4, &isnull);

				entry->table_oid = isnull ? InvalidOid : DatumGetObjectId(d);
			}

			{
				Datum		d = SPI_getbinval(tuple, tupdesc, 5, &isnull);

				entry->shard_id = isnull ? 0 : DatumGetInt32(d);
			}

			{
				Datum		d = SPI_getbinval(tuple, tupdesc, 6, &isnull);

				if (!isnull)
				{
					oldcxt = MemoryContextSwitchTo(TopMemoryContext);
					entry->sql_cmd = TextDatumGetCString(d);
					MemoryContextSwitchTo(oldcxt);
				}
			}

			oldcxt = MemoryContextSwitchTo(TopMemoryContext);
			result = lappend(result, entry);
			MemoryContextSwitchTo(oldcxt);
		}
	}

	SPI_finish();
	pfree(buf.data);

	return result;
}

/*
 * RaftLogAppendBatch
 *		Append multiple entries to the log.
 */
void
RaftLogAppendBatch(int raft_group_id, List *entries)
{
	ListCell   *lc;

	foreach(lc, entries)
	{
		RaftLogEntry *entry = (RaftLogEntry *) lfirst(lc);

		RaftLogAppend(raft_group_id, entry);
	}
}

/*
 * RaftLogEntryFree
 *		Free a RaftLogEntry structure.
 */
void
RaftLogEntryFree(RaftLogEntry *entry)
{
	if (entry == NULL)
		return;

	if (entry->sql_cmd)
		pfree(entry->sql_cmd);
	if (entry->tuple_data)
		pfree(entry->tuple_data);
	pfree(entry);
}
