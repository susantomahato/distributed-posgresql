/*-------------------------------------------------------------------------
 *
 * dist_commands.h
 *	  DDL command declarations for the distributed subsystem
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/include/distributed/dist_commands.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DIST_COMMANDS_H
#define DIST_COMMANDS_H

#include "fmgr.h"

/* SQL-callable functions */
extern Datum create_distributed_table(PG_FUNCTION_ARGS);
extern Datum dist_rebalance_shards(PG_FUNCTION_ARGS);
extern Datum dist_shard_status(PG_FUNCTION_ARGS);
extern Datum dist_raft_status(PG_FUNCTION_ARGS);

/* Internal helpers */
extern void CreateDistributedTableInternal(Oid table_oid,
										   const char *shard_key_col,
										   int shard_count,
										   int replication_factor);

#endif							/* DIST_COMMANDS_H */
