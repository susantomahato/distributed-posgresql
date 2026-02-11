/*-------------------------------------------------------------------------
 *
 * shard.h
 *	  Distributed sharding infrastructure for PostgreSQL
 *
 * This file contains data structures and function declarations for
 * managing horizontal sharding across multiple PostgreSQL nodes.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/include/distributed/shard.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SHARD_H
#define SHARD_H

#include "postgres.h"
#include "nodes/pg_list.h"
#include "utils/timestamp.h"

/*
 * Shard node state
 */
typedef enum ShardNodeState
{
	SHARD_NODE_ONLINE,			/* Node is healthy and accepting queries */
	SHARD_NODE_OFFLINE,			/* Node is unreachable */
	SHARD_NODE_DEGRADED			/* Node is reachable but experiencing issues */
} ShardNodeState;

/*
 * Shard state
 */
typedef enum ShardState
{
	SHARD_STATE_ACTIVE,			/* Shard is active and serving queries */
	SHARD_STATE_SPLITTING,		/* Shard is being split */
	SHARD_STATE_MIGRATING		/* Shard data is being migrated */
} ShardState;

/*
 * Shard method
 */
typedef enum ShardMethod
{
	SHARD_METHOD_HASH,			/* Hash-based sharding */
	SHARD_METHOD_RANGE			/* Range-based sharding */
} ShardMethod;

/*
 * ShardNode - represents a PostgreSQL node that can store shards
 */
typedef struct ShardNode
{
	char	   *node_name;		/* Unique node identifier */
	char	   *connection_string;	/* libpq connection string */
	ShardNodeState state;		/* Current node state */
	int32		shard_count;	/* Number of shards on this node */
	TimestampTz last_health_check;	/* Last successful health check */
	TimestampTz created_at;		/* Node registration timestamp */
} ShardNode;

/*
 * ShardMapEntry - represents a single shard and its location
 */
typedef struct ShardMapEntry
{
	int32		shard_id;		/* Unique shard identifier */
	Oid			table_oid;		/* OID of sharded table */
	char	   *node_name;		/* Node where shard resides */
	ShardMethod shard_method;	/* Hash or range sharding */
	
	/* For range sharding */
	Datum		range_min;		/* Minimum key value (inclusive) */
	Datum		range_max;		/* Maximum key value (exclusive) */
	
	/* For hash sharding */
	int32		hash_min;		/* Minimum hash value (inclusive) */
	int32		hash_max;		/* Maximum hash value (exclusive) */
	
	ShardState	state;			/* Current shard state */
	TimestampTz created_at;		/* Shard creation timestamp */
} ShardMapEntry;

/*
 * ShardedTableInfo - metadata about a sharded table
 */
typedef struct ShardedTableInfo
{
	Oid			table_oid;		/* OID of the table */
	List	   *shard_key_attnums;	/* Attribute numbers of shard key columns */
	ShardMethod shard_method;	/* Hash or range sharding */
	int32		shard_count;	/* Total number of shards */
	TimestampTz created_at;		/* Table sharding timestamp */
} ShardedTableInfo;

/*
 * Memory management functions
 */
extern ShardNode *MakeShardNode(const char *node_name,
								const char *connection_string,
								ShardNodeState state);
extern ShardMapEntry *MakeShardMapEntry(int32 shard_id,
										Oid table_oid,
										const char *node_name,
										ShardMethod method);
extern ShardedTableInfo *MakeShardedTableInfo(Oid table_oid,
											   List *shard_key_attnums,
											   ShardMethod method,
											   int32 shard_count);

extern void FreeShardNode(ShardNode *node);
extern void FreeShardMapEntry(ShardMapEntry *entry);
extern void FreeShardedTableInfo(ShardedTableInfo *info);

/*
 * Shard map lookup functions
 */
extern ShardMapEntry *GetShardForKey(Oid table_oid, Datum shard_key);
extern List *GetAllShardsForTable(Oid table_oid);
extern ShardMapEntry *GetShardById(int32 shard_id);
extern List *GetShardsForNode(const char *node_name);

/*
 * Shard node management
 */
extern void RegisterShardNode(const char *node_name,
							  const char *connection_string);
extern void RemoveShardNode(const char *node_name);
extern ShardNode *GetShardNode(const char *node_name);
extern List *GetAllShardNodes(void);
extern void UpdateShardNodeState(const char *node_name, ShardNodeState state);

/*
 * Sharded table management
 */
extern bool IsShardedTable(Oid table_oid);
extern ShardedTableInfo *GetShardedTableInfo(Oid table_oid);
extern void RegisterShardedTable(Oid table_oid,
								 List *shard_key_attnums,
								 ShardMethod method,
								 int32 shard_count);

/*
 * Shard map updates
 */
extern void AddShardMapEntry(ShardMapEntry *entry);
extern void UpdateShardMapEntry(ShardMapEntry *entry);
extern void RemoveShardMapEntry(int32 shard_id);
extern void UpdateShardState(int32 shard_id, ShardState state);

/*
 * Hash function for shard key
 */
extern int32 ComputeShardHash(Datum key, Oid key_type);

/*
 * Utility functions
 */
extern bool ShardIsActive(int32 shard_id);
extern bool ShardNodeIsOnline(const char *node_name);
extern int32 GetShardCount(Oid table_oid);

/*
 * Helper to get the default hash opclass for a type.
 * Wraps GetDefaultOpClass(type, HASH_AM_OID).
 */
extern Oid get_opclass_for_type(Oid type_oid);

#endif							/* SHARD_H */
