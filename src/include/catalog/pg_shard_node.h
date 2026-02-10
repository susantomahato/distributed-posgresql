/*-------------------------------------------------------------------------
 *
 * pg_shard_node.h
 *	  definition of the "shard node" system catalog (pg_shard_node)
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/include/catalog/pg_shard_node.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_SHARD_NODE_H
#define PG_SHARD_NODE_H

#include "catalog/genbki.h"
#include "catalog/pg_shard_node_d.h"

/* ----------------
 *		pg_shard_node definition.  cpp turns this into
 *		typedef struct FormData_pg_shard_node
 * ----------------
 */
CATALOG(pg_shard_node,9000,ShardNodeRelationId)
{
	/* node name (unique identifier) */
	NameData	nodename;

	/* libpq connection string */
	text		nodeconnstr BKI_FORCE_NOT_NULL;

	/* node state: 'o' = online, 'f' = offline, 'd' = degraded */
	char		nodestate BKI_DEFAULT(o);

	/* number of shards on this node */
	int32		shardcount BKI_DEFAULT(0);

	/* last successful health check */
	timestamptz	lasthealthcheck;

	/* node creation timestamp */
	timestamptz	createdat BKI_DEFAULT(now);
} FormData_pg_shard_node;

/* ----------------
 *		Form_pg_shard_node corresponds to a pointer to a tuple with
 *		the format of pg_shard_node relation.
 * ----------------
 */
typedef FormData_pg_shard_node *Form_pg_shard_node;

DECLARE_TOAST(pg_shard_node, 9001, 9002);

DECLARE_UNIQUE_INDEX_PKEY(pg_shard_node_nodename_index, 9003, ShardNodeNodenameIndexId, pg_shard_node, btree(nodename name_ops));

MAKE_SYSCACHE(SHARDNODENAME, pg_shard_node_nodename_index, 4);

/*
 * Node state constants
 */
#define SHARD_NODE_STATE_ONLINE		'o'
#define SHARD_NODE_STATE_OFFLINE	'f'
#define SHARD_NODE_STATE_DEGRADED	'd'

extern Oid	get_shard_node_oid(const char *nodename, bool missing_ok);
extern bool shard_node_is_online(const char *nodename);

#endif							/* PG_SHARD_NODE_H */
