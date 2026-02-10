/*-------------------------------------------------------------------------
 *
 * pg_shard_map.h
 *	  definition of the "shard map" system catalog (pg_shard_map)
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/include/catalog/pg_shard_map.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_SHARD_MAP_H
#define PG_SHARD_MAP_H

#include "catalog/genbki.h"
#include "catalog/pg_shard_map_d.h"

/* ----------------
 *		pg_shard_map definition.  cpp turns this into
 *		typedef struct FormData_pg_shard_map
 * ----------------
 */
CATALOG(pg_shard_map,9020,ShardMapRelationId)
{
	/* unique shard identifier */
	int32		shardid;

	/* OID of the sharded table */
	Oid			tableoid BKI_LOOKUP(pg_sharded_table);

	/* node name where shard resides */
	NameData	nodename BKI_LOOKUP_OPT(pg_shard_node);

	/* shard method: 'h' = hash, 'r' = range */
	char		shardmethod BKI_FORCE_NOT_NULL;

	/* minimum range value (for range sharding) */
	text		rangemin;

	/* maximum range value (for range sharding) */
	text		rangemax;

	/* minimum hash value (for hash sharding) */
	int32		hashmin;

	/* maximum hash value (for hash sharding) */
	int32		hashmax;

	/* shard state: 'a' = active, 's' = splitting, 'm' = migrating */
	char		shardstate BKI_DEFAULT(a);

	/* shard creation timestamp */
	timestamptz	createdat BKI_DEFAULT(now);
} FormData_pg_shard_map;

/* ----------------
 *		Form_pg_shard_map corresponds to a pointer to a tuple with
 *		the format of pg_shard_map relation.
 * ----------------
 */
typedef FormData_pg_shard_map *Form_pg_shard_map;

DECLARE_TOAST(pg_shard_map, 9021, 9022);

DECLARE_UNIQUE_INDEX_PKEY(pg_shard_map_shardid_index, 9023, ShardMapShardidIndexId, pg_shard_map, btree(shardid int4_ops));
DECLARE_INDEX(pg_shard_map_tableoid_index, 9024, ShardMapTableoidIndexId, pg_shard_map, btree(tableoid oid_ops));
DECLARE_INDEX(pg_shard_map_nodename_index, 9025, ShardMapNodenameIndexId, pg_shard_map, btree(nodename name_ops));

MAKE_SYSCACHE(SHARDMAPID, pg_shard_map_shardid_index, 4);

/*
 * Shard state constants
 */
#define SHARD_STATE_ACTIVE		'a'
#define SHARD_STATE_SPLITTING	's'
#define SHARD_STATE_MIGRATING	'm'

extern int32 get_shard_for_key(Oid tableoid, Datum shardkey);
extern List *get_all_shards_for_table(Oid tableoid);
extern bool shard_is_active(int32 shardid);

#endif							/* PG_SHARD_MAP_H */
