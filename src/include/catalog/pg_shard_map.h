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
#include "datatype/timestamp.h"
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
	Oid			relid BKI_LOOKUP(pg_sharded_table);

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
	TimestampTz	createdat BKI_DEFAULT(now);
} FormData_pg_shard_map;

/* ----------------
 *		Form_pg_shard_map corresponds to a pointer to a tuple with
 *		the format of pg_shard_map relation.
 * ----------------
 */
typedef FormData_pg_shard_map *Form_pg_shard_map;

DECLARE_TOAST(pg_shard_map, 9121, 9122);

DECLARE_UNIQUE_INDEX_PKEY(pg_shard_map_shardid_index, 9123, ShardMapShardidIndexId, pg_shard_map, btree(shardid int4_ops));
DECLARE_INDEX(pg_shard_map_relid_index, 9124, ShardMapRelidIndexId, pg_shard_map, btree(relid oid_ops));
DECLARE_INDEX(pg_shard_map_nodename_index, 9125, ShardMapNodenameIndexId, pg_shard_map, btree(nodename name_ops));

MAKE_SYSCACHE(SHARDMAPID, pg_shard_map_shardid_index, 4);

/*
 * Shard state constants
 */
#define SHARD_STATE_CHAR_ACTIVE		'a'
#define SHARD_STATE_CHAR_SPLITTING	's'
#define SHARD_STATE_CHAR_MIGRATING	'm'

extern int32 get_shard_for_key(Oid relid, Datum shardkey);
extern List *get_all_shards_for_table(Oid relid);
extern bool shard_is_active(int32 shardid);

#endif							/* PG_SHARD_MAP_H */
