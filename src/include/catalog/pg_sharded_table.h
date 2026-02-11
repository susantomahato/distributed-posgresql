/*-------------------------------------------------------------------------
 *
 * pg_sharded_table.h
 *	  definition of the "sharded table" system catalog (pg_sharded_table)
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/include/catalog/pg_sharded_table.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_SHARDED_TABLE_H
#define PG_SHARDED_TABLE_H

#include "catalog/genbki.h"
#include "datatype/timestamp.h"
#include "catalog/pg_sharded_table_d.h"

/* ----------------
 *		pg_sharded_table definition.  cpp turns this into
 *		typedef struct FormData_pg_sharded_table
 * ----------------
 */
CATALOG(pg_sharded_table,9010,ShardedTableRelationId)
{
	/* OID of the sharded table */
	Oid			relid BKI_LOOKUP(pg_class);

	/* shard key column names (array) */
	text		shardkey[1] BKI_FORCE_NOT_NULL;

	/* shard method: 'h' = hash, 'r' = range */
	char		shardmethod BKI_FORCE_NOT_NULL;

	/* number of shards for this table */
	int32		shardcount BKI_DEFAULT(1);

	/* table creation timestamp */
	TimestampTz	createdat BKI_DEFAULT(now);
} FormData_pg_sharded_table;

/* ----------------
 *		Form_pg_sharded_table corresponds to a pointer to a tuple with
 *		the format of pg_sharded_table relation.
 * ----------------
 */
typedef FormData_pg_sharded_table *Form_pg_sharded_table;

DECLARE_TOAST(pg_sharded_table, 9111, 9112);

DECLARE_UNIQUE_INDEX_PKEY(pg_sharded_table_relid_index, 9113, ShardedTableRelidIndexId, pg_sharded_table, btree(relid oid_ops));

MAKE_SYSCACHE(SHARDEDTABLE, pg_sharded_table_relid_index, 4);

/*
 * Shard method constants
 */
#define SHARD_METHOD_CHAR_HASH		'h'
#define SHARD_METHOD_CHAR_RANGE		'r'

extern bool is_sharded_table(Oid relid);
extern char get_shard_method(Oid relid);

#endif							/* PG_SHARDED_TABLE_H */
