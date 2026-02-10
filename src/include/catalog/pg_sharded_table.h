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
#include "catalog/pg_sharded_table_d.h"

/* ----------------
 *		pg_sharded_table definition.  cpp turns this into
 *		typedef struct FormData_pg_sharded_table
 * ----------------
 */
CATALOG(pg_sharded_table,9010,ShardedTableRelationId)
{
	/* OID of the sharded table */
	Oid			tableoid BKI_LOOKUP(pg_class);

	/* shard key column names (array) */
	text		shardkey[1] BKI_FORCE_NOT_NULL;

	/* shard method: 'h' = hash, 'r' = range */
	char		shardmethod BKI_FORCE_NOT_NULL;

	/* number of shards for this table */
	int32		shardcount BKI_DEFAULT(1);

	/* table creation timestamp */
	timestamptz	createdat BKI_DEFAULT(now);
} FormData_pg_sharded_table;

/* ----------------
 *		Form_pg_sharded_table corresponds to a pointer to a tuple with
 *		the format of pg_sharded_table relation.
 * ----------------
 */
typedef FormData_pg_sharded_table *Form_pg_sharded_table;

DECLARE_TOAST(pg_sharded_table, 9011, 9012);

DECLARE_UNIQUE_INDEX_PKEY(pg_sharded_table_tableoid_index, 9013, ShardedTableTableoidIndexId, pg_sharded_table, btree(tableoid oid_ops));

MAKE_SYSCACHE(SHARDEDTABLE, pg_sharded_table_tableoid_index, 4);

/*
 * Shard method constants
 */
#define SHARD_METHOD_HASH		'h'
#define SHARD_METHOD_RANGE		'r'

extern bool is_sharded_table(Oid tableoid);
extern char get_shard_method(Oid tableoid);

#endif							/* PG_SHARDED_TABLE_H */
