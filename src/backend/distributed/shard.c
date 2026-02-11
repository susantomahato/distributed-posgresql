/*-------------------------------------------------------------------------
 *
 * shard.c
 *	  Shard map manager implementation
 *
 * This file implements the core shard map management functionality including
 * shard lookup, node registration, and metadata management.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/distributed/shard.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/hash.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/indexing.h"
#include "catalog/pg_shard_map.h"
#include "catalog/pg_shard_node.h"
#include "catalog/pg_sharded_table.h"
#include "distributed/shard.h"
#include "commands/defrem.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/*
 * Hash function constants
 */
#define SHARD_HASH_SEED		0x5CA1AB1E	/* Seed for hash function */
#define HASH_SPACE_SIZE		0xFFFFFFFF	/* 32-bit hash space */

/*
 * MakeShardNode
 *		Create a new ShardNode structure
 */
ShardNode *
MakeShardNode(const char *node_name,
			  const char *connection_string,
			  ShardNodeState state)
{
	ShardNode  *node;

	Assert(node_name != NULL);
	Assert(connection_string != NULL);

	node = (ShardNode *) palloc(sizeof(ShardNode));
	node->node_name = pstrdup(node_name);
	node->connection_string = pstrdup(connection_string);
	node->state = state;
	node->shard_count = 0;
	node->last_health_check = GetCurrentTimestamp();
	node->created_at = GetCurrentTimestamp();

	return node;
}

/*
 * MakeShardMapEntry
 *		Create a new ShardMapEntry structure
 */
ShardMapEntry *
MakeShardMapEntry(int32 shard_id,
				  Oid table_oid,
				  const char *node_name,
				  ShardMethod method)
{
	ShardMapEntry *entry;

	Assert(node_name != NULL);
	Assert(OidIsValid(table_oid));

	entry = (ShardMapEntry *) palloc0(sizeof(ShardMapEntry));
	entry->shard_id = shard_id;
	entry->table_oid = table_oid;
	entry->node_name = pstrdup(node_name);
	entry->shard_method = method;
	entry->state = SHARD_STATE_ACTIVE;
	entry->created_at = GetCurrentTimestamp();

	return entry;
}

/*
 * MakeShardedTableInfo
 *		Create a new ShardedTableInfo structure
 */
ShardedTableInfo *
MakeShardedTableInfo(Oid table_oid,
					 List *shard_key_attnums,
					 ShardMethod method,
					 int32 shard_count)
{
	ShardedTableInfo *info;

	Assert(OidIsValid(table_oid));
	Assert(shard_key_attnums != NIL);
	Assert(shard_count > 0);

	info = (ShardedTableInfo *) palloc(sizeof(ShardedTableInfo));
	info->table_oid = table_oid;
	info->shard_key_attnums = list_copy(shard_key_attnums);
	info->shard_method = method;
	info->shard_count = shard_count;
	info->created_at = GetCurrentTimestamp();

	return info;
}

/*
 * FreeShardNode
 *		Free a ShardNode structure
 */
void
FreeShardNode(ShardNode *node)
{
	if (node == NULL)
		return;

	if (node->node_name)
		pfree(node->node_name);
	if (node->connection_string)
		pfree(node->connection_string);
	pfree(node);
}

/*
 * FreeShardMapEntry
 *		Free a ShardMapEntry structure
 */
void
FreeShardMapEntry(ShardMapEntry *entry)
{
	if (entry == NULL)
		return;

	if (entry->node_name)
		pfree(entry->node_name);
	pfree(entry);
}

/*
 * FreeShardedTableInfo
 *		Free a ShardedTableInfo structure
 */
void
FreeShardedTableInfo(ShardedTableInfo *info)
{
	if (info == NULL)
		return;

	if (info->shard_key_attnums)
		list_free(info->shard_key_attnums);
	pfree(info);
}

/*
 * ComputeShardHash
 *		Compute hash value for a shard key
 *
 * Uses the type's hash function to compute a 32-bit hash value.
 */
int32
ComputeShardHash(Datum key, Oid key_type)
{
	Oid			hash_proc;
	int32		hash_value;

	/* Get the hash function for this type */
	hash_proc = get_opfamily_proc(get_opclass_family(get_opclass_for_type(key_type)),
								   key_type,
								   key_type,
								   HASHSTANDARD_PROC);

	if (!OidIsValid(hash_proc))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("could not find hash function for type %s",
						format_type_be(key_type)),
				 errhint("The shard key type must be hashable.")));

	/* Compute hash value */
	hash_value = DatumGetInt32(OidFunctionCall1(hash_proc, key));

	return hash_value;
}

/*
 * GetShardForKey
 *		Find the shard that should contain a given key value
 *
 * Returns the ShardMapEntry for the shard, or NULL if not found.
 */
ShardMapEntry *
GetShardForKey(Oid table_oid, Datum shard_key)
{
	ShardedTableInfo *table_info;
	Relation	shard_map_rel;
	SysScanDesc scan;
	ScanKeyData skey[1];
	HeapTuple	tuple;
	ShardMapEntry *result = NULL;
	int32		hash_value;

	/* Get sharded table info */
	table_info = GetShardedTableInfo(table_oid);
	if (table_info == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("table with OID %u is not sharded", table_oid)));

	/* Open shard map catalog */
	shard_map_rel = table_open(ShardMapRelationId, AccessShareLock);

	/* Set up scan key for table OID */
	ScanKeyInit(&skey[0],
				Anum_pg_shard_map_relid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(table_oid));

	scan = systable_beginscan(shard_map_rel, ShardMapRelidIndexId,
							   true, NULL, 1, skey);

	if (table_info->shard_method == SHARD_METHOD_HASH)
	{
		/* Compute hash for the key */
		Oid			key_type = get_atttype(table_oid,
										   linitial_int(table_info->shard_key_attnums));
		hash_value = ComputeShardHash(shard_key, key_type);

		/* Find shard whose hash range contains this value */
		while (HeapTupleIsValid(tuple = systable_getnext(scan)))
		{
			Form_pg_shard_map form = (Form_pg_shard_map) GETSTRUCT(tuple);
			TupleDesc	map_desc = RelationGetDescr(shard_map_rel);
			Datum		d_hashmin,
						d_hashmax,
						d_shardstate;
			bool		n_hashmin,
						n_hashmax,
						n_shardstate;
			int32		cur_hashmin,
						cur_hashmax;
			char		cur_shardstate;

			/*
			 * hashmin, hashmax, shardstate are after variable-length
			 * rangemin/rangemax text fields — must use heap_getattr.
			 */
			d_hashmin = heap_getattr(tuple, Anum_pg_shard_map_hashmin,
									 map_desc, &n_hashmin);
			d_hashmax = heap_getattr(tuple, Anum_pg_shard_map_hashmax,
									 map_desc, &n_hashmax);
			d_shardstate = heap_getattr(tuple, Anum_pg_shard_map_shardstate,
										map_desc, &n_shardstate);

			cur_hashmin = n_hashmin ? 0 : DatumGetInt32(d_hashmin);
			cur_hashmax = n_hashmax ? 0 : DatumGetInt32(d_hashmax);
			cur_shardstate = n_shardstate ? '\0' : DatumGetChar(d_shardstate);

			/* shardmethod is before varlena fields, safe via form */
			if (form->shardmethod == SHARD_METHOD_CHAR_HASH &&
				hash_value >= cur_hashmin &&
				hash_value < cur_hashmax)
			{
				result = MakeShardMapEntry(form->shardid,
										   form->relid,
										   NameStr(form->nodename),
										   SHARD_METHOD_HASH);
				result->hash_min = cur_hashmin;
				result->hash_max = cur_hashmax;
				result->state = (cur_shardstate == SHARD_STATE_CHAR_ACTIVE) ?
					SHARD_STATE_ACTIVE :
					(cur_shardstate == SHARD_STATE_CHAR_SPLITTING) ?
					SHARD_STATE_SPLITTING : SHARD_STATE_MIGRATING;
				break;
			}
		}
	}
	else
	{
		/* Range-based sharding - compare key with range boundaries */
		/* TODO: Implement range comparison logic */
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("range-based sharding not yet implemented")));
	}

	systable_endscan(scan);
	table_close(shard_map_rel, AccessShareLock);

	FreeShardedTableInfo(table_info);

	return result;
}

/*
 * GetAllShardsForTable
 *		Get all shards for a given table
 *
 * Returns a list of ShardMapEntry structures.
 */
List *
GetAllShardsForTable(Oid table_oid)
{
	Relation	shard_map_rel;
	SysScanDesc scan;
	ScanKeyData skey[1];
	HeapTuple	tuple;
	List	   *result = NIL;

	/* Open shard map catalog */
	shard_map_rel = table_open(ShardMapRelationId, AccessShareLock);

	/* Set up scan key for table OID */
	ScanKeyInit(&skey[0],
				Anum_pg_shard_map_relid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(table_oid));

	scan = systable_beginscan(shard_map_rel, ShardMapRelidIndexId,
							   true, NULL, 1, skey);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_shard_map form = (Form_pg_shard_map) GETSTRUCT(tuple);
		TupleDesc	map_desc = RelationGetDescr(shard_map_rel);
		ShardMapEntry *entry;
		ShardMethod method;
		Datum		d_hashmin,
					d_hashmax,
					d_shardstate;
		bool		n_hashmin,
					n_hashmax,
					n_shardstate;
		char		cur_shardstate;

		/* shardmethod is before varlena fields, safe via form */
		method = (form->shardmethod == SHARD_METHOD_CHAR_HASH) ?
			SHARD_METHOD_HASH : SHARD_METHOD_RANGE;

		entry = MakeShardMapEntry(form->shardid,
								  form->relid,
								  NameStr(form->nodename),
								  method);

		/*
		 * hashmin, hashmax, shardstate are after variable-length
		 * rangemin/rangemax text fields — must use heap_getattr.
		 */
		d_hashmin = heap_getattr(tuple, Anum_pg_shard_map_hashmin,
								 map_desc, &n_hashmin);
		d_hashmax = heap_getattr(tuple, Anum_pg_shard_map_hashmax,
								 map_desc, &n_hashmax);
		d_shardstate = heap_getattr(tuple, Anum_pg_shard_map_shardstate,
									map_desc, &n_shardstate);

		if (method == SHARD_METHOD_HASH)
		{
			entry->hash_min = n_hashmin ? 0 : DatumGetInt32(d_hashmin);
			entry->hash_max = n_hashmax ? 0 : DatumGetInt32(d_hashmax);
		}

		cur_shardstate = n_shardstate ? '\0' : DatumGetChar(d_shardstate);
		entry->state = (cur_shardstate == SHARD_STATE_CHAR_ACTIVE) ?
			SHARD_STATE_ACTIVE :
			(cur_shardstate == SHARD_STATE_CHAR_SPLITTING) ?
			SHARD_STATE_SPLITTING : SHARD_STATE_MIGRATING;

		result = lappend(result, entry);
	}

	systable_endscan(scan);
	table_close(shard_map_rel, AccessShareLock);

	return result;
}

/*
 * IsShardedTable
 *		Check if a table is sharded
 */
bool
IsShardedTable(Oid table_oid)
{
	HeapTuple	tuple;
	bool		result;

	tuple = SearchSysCache1(SHARDEDTABLE, ObjectIdGetDatum(table_oid));
	result = HeapTupleIsValid(tuple);

	if (result)
		ReleaseSysCache(tuple);

	return result;
}

/*
 * GetShardedTableInfo
 *		Get metadata about a sharded table
 */
ShardedTableInfo *
GetShardedTableInfo(Oid table_oid)
{
	HeapTuple	tuple;
	Form_pg_sharded_table form;
	ShardedTableInfo *info;
	Datum		shard_key_datum;
	bool		isnull;
	ArrayType  *shard_key_array;
	int			nelems;
	Datum	   *elems;
	List	   *attnums = NIL;
	int			i;

	tuple = SearchSysCache1(SHARDEDTABLE, ObjectIdGetDatum(table_oid));
	if (!HeapTupleIsValid(tuple))
		return NULL;

	form = (Form_pg_sharded_table) GETSTRUCT(tuple);

	/* Extract shard key column names */
	shard_key_datum = SysCacheGetAttr(SHARDEDTABLE, tuple,
									  Anum_pg_sharded_table_shardkey,
									  &isnull);
	Assert(!isnull);

	shard_key_array = DatumGetArrayTypeP(shard_key_datum);
	deconstruct_array(shard_key_array, TEXTOID, -1, false, TYPALIGN_INT,
					  &elems, NULL, &nelems);

	/* Convert column names to attribute numbers */
	for (i = 0; i < nelems; i++)
	{
		char	   *colname = TextDatumGetCString(elems[i]);
		AttrNumber	attnum = get_attnum(table_oid, colname);

		if (attnum == InvalidAttrNumber)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" does not exist", colname)));

		attnums = lappend_int(attnums, attnum);
	}

	info = MakeShardedTableInfo(table_oid,
								attnums,
								(form->shardmethod == SHARD_METHOD_CHAR_HASH) ?
								SHARD_METHOD_HASH : SHARD_METHOD_RANGE,
								form->shardcount);

	ReleaseSysCache(tuple);

	return info;
}

/*
 * ShardIsActive
 *		Check if a shard is in active state
 */
bool
ShardIsActive(int32 shard_id)
{
	HeapTuple	tuple;
	Form_pg_shard_map form;
	bool		result;

	tuple = SearchSysCache1(SHARDMAPID, Int32GetDatum(shard_id));
	if (!HeapTupleIsValid(tuple))
		return false;

	form = (Form_pg_shard_map) GETSTRUCT(tuple);
	result = (form->shardstate == SHARD_STATE_CHAR_ACTIVE);

	ReleaseSysCache(tuple);

	return result;
}

/*
 * ShardNodeIsOnline
 *		Check if a shard node is online
 */
bool
ShardNodeIsOnline(const char *node_name)
{
	HeapTuple	tuple;
	Datum		datum;
	bool		isnull;
	bool		result;

	tuple = SearchSysCache1(SHARDNODENAME, CStringGetDatum(node_name));
	if (!HeapTupleIsValid(tuple))
		return false;

	/*
	 * nodestate is after the variable-length nodeconnstr field,
	 * so we must use SysCacheGetAttr instead of Form_ access.
	 */
	datum = SysCacheGetAttr(SHARDNODENAME, tuple,
							Anum_pg_shard_node_nodestate, &isnull);
	result = (!isnull && DatumGetChar(datum) == SHARD_NODE_STATE_ONLINE);

	ReleaseSysCache(tuple);

	return result;
}

/*
 * GetShardCount
 *		Get the total number of shards for a table
 */
int32
GetShardCount(Oid table_oid)
{
	HeapTuple	tuple;
	Datum		datum;
	bool		isnull;
	int32		result;

	tuple = SearchSysCache1(SHARDEDTABLE, ObjectIdGetDatum(table_oid));
	if (!HeapTupleIsValid(tuple))
		return 0;

	/* shardcount is after variable-length shardkey — use SysCacheGetAttr */
	datum = SysCacheGetAttr(SHARDEDTABLE, tuple,
							Anum_pg_sharded_table_shardcount, &isnull);
	result = isnull ? 0 : DatumGetInt32(datum);

	ReleaseSysCache(tuple);

	return result;
}

/*
 * get_opclass_for_type
 *		Get the default hash operator class for a given type.
 *
 * This is a convenience wrapper around GetDefaultOpClass().
 */
Oid
get_opclass_for_type(Oid type_oid)
{
	Oid			opclass;

	opclass = GetDefaultOpClass(type_oid, HASH_AM_OID);
	if (!OidIsValid(opclass))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("no default hash operator class for type %s",
						format_type_be(type_oid))));

	return opclass;
}
