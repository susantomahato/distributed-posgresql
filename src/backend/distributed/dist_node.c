/*-------------------------------------------------------------------------
 *
 * dist_node.c
 *	  Node management for the distributed subsystem
 *
 * Provides SQL-callable functions for adding and removing nodes from
 * the cluster, and internal helpers for node lookups.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/distributed/dist_node.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distributed/dist_guc.h"
#include "distributed/dist_node.h"
#include "distributed/dist_shmem.h"
#include "distributed/rebalancer.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_shard_node.h"
#include "distributed/shard.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

PG_FUNCTION_INFO_V1(dist_add_node);
PG_FUNCTION_INFO_V1(dist_remove_node);
PG_FUNCTION_INFO_V1(dist_node_status);

/*
 * dist_add_node(node_name text, conninfo text) — SQL function
 *
 * Registers a new node in the cluster and triggers rebalancing.
 */
Datum
dist_add_node(PG_FUNCTION_ARGS)
{
	char	   *node_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *conninfo = text_to_cstring(PG_GETARG_TEXT_PP(1));

	AddDistNode(node_name, conninfo);

	PG_RETURN_VOID();
}

/*
 * dist_remove_node(node_name text) — SQL function
 */
Datum
dist_remove_node(PG_FUNCTION_ARGS)
{
	char	   *node_name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	RemoveDistNode(node_name);

	PG_RETURN_VOID();
}

/*
 * dist_node_status() — SQL function returning set of node info
 */
Datum
dist_node_status(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Relation	rel;
	SysScanDesc scan;
	HeapTuple	tuple;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/* Scan pg_shard_node */
	rel = table_open(ShardNodeRelationId, AccessShareLock);
	scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_shard_node form = (Form_pg_shard_node) GETSTRUCT(tuple);
		TupleDesc	reldesc = RelationGetDescr(rel);
		Datum		values[4];
		bool		nulls[4] = {false};
		Datum		datum;
		bool		isnull;

		/* nodename is fixed-length (NameData), safe to access via form */
		values[0] = NameGetDatum(&form->nodename);

		/*
		 * Fields after the variable-length nodeconnstr (text) cannot be
		 * accessed via the Form_ pointer — use heap_getattr instead.
		 */
		datum = heap_getattr(tuple, Anum_pg_shard_node_nodestate,
							 reldesc, &isnull);
		values[1] = datum;
		nulls[1] = isnull;

		datum = heap_getattr(tuple, Anum_pg_shard_node_shardcount,
							 reldesc, &isnull);
		values[2] = datum;
		nulls[2] = isnull;

		datum = heap_getattr(tuple, Anum_pg_shard_node_nodeconnstr,
							 reldesc, &isnull);
		values[3] = datum;
		nulls[3] = isnull;

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	return (Datum) 0;
}

/*
 * AddDistNode
 *		Register a new node in pg_shard_node catalog.
 */
void
AddDistNode(const char *node_name, const char *conninfo)
{
	Relation	rel;
	HeapTuple	tuple;
	Datum		values[Natts_pg_shard_node];
	bool		nulls[Natts_pg_shard_node];

	/* Check if node already exists */
	if (ShardNodeIsOnline(node_name))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("node \"%s\" already exists", node_name)));

	/* Insert into pg_shard_node */
	rel = table_open(ShardNodeRelationId, RowExclusiveLock);

	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_shard_node_nodename - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(node_name));
	values[Anum_pg_shard_node_nodeconnstr - 1] =
		CStringGetTextDatum(conninfo);
	values[Anum_pg_shard_node_nodestate - 1] =
		CharGetDatum(SHARD_NODE_STATE_ONLINE);
	values[Anum_pg_shard_node_shardcount - 1] = Int32GetDatum(0);
	values[Anum_pg_shard_node_lasthealthcheck - 1] =
		TimestampTzGetDatum(GetCurrentTimestamp());
	values[Anum_pg_shard_node_createdat - 1] =
		TimestampTzGetDatum(GetCurrentTimestamp());

	tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
	CatalogTupleInsert(rel, tuple);
	heap_freetuple(tuple);

	table_close(rel, RowExclusiveLock);

	/* Update shared memory health tracking */
	DistShmemUpdateNodeHealth(node_name, GetCurrentTimestamp());

	CommandCounterIncrement();

	elog(LOG, "distributed: added node \"%s\"", node_name);
}

/*
 * RemoveDistNode
 *		Remove a node from pg_shard_node catalog.
 */
void
RemoveDistNode(const char *node_name)
{
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData skey[1];
	HeapTuple	tuple;

	rel = table_open(ShardNodeRelationId, RowExclusiveLock);

	ScanKeyInit(&skey[0],
				Anum_pg_shard_node_nodename,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(node_name));

	scan = systable_beginscan(rel, ShardNodeNodenameIndexId,
							  true, NULL, 1, skey);

	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("node \"%s\" does not exist", node_name)));

	CatalogTupleDelete(rel, &tuple->t_self);

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	CommandCounterIncrement();

	elog(LOG, "distributed: removed node \"%s\"", node_name);
}

/*
 * GetAllDistNodes
 *		Return a list of all node names (List of char*).
 */
List *
GetAllDistNodes(void)
{
	Relation	rel;
	SysScanDesc scan;
	HeapTuple	tuple;
	List	   *result = NIL;

	rel = table_open(ShardNodeRelationId, AccessShareLock);
	scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_shard_node form = (Form_pg_shard_node) GETSTRUCT(tuple);
		Datum		state_datum;
		bool		state_isnull;
		char		nodestate;

		/*
		 * nodestate is after the variable-length nodeconnstr field,
		 * so we must use heap_getattr instead of form->nodestate.
		 */
		state_datum = heap_getattr(tuple, Anum_pg_shard_node_nodestate,
								   RelationGetDescr(rel), &state_isnull);
		nodestate = state_isnull ? '\0' : DatumGetChar(state_datum);

		if (nodestate == SHARD_NODE_STATE_ONLINE)
			result = lappend(result, pstrdup(NameStr(form->nodename)));
	}

	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	return result;
}

/*
 * GetNodeConnInfo
 *		Look up connection info string for a node.
 *
 * Returns a palloc'd string, or NULL if node not found.
 */
char *
GetNodeConnInfo(const char *node_name)
{
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData skey[1];
	HeapTuple	tuple;
	char	   *result = NULL;

	rel = table_open(ShardNodeRelationId, AccessShareLock);

	ScanKeyInit(&skey[0],
				Anum_pg_shard_node_nodename,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(node_name));

	scan = systable_beginscan(rel, ShardNodeNodenameIndexId,
							  true, NULL, 1, skey);

	tuple = systable_getnext(scan);
	if (HeapTupleIsValid(tuple))
	{
		Datum		datum;
		bool		isnull;

		datum = heap_getattr(tuple, Anum_pg_shard_node_nodeconnstr,
							 RelationGetDescr(rel), &isnull);
		if (!isnull)
			result = TextDatumGetCString(datum);
	}

	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	return result;
}

/*
 * IsLocalNode
 *		Check if a node name matches this node.
 */
bool
IsLocalNode(const char *node_name)
{
	if (dist_node_name == NULL || dist_node_name[0] == '\0')
		return false;
	return strcmp(node_name, dist_node_name) == 0;
}
