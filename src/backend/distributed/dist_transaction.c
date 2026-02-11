/*-------------------------------------------------------------------------
 *
 * dist_transaction.c
 *	  Two-phase commit coordination for multi-shard writes
 *
 * When a write spans multiple shards, each shard leader PREPAREs
 * the transaction. If all succeed, all are COMMITted. If any fails,
 * all are ABORTed. This uses PostgreSQL's built-in PREPARE TRANSACTION
 * / COMMIT PREPARED mechanism.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/distributed/dist_transaction.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distributed/dist_connection.h"
#include "distributed/dist_guc.h"
#include "distributed/dist_node.h"
#include "distributed/placement.h"
#include "distributed/query_router.h"
#include "distributed/raft.h"
#include "libpq-fe.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

/*
 * DistTransactionParticipant — one shard's participation in a dist txn
 */
typedef struct DistTransactionParticipant
{
	int32		shard_id;
	char		node_name[NAMEDATALEN];
	char		gid[128];		/* global transaction ID */
	bool		prepared;
	bool		committed;
} DistTransactionParticipant;

/*
 * PrepareDistributedTransaction
 *		Coordinate a multi-shard write using 2PC.
 *
 * Steps:
 *   1. Begin transaction on each shard leader
 *   2. Execute the write on each shard
 *   3. PREPARE TRANSACTION on each shard
 *   4. If all succeed: COMMIT PREPARED on all
 *      If any fail: ROLLBACK PREPARED on all that prepared
 */
void
PrepareDistributedTransaction(ShardRouteInfo *route_info,
							  const char *query_string)
{
	DistTransactionParticipant *participants;
	int			num_participants;
	int			i;
	bool		all_prepared = true;
	TransactionId local_xid;
	StringInfoData gid_base;

	Assert(route_info->route_type == ROUTE_SCATTER_GATHER);

	num_participants = route_info->num_shards;
	participants = palloc0(num_participants *
						   sizeof(DistTransactionParticipant));

	/* Generate a unique global transaction ID base */
	local_xid = GetCurrentTransactionId();
	initStringInfo(&gid_base);
	appendStringInfo(&gid_base, "dist_%s_%u",
					 dist_node_name ? dist_node_name : "node",
					 local_xid);

	/* Phase 1: PREPARE */
	for (i = 0; i < num_participants; i++)
	{
		PGresult   *res;
		StringInfoData cmd;

		participants[i].shard_id = route_info->shard_ids[i];
		strlcpy(participants[i].node_name,
				route_info->leader_nodes[i], NAMEDATALEN);
		snprintf(participants[i].gid, sizeof(participants[i].gid),
				 "%s_s%d", gid_base.data, route_info->shard_ids[i]);
		participants[i].prepared = false;
		participants[i].committed = false;

		initStringInfo(&cmd);

		PG_TRY();
		{
			/* Begin transaction */
			res = DistExecSimpleQuery(participants[i].node_name, "BEGIN");
			PQclear(res);

			/* Execute the write */
			res = DistExecSimpleQuery(participants[i].node_name,
									  query_string);
			PQclear(res);

			/* Prepare */
			resetStringInfo(&cmd);
			appendStringInfo(&cmd, "PREPARE TRANSACTION '%s'",
							 participants[i].gid);
			res = DistExecSimpleQuery(participants[i].node_name,
									  cmd.data);
			PQclear(res);

			participants[i].prepared = true;
		}
		PG_CATCH();
		{
			elog(WARNING, "distributed: PREPARE failed on node \"%s\" "
				 "for shard %d",
				 participants[i].node_name,
				 participants[i].shard_id);
			FlushErrorState();
			all_prepared = false;

			/* Try to rollback on this node */
			PG_TRY();
			{
				res = DistExecSimpleQuery(participants[i].node_name,
										  "ROLLBACK");
				PQclear(res);
			}
			PG_CATCH();
			{
				FlushErrorState();
			}
			PG_END_TRY();
		}
		PG_END_TRY();

		pfree(cmd.data);

		if (!all_prepared)
			break;
	}

	/* Phase 2: COMMIT or ABORT */
	if (all_prepared)
	{
		/* All prepared — commit all */
		for (i = 0; i < num_participants; i++)
		{
			StringInfoData cmd;
			PGresult   *res;

			initStringInfo(&cmd);
			appendStringInfo(&cmd, "COMMIT PREPARED '%s'",
							 participants[i].gid);

			PG_TRY();
			{
				res = DistExecSimpleQuery(participants[i].node_name,
										  cmd.data);
				PQclear(res);
				participants[i].committed = true;
			}
			PG_CATCH();
			{
				elog(WARNING, "distributed: COMMIT PREPARED failed "
					 "on node \"%s\" (gid=%s)",
					 participants[i].node_name,
					 participants[i].gid);
				FlushErrorState();
			}
			PG_END_TRY();

			pfree(cmd.data);
		}

		elog(DEBUG1, "distributed: multi-shard transaction committed "
			 "(%d participants)",
			 num_participants);
	}
	else
	{
		/* Some failed — abort all prepared */
		for (i = 0; i < num_participants; i++)
		{
			if (participants[i].prepared && !participants[i].committed)
			{
				StringInfoData cmd;
				PGresult   *res;

				initStringInfo(&cmd);
				appendStringInfo(&cmd, "ROLLBACK PREPARED '%s'",
								 participants[i].gid);

				PG_TRY();
				{
					res = DistExecSimpleQuery(
						participants[i].node_name, cmd.data);
					PQclear(res);
				}
				PG_CATCH();
				{
					elog(WARNING, "distributed: ROLLBACK PREPARED failed "
						 "on node \"%s\" (gid=%s)",
						 participants[i].node_name,
						 participants[i].gid);
					FlushErrorState();
				}
				PG_END_TRY();

				pfree(cmd.data);
			}
		}

		ereport(ERROR,
				(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
				 errmsg("distributed: multi-shard write failed, "
						"transaction aborted")));
	}

	pfree(participants);
	pfree(gid_base.data);
}
