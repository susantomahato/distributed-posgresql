/*-------------------------------------------------------------------------
 *
 * dist_guc.c
 *	  GUC parameter definitions for the distributed subsystem
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/distributed/dist_guc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distributed/dist_guc.h"
#include "miscadmin.h"
#include "utils/guc.h"

/* GUC variable definitions */
bool		dist_enabled = false;
char	   *dist_node_name = NULL;
int			dist_replication_factor = 3;
int			dist_shard_count = 32;
int			dist_heartbeat_interval_ms = 1000;
int			dist_election_timeout_min_ms = 3000;
int			dist_election_timeout_max_ms = 5000;
int			dist_raft_tick_interval_ms = 50;
bool		dist_allow_stale_reads = false;

/*
 * DistributedGucInit
 *		Register all GUC parameters for the distributed subsystem.
 */
void
DistributedGucInit(void)
{
	DefineCustomBoolVariable("distributed.enabled",
							 "Enable the distributed query processing subsystem.",
							 NULL,
							 &dist_enabled,
							 false,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	DefineCustomStringVariable("distributed.node_name",
							   "Unique name for this node in the cluster.",
							   NULL,
							   &dist_node_name,
							   "",
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	DefineCustomIntVariable("distributed.replication_factor",
							"Number of replicas for each shard.",
							NULL,
							&dist_replication_factor,
							3,
							1,
							5,
							PGC_SUSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("distributed.shard_count",
							"Default number of shards for new distributed tables.",
							NULL,
							&dist_shard_count,
							32,
							1,
							4096,
							PGC_SUSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("distributed.heartbeat_interval_ms",
							"Interval between heartbeat checks in milliseconds.",
							NULL,
							&dist_heartbeat_interval_ms,
							1000,
							100,
							60000,
							PGC_SIGHUP,
							GUC_UNIT_MS,
							NULL, NULL, NULL);

	DefineCustomIntVariable("distributed.election_timeout_min_ms",
							"Minimum election timeout in milliseconds.",
							NULL,
							&dist_election_timeout_min_ms,
							3000,
							500,
							60000,
							PGC_SIGHUP,
							GUC_UNIT_MS,
							NULL, NULL, NULL);

	DefineCustomIntVariable("distributed.election_timeout_max_ms",
							"Maximum election timeout in milliseconds.",
							NULL,
							&dist_election_timeout_max_ms,
							5000,
							1000,
							120000,
							PGC_SIGHUP,
							GUC_UNIT_MS,
							NULL, NULL, NULL);

	DefineCustomIntVariable("distributed.raft_tick_interval_ms",
							"Interval between Raft tick iterations in milliseconds.",
							NULL,
							&dist_raft_tick_interval_ms,
							50,
							10,
							5000,
							PGC_SIGHUP,
							GUC_UNIT_MS,
							NULL, NULL, NULL);

	DefineCustomBoolVariable("distributed.allow_stale_reads",
							 "Allow reads from follower replicas (eventual consistency).",
							 NULL,
							 &dist_allow_stale_reads,
							 false,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	/* Only reserve the prefix during shared_preload_libraries phase */
	if (process_shared_preload_libraries_in_progress)
		MarkGUCPrefixReserved("distributed");
}
