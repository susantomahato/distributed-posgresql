/*-------------------------------------------------------------------------
 *
 * dist_guc.h
 *	  GUC parameter declarations for the distributed subsystem
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/include/distributed/dist_guc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DIST_GUC_H
#define DIST_GUC_H

/* GUC variables */
extern PGDLLIMPORT bool dist_enabled;
extern PGDLLIMPORT char *dist_node_name;
extern PGDLLIMPORT int dist_replication_factor;
extern PGDLLIMPORT int dist_shard_count;
extern PGDLLIMPORT int dist_heartbeat_interval_ms;
extern PGDLLIMPORT int dist_election_timeout_min_ms;
extern PGDLLIMPORT int dist_election_timeout_max_ms;
extern PGDLLIMPORT int dist_raft_tick_interval_ms;
extern PGDLLIMPORT bool dist_allow_stale_reads;
extern PGDLLIMPORT bool dist_propagating;
extern PGDLLIMPORT bool dist_forwarded;

/* Register GUC parameters */
extern void DistributedGucInit(void);

#endif							/* DIST_GUC_H */
