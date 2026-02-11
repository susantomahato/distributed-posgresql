/*-------------------------------------------------------------------------
 *
 * failure_detector.h
 *	  Health monitoring and auto-recovery API
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/include/distributed/failure_detector.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FAILURE_DETECTOR_H
#define FAILURE_DETECTOR_H

/* Failure detection */
extern void FailureDetectorCheck(void);
extern void HandleNodeFailure(const char *node_name);

/* Re-replication */
extern void InitiateReReplication(int raft_group_id, const char *new_node);
extern void ReReplicationComplete(int raft_group_id, const char *new_node);

/* Node recovery */
extern void HandleNodeRecovery(const char *node_name);

#endif							/* FAILURE_DETECTOR_H */
