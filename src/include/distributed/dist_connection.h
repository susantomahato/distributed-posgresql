/*-------------------------------------------------------------------------
 *
 * dist_connection.h
 *	  Connection pool for inter-node communication
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/include/distributed/dist_connection.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DIST_CONNECTION_H
#define DIST_CONNECTION_H

#include "libpq-fe.h"
#include "datatype/timestamp.h"
#include "access/xact.h"

/*
 * DistConnection — cached connection to a remote node
 */
typedef struct DistConnection
{
	char		node_name[NAMEDATALEN];
	PGconn	   *conn;
	bool		in_use;
	TimestampTz last_used;
} DistConnection;

/* Connection pool management */
extern void DistConnectionInit(void);
extern void DistConnectionCleanup(void);

/* Get a connection to a node (creates or reuses) */
extern PGconn *GetDistConnection(const char *node_name);

/* Return a connection to the pool */
extern void ReleaseDistConnection(const char *node_name);

/* Close all cached connections */
extern void CloseAllDistConnections(void);

/* Execute a simple query on a remote node */
extern PGresult *DistExecSimpleQuery(const char *node_name,
									 const char *query);

/* Transaction callback to cleanup connections */
extern void DistConnectionXactCallback(XactEvent event, void *arg);

#endif							/* DIST_CONNECTION_H */
