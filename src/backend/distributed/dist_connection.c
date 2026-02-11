/*-------------------------------------------------------------------------
 *
 * dist_connection.c
 *	  Connection pool for inter-node communication
 *
 * Maintains a per-backend hash table of PGconn* connections to remote
 * nodes, following the postgres_fdw pattern. Connections are cached for
 * the duration of a transaction and cleaned up at transaction end.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/distributed/dist_connection.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distributed/dist_connection.h"
#include "distributed/dist_guc.h"
#include "distributed/dist_node.h"
#include "access/xact.h"
#include "libpq-fe.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

/* Hash table entry for cached connections */
typedef struct ConnCacheEntry
{
	char		node_name[NAMEDATALEN];	/* hash key */
	PGconn	   *conn;
	bool		in_use;
	TimestampTz last_used;
} ConnCacheEntry;

/* Connection cache hash table (per-backend) */
static HTAB *ConnCache = NULL;

/*
 * DistConnectionInit
 *		Initialize the per-backend connection cache.
 */
void
DistConnectionInit(void)
{
	HASHCTL		ctl;

	if (ConnCache != NULL)
		return;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = NAMEDATALEN;
	ctl.entrysize = sizeof(ConnCacheEntry);
	ctl.hcxt = TopMemoryContext;

	ConnCache = hash_create("Distributed Connection Cache",
							32, &ctl,
							HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);
}

/*
 * GetDistConnection
 *		Get a connection to a remote node.
 *
 * Returns a cached connection if available, otherwise creates a new one.
 */
PGconn *
GetDistConnection(const char *node_name)
{
	ConnCacheEntry *entry;
	bool		found;
	char	   *conninfo;

	if (ConnCache == NULL)
		DistConnectionInit();

	entry = (ConnCacheEntry *) hash_search(ConnCache, node_name,
										   HASH_ENTER, &found);

	if (found && entry->conn != NULL)
	{
		/* Check if connection is still alive */
		if (PQstatus(entry->conn) == CONNECTION_OK)
		{
			entry->last_used = GetCurrentTimestamp();
			return entry->conn;
		}

		/* Connection dead, clean up */
		PQfinish(entry->conn);
		entry->conn = NULL;
	}

	/* Create new connection */
	conninfo = GetNodeConnInfo(node_name);
	if (conninfo == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("no connection info for node \"%s\"", node_name)));

	entry->conn = PQconnectdb(conninfo);
	pfree(conninfo);

	if (PQstatus(entry->conn) != CONNECTION_OK)
	{
		char	   *errmsg_text = pstrdup(PQerrorMessage(entry->conn));

		PQfinish(entry->conn);
		entry->conn = NULL;
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not connect to node \"%s\": %s",
						node_name, errmsg_text)));
	}

	entry->in_use = true;
	entry->last_used = GetCurrentTimestamp();
	strlcpy(entry->node_name, node_name, NAMEDATALEN);

	return entry->conn;
}

/*
 * ReleaseDistConnection
 *		Mark a connection as not in active use (keep cached).
 */
void
ReleaseDistConnection(const char *node_name)
{
	ConnCacheEntry *entry;
	bool		found;

	if (ConnCache == NULL)
		return;

	entry = (ConnCacheEntry *) hash_search(ConnCache, node_name,
										   HASH_FIND, &found);
	if (found)
		entry->in_use = false;
}

/*
 * CloseAllDistConnections
 *		Close and free all cached connections.
 */
void
CloseAllDistConnections(void)
{
	HASH_SEQ_STATUS status;
	ConnCacheEntry *entry;

	if (ConnCache == NULL)
		return;

	hash_seq_init(&status, ConnCache);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->conn != NULL)
		{
			PQfinish(entry->conn);
			entry->conn = NULL;
		}
	}
}

/*
 * DistExecSimpleQuery
 *		Execute a query on a remote node and return the result.
 *
 * The caller is responsible for PQclear()ing the result.
 */
PGresult *
DistExecSimpleQuery(const char *node_name, const char *query)
{
	PGconn	   *conn;
	PGresult   *result;

	conn = GetDistConnection(node_name);
	result = PQexec(conn, query);

	if (PQresultStatus(result) != PGRES_TUPLES_OK &&
		PQresultStatus(result) != PGRES_COMMAND_OK)
	{
		char	   *errmsg_text = pstrdup(PQresultErrorMessage(result));

		PQclear(result);
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("remote query on node \"%s\" failed: %s",
						node_name, errmsg_text)));
	}

	return result;
}

/*
 * DistConnectionXactCallback
 *		Transaction callback to clean up connections.
 *
 * On COMMIT or ABORT, close all connections that are not in use.
 * This prevents connection leaks across transactions.
 */
void
DistConnectionXactCallback(XactEvent event, void *arg)
{
	if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_ABORT)
	{
		CloseAllDistConnections();
	}
}
