/*-------------------------------------------------------------------------
 *
 * dist_node.h
 *	  Node management for the distributed subsystem
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/include/distributed/dist_node.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DIST_NODE_H
#define DIST_NODE_H

#include "postgres.h"
#include "fmgr.h"
#include "nodes/pg_list.h"

/* SQL-callable functions */
extern Datum dist_add_node(PG_FUNCTION_ARGS);
extern Datum dist_remove_node(PG_FUNCTION_ARGS);
extern Datum dist_node_status(PG_FUNCTION_ARGS);

/* Internal functions */
extern void AddDistNode(const char *node_name, const char *conninfo);
extern void RemoveDistNode(const char *node_name);
extern List *GetAllDistNodes(void);
extern char *GetNodeConnInfo(const char *node_name);
extern bool IsLocalNode(const char *node_name);

#endif							/* DIST_NODE_H */
