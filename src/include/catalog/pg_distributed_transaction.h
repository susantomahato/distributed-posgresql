/*-------------------------------------------------------------------------
 *
 * pg_distributed_transaction.h
 *	  definition of the "distributed transaction" system catalog
 *	  (pg_distributed_transaction)
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/include/catalog/pg_distributed_transaction.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_DISTRIBUTED_TRANSACTION_H
#define PG_DISTRIBUTED_TRANSACTION_H

#include "catalog/genbki.h"
#include "catalog/pg_distributed_transaction_d.h"

/* ----------------
 *		pg_distributed_transaction definition.  cpp turns this into
 *		typedef struct FormData_pg_distributed_transaction
 * ----------------
 */
CATALOG(pg_distributed_transaction,9030,DistributedTransactionRelationId)
{
	/* global transaction identifier */
	text		gid BKI_FORCE_NOT_NULL;

	/* array of participating shard node names */
	text		participants[1] BKI_FORCE_NOT_NULL;

	/* transaction state: 'p' = preparing, 'r' = prepared, 'c' = committing */
	char		txnstate BKI_FORCE_NOT_NULL;

	/* transaction start timestamp */
	timestamptz	startedat BKI_FORCE_NOT_NULL;

	/* timestamp when all participants prepared */
	timestamptz	preparedat;
} FormData_pg_distributed_transaction;

/* ----------------
 *		Form_pg_distributed_transaction corresponds to a pointer to a tuple
 *		with the format of pg_distributed_transaction relation.
 * ----------------
 */
typedef FormData_pg_distributed_transaction *Form_pg_distributed_transaction;

DECLARE_TOAST(pg_distributed_transaction, 9031, 9032);

DECLARE_UNIQUE_INDEX_PKEY(pg_distributed_transaction_gid_index, 9033, DistributedTransactionGidIndexId, pg_distributed_transaction, btree(gid text_ops));

MAKE_SYSCACHE(DISTRIBUTEDTXNGID, pg_distributed_transaction_gid_index, 4);

/*
 * Transaction state constants
 */
#define DTXN_STATE_PREPARING	'p'
#define DTXN_STATE_PREPARED		'r'
#define DTXN_STATE_COMMITTING	'c'

extern bool is_distributed_transaction(const char *gid);
extern void register_distributed_transaction(const char *gid, List *participants);
extern void update_distributed_transaction_state(const char *gid, char newstate);

#endif							/* PG_DISTRIBUTED_TRANSACTION_H */
