#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for distributed
#
# IDENTIFICATION
#    src/backend/distributed/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/distributed
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)

OBJS = \
	dist_commands.o \
	dist_connection.o \
	dist_copy.o \
	dist_executor.o \
	dist_guc.o \
	dist_main.o \
	dist_node.o \
	dist_shmem.o \
	dist_transaction.o \
	failure_detector.o \
	placement.o \
	query_router.o \
	raft.o \
	raft_log.o \
	raft_rpc.o \
	raft_worker.o \
	rebalancer.o \
	shard.o

include $(top_srcdir)/src/backend/common.mk
