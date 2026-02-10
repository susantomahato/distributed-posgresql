# Implementation Plan: Horizontal Sharding for PostgreSQL

## Phase 1: Core Infrastructure

- [x] 1. Create system catalog tables for shard metadata
  - Create `pg_shard_map` table with indexes
  - Create `pg_sharded_tables` table with indexes
  - Create `pg_shard_nodes` table with indexes
  - Create `pg_distributed_transactions` table
  - Add catalog initialization to bootstrap process
  - _Requirements: 1.1, 1.3, 1.5_

- [ ]* 1.1 Write property test for catalog table creation
  - **Property 1: Table Definition Consistency**
  - **Validates: Requirements 1.1**

- [-] 2. Implement shard map manager
  - [x] 2.1 Create `ShardMapEntry` and `ShardNode` data structures
    - Define structs in `src/include/catalog/pg_shard.h`
    - Implement memory management functions
    - _Requirements: 1.3, 1.5_

  - [ ] 2.2 Implement shard map lookup functions
    - `GetShardForKey()` for hash-based lookup
    - `GetShardForKey()` for range-based lookup
    - `GetAllShardsForTable()` for table scanning
    - Add caching layer for performance
    - _Requirements: 2.1, 2.3_

  - [ ]* 2.3 Write property test for shard lookup
    - **Property 6: Insert Routing Correctness**
    - **Property 7: Hash Distribution Consistency**
    - **Property 8: Range Routing Correctness**
    - **Validates: Requirements 2.1, 2.2, 2.3**

  - [ ] 2.4 Implement shard node registration
    - `RegisterShardNode()` function
    - Validate connection string format
    - Test connectivity on registration
    - _Requirements: 1.3_

  - [ ]* 2.5 Write property test for node registration
    - **Property 3: Node Registration Completeness**
    - **Validates: Requirements 1.3**

  - [ ] 2.6 Implement shard node removal with safety checks
    - `RemoveShardNode()` function
    - Check for data on node before removal
    - Prevent removal if data exists
    - _Requirements: 1.4_

  - [ ]* 2.7 Write property test for safe node removal
    - **Property 4: Safe Node Removal**
    - **Validates: Requirements 1.4**

- [ ] 3. Implement shard key validation
  - [ ] 3.1 Create shard key validation logic
    - Check column immutability
    - Verify column types are hashable/comparable
    - Validate multi-column shard keys
    - _Requirements: 1.2_

  - [ ]* 3.2 Write property test for shard key validation
    - **Property 2: Shard Key Validation**
    - **Validates: Requirements 1.2**

- [ ] 4. Implement hash distribution function
  - [ ] 4.1 Create consistent hash function
    - Use CRC32 or MurmurHash for hashing
    - Implement hash space partitioning
    - Handle hash collisions
    - _Requirements: 2.2_

  - [ ]* 4.2 Write property test for hash consistency
    - **Property 7: Hash Distribution Consistency**
    - **Validates: Requirements 2.2**

- [ ] 5. Create DDL commands for sharded tables
  - [ ] 5.1 Extend CREATE TABLE syntax
    - Add `SHARD BY HASH(column)` clause
    - Add `SHARD BY RANGE(column)` clause
    - Parse and validate shard specifications
    - _Requirements: 1.1_

  - [ ] 5.2 Implement sharded table creation
    - Create table definition on coordinator
    - Propagate table definition to all shard nodes
    - Register table in `pg_sharded_tables`
    - Create shard map entries
    - _Requirements: 1.1_

  - [ ]* 5.3 Write property test for table definition consistency
    - **Property 1: Table Definition Consistency**
    - **Validates: Requirements 1.1**

- [ ] 6. Implement connection pool manager
  - [ ] 6.1 Create connection pool data structures
    - Define `ShardConnection` and `ConnectionPool` structs
    - Implement in `src/backend/distributed/connection_pool.c`
    - _Requirements: 6.1_

  - [ ] 6.2 Implement connection acquisition and release
    - `AcquireShardConnection()` function
    - `ReleaseShardConnection()` function
    - Connection reuse logic
    - _Requirements: 6.1, 6.2_

  - [ ]* 6.3 Write property test for connection pooling
    - **Property 26: Connection Pool Maintenance**
    - **Property 27: Connection Reuse**
    - **Validates: Requirements 6.1, 6.2**

  - [ ] 6.4 Implement connection health checking
    - Periodic health checks for pooled connections
    - Mark invalid connections on failure
    - Automatic reconnection logic
    - _Requirements: 6.3, 6.5_

  - [ ]* 6.5 Write property test for connection failure handling
    - **Property 28: Connection Failure Handling**
    - **Property 30: Connection Recovery**
    - **Validates: Requirements 6.3, 6.5**

  - [ ] 6.6 Implement pool exhaustion handling
    - Request queuing when pool exhausted
    - Configurable timeout for queued requests
    - Error reporting for rejected requests
    - _Requirements: 6.4_

  - [ ]* 6.7 Write property test for pool exhaustion
    - **Property 29: Pool Exhaustion Handling**
    - **Validates: Requirements 6.4**

- [ ] 7. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Phase 2: Basic Query Routing

- [ ] 8. Implement single-shard query detection
  - [ ] 8.1 Analyze query predicates for shard key
    - Extract shard key values from WHERE clauses
    - Determine if query targets single shard
    - Mark query plan with single-shard flag
    - _Requirements: 3.1_

  - [ ]* 8.2 Write property test for single-shard detection
    - **Property 11: Single-Shard Query Optimization**
    - **Validates: Requirements 3.1**

- [ ] 9. Implement single-shard query execution
  - [ ] 9.1 Create query routing for single shard
    - Route entire query to target shard
    - Execute query remotely via postgres_fdw
    - Return results directly to client
    - _Requirements: 3.1_

  - [ ] 9.2 Implement INSERT routing
    - Calculate target shard from shard key
    - Route INSERT to appropriate shard node
    - Handle batch inserts efficiently
    - _Requirements: 2.1, 2.5_

  - [ ]* 9.3 Write property test for insert routing
    - **Property 6: Insert Routing Correctness**
    - **Property 10: Batch Insert Routing**
    - **Validates: Requirements 2.1, 2.5**

  - [ ] 9.4 Implement UPDATE/DELETE routing
    - Detect single-shard updates/deletes
    - Route to appropriate shard
    - Handle multi-shard updates/deletes
    - _Requirements: 3.1, 3.2_

- [ ] 10. Implement multi-shard query execution
  - [ ] 10.1 Create distributed query plan
    - Generate per-shard query plans
    - Determine merge strategy
    - Optimize for parallel execution
    - _Requirements: 3.2_

  - [ ] 10.2 Implement parallel shard execution
    - Execute queries on all relevant shards in parallel
    - Use connection pool for shard connections
    - Collect results from all shards
    - _Requirements: 3.2_

  - [ ]* 10.3 Write property test for multi-shard queries
    - **Property 12: Multi-Shard Result Completeness**
    - **Validates: Requirements 3.2**

  - [ ] 10.4 Implement result merging for simple queries
    - Merge result sets from multiple shards
    - Handle UNION of results
    - Preserve row order where needed
    - _Requirements: 3.2_

- [ ] 11. Implement distributed aggregations
  - [ ] 11.1 Implement aggregation pushdown
    - Push partial aggregations to shards (SUM, COUNT, etc.)
    - Collect partial results from shards
    - Combine partial results at coordinator
    - _Requirements: 3.3_

  - [ ]* 11.2 Write property test for distributed aggregations
    - **Property 13: Distributed Aggregation Correctness**
    - **Validates: Requirements 3.3**

- [ ] 12. Implement distributed sorting and limiting
  - [ ] 12.1 Implement distributed ORDER BY
    - Push ORDER BY to shards
    - Merge-sort results from shards
    - Handle multiple sort keys
    - _Requirements: 3.5_

  - [ ] 12.2 Implement distributed LIMIT
    - Push LIMIT to shards for optimization
    - Apply final LIMIT at coordinator
    - Handle OFFSET correctly
    - _Requirements: 3.5_

  - [ ]* 12.3 Write property test for distributed sorting
    - **Property 15: Distributed Sort Correctness**
    - **Validates: Requirements 3.5**

- [ ] 13. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Phase 3: Distributed Transactions

- [ ] 14. Implement distributed transaction manager
  - [ ] 14.1 Create distributed transaction data structures
    - Define `DistributedTransaction` struct
    - Implement transaction state tracking
    - Create transaction log for recovery
    - _Requirements: 4.1, 4.2_

  - [ ] 14.2 Implement BEGIN for distributed transactions
    - Detect multi-shard transactions
    - Initialize distributed transaction context
    - Assign global transaction ID
    - _Requirements: 4.1_

  - [ ] 14.3 Implement two-phase commit protocol
    - PREPARE phase: send PREPARE to all participants
    - COMMIT phase: send COMMIT if all prepared
    - ABORT phase: send ABORT if any failed
    - _Requirements: 4.1, 4.2_

  - [ ]* 14.4 Write property test for 2PC protocol
    - **Property 16: Two-Phase Commit Usage**
    - **Property 17: Distributed Transaction Atomicity**
    - **Validates: Requirements 4.1, 4.2**

  - [ ] 14.5 Implement transaction recovery
    - Scan prepared transactions on startup
    - Resolve in-doubt transactions
    - Coordinator-driven recovery
    - _Requirements: 4.3_

  - [ ]* 14.6 Write property test for transaction recovery
    - **Property 18: Transaction Recovery Correctness**
    - **Validates: Requirements 4.3**

- [ ] 15. Implement distributed snapshot isolation
  - [ ] 15.1 Create distributed snapshot mechanism
    - Generate consistent snapshots across shards
    - Propagate snapshot to all participants
    - Ensure all shards use same snapshot
    - _Requirements: 4.4_

  - [ ]* 15.2 Write property test for snapshot isolation
    - **Property 19: Distributed Snapshot Isolation**
    - **Validates: Requirements 4.4**

- [ ] 16. Implement distributed deadlock detection
  - [ ] 16.1 Create wait-for graph across shards
    - Collect lock wait information from shards
    - Build global wait-for graph
    - Detect cycles in wait-for graph
    - _Requirements: 4.5_

  - [ ] 16.2 Implement deadlock resolution
    - Select victim transaction to abort
    - Abort victim on all participants
    - Return deadlock error to client
    - _Requirements: 4.5_

  - [ ]* 16.3 Write property test for deadlock detection
    - **Property 20: Distributed Deadlock Resolution**
    - **Validates: Requirements 4.5**

- [ ] 17. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Phase 4: Rebalancing and Splitting

- [ ] 18. Implement rebalancing plan generation
  - [ ] 18.1 Create rebalancing planner
    - Analyze current data distribution
    - Calculate target distribution
    - Generate data movement plan
    - _Requirements: 5.1_

  - [ ]* 18.2 Write property test for rebalancing plans
    - **Property 21: Rebalancing Plan Validity**
    - **Validates: Requirements 5.1**

- [ ] 19. Implement online data migration
  - [ ] 19.1 Create data migration executor
    - Use logical replication for data copy
    - Track migration progress
    - Handle concurrent writes during migration
    - _Requirements: 5.2, 5.3_

  - [ ]* 19.2 Write property test for migration availability
    - **Property 22: Rebalancing Read Availability**
    - **Property 23: Migration Query Routing**
    - **Validates: Requirements 5.2, 5.3**

  - [ ] 19.3 Implement atomic shard map update
    - Prepare new shard map
    - Atomically switch to new map
    - Invalidate old routing cache
    - _Requirements: 5.4_

  - [ ]* 19.4 Write property test for shard map atomicity
    - **Property 24: Shard Map Update Atomicity**
    - **Validates: Requirements 5.4**

  - [ ] 19.5 Implement rebalancing rollback
    - Detect migration failures
    - Rollback to previous configuration
    - Verify no data loss
    - _Requirements: 5.5_

  - [ ]* 19.6 Write property test for rollback safety
    - **Property 25: Rebalancing Rollback Safety**
    - **Validates: Requirements 5.5**

- [ ] 20. Implement automatic shard splitting
  - [ ] 20.1 Create shard monitoring
    - Track shard size metrics
    - Monitor query load per shard
    - Detect hot spots
    - _Requirements: 11.1, 11.6_

  - [ ]* 20.2 Write property test for split candidate identification
    - **Property 51: Split Candidate Identification**
    - **Property 56: Hot Spot Detection**
    - **Validates: Requirements 11.1, 11.6**

  - [ ] 20.3 Implement split point calculation
    - Analyze shard key distribution
    - Calculate median split point
    - Ensure balanced split
    - _Requirements: 11.2_

  - [ ]* 20.4 Write property test for split point balance
    - **Property 52: Split Point Balance**
    - **Validates: Requirements 11.2**

  - [ ] 20.5 Implement shard split execution
    - Create new shard with updated boundaries
    - Copy data to new shard
    - Maintain read availability during split
    - Route writes correctly during split
    - _Requirements: 11.3, 11.4_

  - [ ]* 20.6 Write property test for split execution
    - **Property 53: Split Read Availability**
    - **Property 54: Split Write Routing**
    - **Validates: Requirements 11.3, 11.4**

  - [ ] 20.7 Implement split completion
    - Atomically update shard map
    - Remove old shard configuration
    - Clean up temporary state
    - _Requirements: 11.5_

  - [ ]* 20.8 Write property test for split completion
    - **Property 55: Split Completion Atomicity**
    - **Validates: Requirements 11.5**

  - [ ] 20.9 Implement split prioritization
    - Rank shards by size, growth, and load
    - Schedule splits based on priority
    - Limit concurrent splits
    - _Requirements: 11.7_

  - [ ]* 20.10 Write property test for split prioritization
    - **Property 57: Split Prioritization**
    - **Validates: Requirements 11.7**

- [ ] 21. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Phase 5: Monitoring and Observability

- [ ] 22. Implement monitoring system views
  - [ ] 22.1 Create `pg_shard_stats` view
    - Show query distribution across shards
    - Display per-shard query counts
    - Include timing statistics
    - _Requirements: 7.1_

  - [ ]* 22.2 Write property test for query statistics
    - **Property 31: Query Distribution Statistics**
    - **Validates: Requirements 7.1**

  - [ ] 22.3 Create `pg_shard_health` view
    - Show shard node health status
    - Display connection pool statistics
    - Include error counts
    - _Requirements: 7.3_

  - [ ]* 22.4 Write property test for error tracking
    - **Property 33: Error Tracking Accuracy**
    - **Validates: Requirements 7.3**

  - [ ] 22.5 Create `pg_shard_distribution` view
    - Show data distribution across shards
    - Calculate and display skew metrics
    - Identify imbalanced shards
    - _Requirements: 7.4_

  - [ ]* 22.6 Write property test for skew detection
    - **Property 34: Data Skew Detection**
    - **Validates: Requirements 7.4**

- [ ] 23. Implement query execution logging
  - [ ] 23.1 Add per-shard timing to query logs
    - Log execution time for each shard
    - Include shard routing information
    - Track cross-shard communication latency
    - _Requirements: 7.2, 7.5_

  - [ ]* 23.2 Write property test for execution logging
    - **Property 32: Per-Shard Execution Logging**
    - **Property 35: Latency Tracking**
    - **Validates: Requirements 7.2, 7.5**

- [ ] 24. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Phase 6: High Availability and Security

- [ ] 25. Implement shard replication and failover
  - [ ] 25.1 Configure shard replicas
    - Support multiple replicas per shard
    - Track replica lag
    - Monitor replica health
    - _Requirements: 10.2_

  - [ ] 25.2 Implement automatic failover
    - Detect shard node failures
    - Promote replica to primary
    - Update shard map with new primary
    - _Requirements: 10.1, 10.2_

  - [ ]* 25.3 Write property test for failover
    - **Property 46: Failure Detection Timeliness**
    - **Property 47: Automatic Failover**
    - **Validates: Requirements 10.1, 10.2**

  - [ ] 25.4 Implement shard recovery
    - Reintegrate recovered shard
    - Catch up from replica or WAL
    - Update shard map
    - _Requirements: 10.3_

  - [ ]* 25.5 Write property test for recovery
    - **Property 48: Shard Recovery Reintegration**
    - **Validates: Requirements 10.3**

- [ ] 26. Implement coordinator failover
  - [ ] 26.1 Support standby coordinators
    - Replicate shard map to standbys
    - Track coordinator health
    - Implement promotion mechanism
    - _Requirements: 10.4_

  - [ ]* 26.2 Write property test for coordinator failover
    - **Property 49: Coordinator Failover**
    - **Validates: Requirements 10.4**

- [ ] 27. Implement split-brain prevention
  - [ ] 27.1 Create quorum-based decision making
    - Implement consensus protocol
    - Require majority for shard map updates
    - Prevent conflicting updates
    - _Requirements: 10.5_

  - [ ]* 27.2 Write property test for split-brain prevention
    - **Property 50: Split-Brain Prevention**
    - **Validates: Requirements 10.5**

- [ ] 28. Implement security features
  - [ ] 28.1 Implement credential propagation
    - Propagate user credentials to shards
    - Support SSL connections to shards
    - Encrypt coordinator-to-shard traffic
    - _Requirements: 9.2, 9.4_

  - [ ]* 28.2 Write property test for credential propagation
    - **Property 42: Credential Propagation**
    - **Property 44: SSL Enforcement**
    - **Validates: Requirements 9.2, 9.4**

  - [ ] 28.3 Implement permission replication
    - Replicate GRANT/REVOKE to all shards
    - Ensure consistent permissions
    - Handle permission conflicts
    - _Requirements: 9.3_

  - [ ]* 28.4 Write property test for permission replication
    - **Property 43: Permission Replication**
    - **Validates: Requirements 9.3**

  - [ ] 28.5 Implement distributed RLS
    - Enforce row-level security on all shards
    - Propagate RLS policies to shards
    - Ensure consistent enforcement
    - _Requirements: 9.1_

  - [ ]* 28.6 Write property test for RLS enforcement
    - **Property 41: Distributed RLS Enforcement**
    - **Validates: Requirements 9.1**

  - [ ] 28.7 Implement audit logging
    - Log distributed queries with routing info
    - Include shard access in audit trail
    - Support audit log aggregation
    - _Requirements: 9.5_

  - [ ]* 28.8 Write property test for audit logging
    - **Property 45: Audit Log Completeness**
    - **Validates: Requirements 9.5**

- [ ] 29. Implement backup and recovery
  - [ ] 29.1 Implement coordinated backup
    - Create consistent snapshots across shards
    - Include shard map in backup
    - Support parallel backup of shards
    - _Requirements: 8.1, 8.2_

  - [ ]* 29.2 Write property test for backup consistency
    - **Property 36: Backup Consistency**
    - **Property 37: Backup Metadata Completeness**
    - **Validates: Requirements 8.1, 8.2**

  - [ ] 29.3 Implement coordinated restore
    - Restore all shards to consistent point
    - Restore shard map metadata
    - Support partial shard restore
    - _Requirements: 8.3, 8.4_

  - [ ]* 29.4 Write property test for restore consistency
    - **Property 38: Restore Consistency**
    - **Property 39: Partial Shard Restore**
    - **Validates: Requirements 8.3, 8.4**

  - [ ] 29.5 Implement point-in-time recovery
    - Coordinate WAL replay across shards
    - Ensure consistent recovery point
    - Handle shard-specific recovery
    - _Requirements: 8.5_

  - [ ]* 29.6 Write property test for PITR
    - **Property 40: PITR Coordination**
    - **Validates: Requirements 8.5**

- [ ] 30. Final Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Phase 7: Documentation and Tools

- [ ] 31. Create user documentation
  - Write administrator guide for sharded tables
  - Document DDL syntax for sharding
  - Create troubleshooting guide
  - Write performance tuning guide

- [ ] 32. Create migration tools
  - Tool to convert existing tables to sharded
  - Tool to export/import sharded tables
  - Tool to analyze shard distribution

- [ ] 33. Create monitoring dashboards
  - Grafana dashboard for shard metrics
  - Alert rules for shard health
  - Performance monitoring queries
