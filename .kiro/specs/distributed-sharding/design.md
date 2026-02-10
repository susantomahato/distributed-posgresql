# Design Document: Horizontal Sharding for PostgreSQL

## Overview

This design document describes the architecture for adding native horizontal sharding capabilities to PostgreSQL. The system enables transparent distribution of table data across multiple PostgreSQL nodes while maintaining ACID guarantees, query transparency, and operational simplicity.

The design builds upon PostgreSQL's existing infrastructure:
- **Declarative partitioning** (`src/backend/partitioning/`) for partition management
- **Foreign Data Wrappers** (`contrib/postgres_fdw/`) for remote data access
- **Two-phase commit** for distributed transactions
- **Logical replication** for data movement during rebalancing

## Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Client Applications                      │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                   Coordinator Node                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │ Query Parser │  │ Shard Router │  │ Distributed  │     │
│  │   & Planner  │  │              │  │ Transaction  │     │
│  └──────────────┘  └──────────────┘  │   Manager    │     │
│                                       └──────────────┘     │
│  ┌──────────────────────────────────────────────────┐     │
│  │           Shard Map Metadata                      │     │
│  └──────────────────────────────────────────────────┘     │
└────────────┬──────────────┬──────────────┬─────────────────┘
             │              │              │
             ▼              ▼              ▼
    ┌────────────┐  ┌────────────┐  ┌────────────┐
    │  Shard 1   │  │  Shard 2   │  │  Shard N   │
    │  (Node A)  │  │  (Node B)  │  │  (Node N)  │
    └────────────┘  └────────────┘  └────────────┘
```

### Component Layers

1. **Client Layer**: Applications connect to coordinator using standard PostgreSQL protocol
2. **Coordinator Layer**: Query routing, planning, and distributed transaction coordination
3. **Shard Layer**: Individual PostgreSQL instances storing data partitions
4. **Metadata Layer**: Shard map and configuration stored in coordinator catalog

## Components and Interfaces

### 1. Shard Map Manager

**Purpose**: Manages metadata about shard distribution and node topology.

**Key Structures**:
```c
typedef struct ShardMapEntry
{
    Oid         sharded_table_oid;  /* OID of sharded table */
    int32       shard_id;            /* Unique shard identifier */
    char       *node_name;           /* Shard node connection string */
    Datum       range_min;           /* Min value for range sharding */
    Datum       range_max;           /* Max value for range sharding */
    int32       hash_min;            /* Min hash value for hash sharding */
    int32       hash_max;            /* Max hash value for hash sharding */
    ShardState  state;               /* ACTIVE, SPLITTING, MIGRATING, etc */
} ShardMapEntry;

typedef struct ShardNode
{
    char       *node_name;           /* Connection identifier */
    char       *connection_string;   /* libpq connection string */
    NodeState   state;               /* ONLINE, OFFLINE, DEGRADED */
    int32       shard_count;         /* Number of shards on this node */
    TimestampTz last_health_check;   /* Last successful health check */
} ShardNode;
```

**Interfaces**:
- `ShardMapEntry *GetShardForKey(Oid table_oid, Datum shard_key)` - Lookup shard by key
- `List *GetAllShardsForTable(Oid table_oid)` - Get all shards for a table
- `void RegisterShardNode(ShardNode *node)` - Add new shard node
- `void UpdateShardMap(List *new_mappings)` - Atomic shard map update

### 2. Query Router

**Purpose**: Routes queries to appropriate shards based on query predicates and shard key.

**Key Functions**:
- **Single-shard optimization**: Detect queries targeting one shard
- **Multi-shard fan-out**: Execute query on multiple shards in parallel
- **Result merging**: Combine results from shards (sorting, aggregation, joins)

**Interfaces**:
```c
typedef struct DistributedPlan
{
    List       *shard_plans;         /* Per-shard execution plans */
    MergeStrategy merge_strategy;    /* How to combine results */
    bool        single_shard;        /* Optimization flag */
    List       *target_shards;       /* Shards involved in query */
} DistributedPlan;

DistributedPlan *CreateDistributedPlan(Query *query, Oid table_oid);
void ExecuteDistributedPlan(DistributedPlan *plan, DestReceiver *dest);
```

### 3. Distributed Transaction Manager

**Purpose**: Coordinates ACID transactions across multiple shards using two-phase commit.

**Key Structures**:
```c
typedef struct DistributedTransaction
{
    TransactionId xid;               /* Global transaction ID */
    List       *participants;        /* List of shard nodes */
    TwoPhaseState state;             /* PREPARING, PREPARED, COMMITTING */
    TimestampTz start_time;          /* Transaction start timestamp */
    SnapshotData snapshot;           /* Distributed snapshot */
} DistributedTransaction;
```

**Protocol**:
1. **Prepare Phase**: Send PREPARE to all participants
2. **Commit Phase**: If all prepared successfully, send COMMIT; otherwise ABORT
3. **Recovery**: Coordinator maintains prepared transaction log for crash recovery

**Interfaces**:
- `void BeginDistributedTransaction(List *shards)` - Start distributed txn
- `bool PrepareDistributedTransaction(DistributedTransaction *dtxn)` - Prepare phase
- `void CommitDistributedTransaction(DistributedTransaction *dtxn)` - Commit phase
- `void AbortDistributedTransaction(DistributedTransaction *dtxn)` - Abort phase

### 4. Connection Pool Manager

**Purpose**: Maintains persistent connections to shard nodes for efficient query execution.

**Key Structures**:
```c
typedef struct ShardConnection
{
    char       *node_name;           /* Shard node identifier */
    PGconn     *conn;                /* libpq connection */
    bool        in_use;              /* Connection currently in use */
    bool        in_transaction;      /* Connection has active transaction */
    TimestampTz last_used;           /* Last activity timestamp */
} ShardConnection;

typedef struct ConnectionPool
{
    char       *node_name;           /* Pool for specific shard node */
    List       *connections;         /* List of ShardConnection */
    int         max_connections;     /* Pool size limit */
    int         active_count;        /* Currently in-use connections */
} ConnectionPool;
```

**Interfaces**:
- `ShardConnection *AcquireShardConnection(char *node_name)` - Get connection from pool
- `void ReleaseShardConnection(ShardConnection *conn)` - Return connection to pool
- `void InvalidateShardConnections(char *node_name)` - Mark connections invalid on failure

### 5. Shard Rebalancer

**Purpose**: Moves data between shards to maintain balanced distribution.

**Key Operations**:
- **Rebalancing**: Redistribute data across existing shards
- **Shard splitting**: Split large shards into smaller ones
- **Shard merging**: Combine small shards (future enhancement)

**Interfaces**:
```c
typedef struct RebalancePlan
{
    List       *data_movements;      /* List of DataMovement */
    int64       total_rows;          /* Total rows to move */
    int64       estimated_time;      /* Estimated duration */
} RebalancePlan;

typedef struct DataMovement
{
    int32       source_shard;        /* Source shard ID */
    int32       dest_shard;          /* Destination shard ID */
    Datum       key_min;             /* Min key value to move */
    Datum       key_max;             /* Max key value to move */
    int64       row_count;           /* Estimated rows */
} DataMovement;

RebalancePlan *CreateRebalancePlan(Oid table_oid, List *target_nodes);
void ExecuteRebalancePlan(RebalancePlan *plan);
```

### 6. Shard Splitter

**Purpose**: Automatically splits shards when they exceed size or load thresholds.

**Algorithm**:
1. Monitor shard size and query load
2. Identify split candidates based on thresholds
3. Calculate optimal split point (median of shard key distribution)
4. Create new shard with updated range/hash boundaries
5. Copy data to new shard using logical replication
6. Atomically update shard map to activate new shards

**Interfaces**:
- `List *IdentifySplitCandidates(void)` - Find shards needing split
- `SplitPoint CalculateSplitPoint(int32 shard_id)` - Determine split boundary
- `void ExecuteShardSplit(int32 shard_id, SplitPoint split_point)` - Perform split

## Data Models

### System Catalog Tables

#### pg_shard_map
Stores shard distribution metadata.

```sql
CREATE TABLE pg_catalog.pg_shard_map (
    shard_id        integer PRIMARY KEY,
    table_oid       oid NOT NULL,
    node_name       text NOT NULL,
    shard_method    char NOT NULL,  -- 'h' = hash, 'r' = range
    range_min       text,           -- For range sharding
    range_max       text,
    hash_min        integer,        -- For hash sharding
    hash_max        integer,
    state           char NOT NULL,  -- 'a' = active, 's' = splitting, 'm' = migrating
    created_at      timestamptz DEFAULT now(),
    FOREIGN KEY (table_oid) REFERENCES pg_sharded_tables(table_oid)
);
```

#### pg_sharded_tables
Tracks which tables are sharded.

```sql
CREATE TABLE pg_catalog.pg_sharded_tables (
    table_oid       oid PRIMARY KEY,
    shard_key       text[] NOT NULL,     -- Column names forming shard key
    shard_method    char NOT NULL,       -- 'h' = hash, 'r' = range
    shard_count     integer NOT NULL,
    created_at      timestamptz DEFAULT now()
);
```

#### pg_shard_nodes
Stores shard node configuration.

```sql
CREATE TABLE pg_catalog.pg_shard_nodes (
    node_name       text PRIMARY KEY,
    connection_str  text NOT NULL,
    state           char NOT NULL,       -- 'o' = online, 'f' = offline, 'd' = degraded
    shard_count     integer DEFAULT 0,
    last_health_check timestamptz,
    created_at      timestamptz DEFAULT now()
);
```

#### pg_distributed_transactions
Tracks distributed transactions for recovery.

```sql
CREATE TABLE pg_catalog.pg_distributed_transactions (
    gid             text PRIMARY KEY,    -- Global transaction ID
    participants    text[] NOT NULL,     -- List of shard nodes
    state           char NOT NULL,       -- 'p' = preparing, 'r' = prepared, 'c' = committing
    started_at      timestamptz NOT NULL,
    prepared_at     timestamptz
);
```

### Shard Key Distribution

**Hash Sharding**:
- Uses consistent hashing (e.g., CRC32 or MurmurHash)
- Hash space divided into ranges assigned to shards
- Example: 4 shards divide hash space 0-2^32 into 4 equal ranges

**Range Sharding**:
- Explicit range boundaries defined by DBA
- Suitable for time-series or naturally ordered data
- Example: Shard 1 (2020-01-01 to 2021-01-01), Shard 2 (2021-01-01 to 2022-01-01)

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system-essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Table Definition Consistency
*For any* sharded table creation, querying the table definition on all shard nodes should return identical schemas
**Validates: Requirements 1.1**

### Property 2: Shard Key Validation
*For any* shard key specification, the validation function should accept only immutable columns and reject mutable ones
**Validates: Requirements 1.2**

### Property 3: Node Registration Completeness
*For any* new shard node registration, the node should appear in the shard map and be available for data distribution
**Validates: Requirements 1.3**

### Property 4: Safe Node Removal
*For any* shard node with data, attempting removal without rebalancing should be rejected with an error
**Validates: Requirements 1.4**

### Property 5: Metadata Query Accuracy
*For any* shard configuration, metadata queries should return accurate information about all shards, nodes, and data distribution
**Validates: Requirements 1.5**

### Property 6: Insert Routing Correctness
*For any* row inserted into a sharded table, the row should be stored on exactly the shard determined by the shard key
**Validates: Requirements 2.1**

### Property 7: Hash Distribution Consistency
*For any* shard key value, applying the hash function multiple times should always route to the same shard
**Validates: Requirements 2.2**

### Property 8: Range Routing Correctness
*For any* shard key value and range boundaries, the value should be routed to the shard whose range contains it
**Validates: Requirements 2.3**

### Property 9: Unavailable Shard Rejection
*For any* write operation targeting an unavailable shard, the operation should be rejected with a clear error message
**Validates: Requirements 2.4**

### Property 10: Batch Insert Routing
*For any* set of rows in a batch insert, each row should be routed to its correct shard based on its shard key
**Validates: Requirements 2.5**

### Property 11: Single-Shard Query Optimization
*For any* query with predicates that restrict to one shard, the query should be executed entirely on that shard
**Validates: Requirements 3.1**

### Property 12: Multi-Shard Result Completeness
*For any* query spanning multiple shards, the final result should contain all rows from all relevant shards
**Validates: Requirements 3.2**

### Property 13: Distributed Aggregation Correctness
*For any* aggregation query, the result should equal the aggregation computed over the union of all shard results
**Validates: Requirements 3.3**

### Property 14: Join Result Correctness
*For any* join query across shards, the result should match the join computed on the logical (unsharded) table
**Validates: Requirements 3.4**

### Property 15: Distributed Sort Correctness
*For any* ORDER BY query, the final result should be correctly sorted according to the sort specification
**Validates: Requirements 3.5**

### Property 16: Two-Phase Commit Usage
*For any* transaction modifying multiple shards, the system should execute the two-phase commit protocol
**Validates: Requirements 4.1**

### Property 17: Distributed Transaction Atomicity
*For any* distributed transaction, either all participating shards commit or all abort
**Validates: Requirements 4.2**

### Property 18: Transaction Recovery Correctness
*For any* coordinator crash during commit, recovery should resolve all prepared transactions to consistent state
**Validates: Requirements 4.3**

### Property 19: Distributed Snapshot Isolation
*For any* transaction reading from multiple shards, all reads should see a consistent snapshot across shards
**Validates: Requirements 4.4**

### Property 20: Distributed Deadlock Resolution
*For any* deadlock involving multiple shards, the system should detect it and abort one transaction
**Validates: Requirements 4.5**

### Property 21: Rebalancing Plan Validity
*For any* rebalancing operation, the generated plan should account for all data and maintain total row count
**Validates: Requirements 5.1**

### Property 22: Rebalancing Read Availability
*For any* data being rebalanced, read queries should continue to succeed during the migration
**Validates: Requirements 5.2**

### Property 23: Migration Query Routing
*For any* query during rebalancing, the system should route to correct shard(s) considering both old and new locations
**Validates: Requirements 5.3**

### Property 24: Shard Map Update Atomicity
*For any* rebalancing completion, the shard map update should be atomic - queries see old or new map, never partial
**Validates: Requirements 5.4**

### Property 25: Rebalancing Rollback Safety
*For any* failed rebalancing operation, rolling back should restore the previous configuration without data loss
**Validates: Requirements 5.5**

### Property 26: Connection Pool Maintenance
*For any* shard node, the coordinator should maintain a connection pool with configured size limits
**Validates: Requirements 6.1**

### Property 27: Connection Reuse
*For any* query to a shard, if a pooled connection exists, it should be reused rather than creating a new one
**Validates: Requirements 6.2**

### Property 28: Connection Failure Handling
*For any* shard node becoming unreachable, all connections to that node should be marked invalid
**Validates: Requirements 6.3**

### Property 29: Pool Exhaustion Handling
*For any* connection pool exhaustion, the system should follow configured policy (queue or reject)
**Validates: Requirements 6.4**

### Property 30: Connection Recovery
*For any* recovered shard node, the connection pool should automatically restore connections
**Validates: Requirements 6.5**

### Property 31: Query Distribution Statistics
*For any* query workload, system views should accurately report query counts per shard
**Validates: Requirements 7.1**

### Property 32: Per-Shard Execution Logging
*For any* distributed query, execution logs should contain timing information for each shard
**Validates: Requirements 7.2**

### Property 33: Error Tracking Accuracy
*For any* shard error, the monitoring system should record the error with correct count and type
**Validates: Requirements 7.3**

### Property 34: Data Skew Detection
*For any* imbalanced data distribution, skew metrics should accurately reflect the imbalance
**Validates: Requirements 7.4**

### Property 35: Latency Tracking
*For any* cross-shard communication, the system should measure and report network latency
**Validates: Requirements 7.5**

### Property 36: Backup Consistency
*For any* distributed backup, all shards should be backed up at the same logical point in time
**Validates: Requirements 8.1**

### Property 37: Backup Metadata Completeness
*For any* backup, the shard map metadata should be included and restorable
**Validates: Requirements 8.2**

### Property 38: Restore Consistency
*For any* restore operation, all shards should be restored to the same consistent point in time
**Validates: Requirements 8.3**

### Property 39: Partial Shard Restore
*For any* single shard failure, restoring only that shard should produce a consistent database state
**Validates: Requirements 8.4**

### Property 40: PITR Coordination
*For any* point-in-time recovery, WAL replay should be coordinated to reach the same timestamp on all shards
**Validates: Requirements 8.5**

### Property 41: Distributed RLS Enforcement
*For any* row-level security policy, the policy should be enforced identically on all shards
**Validates: Requirements 9.1**

### Property 42: Credential Propagation
*For any* user authentication at coordinator, credentials should be correctly propagated to all accessed shards
**Validates: Requirements 9.2**

### Property 43: Permission Replication
*For any* permission grant on a sharded table, the permission should be replicated to all shard nodes
**Validates: Requirements 9.3**

### Property 44: SSL Enforcement
*For any* coordinator-to-shard connection when SSL is enabled, the connection should be encrypted
**Validates: Requirements 9.4**

### Property 45: Audit Log Completeness
*For any* distributed query, audit logs should include shard routing information
**Validates: Requirements 9.5**

### Property 46: Failure Detection Timeliness
*For any* shard node failure, detection should occur within the configured timeout period
**Validates: Requirements 10.1**

### Property 47: Automatic Failover
*For any* shard with replicas, failure should trigger automatic failover to a replica
**Validates: Requirements 10.2**

### Property 48: Shard Recovery Reintegration
*For any* recovered shard node, the system should reintegrate it into the shard map
**Validates: Requirements 10.3**

### Property 49: Coordinator Failover
*For any* coordinator failure, a standby coordinator should be promotable to primary
**Validates: Requirements 10.4**

### Property 50: Split-Brain Prevention
*For any* network partition, quorum-based decisions should prevent split-brain scenarios
**Validates: Requirements 10.5**

### Property 51: Split Candidate Identification
*For any* shard exceeding size threshold, the system should identify it as a split candidate
**Validates: Requirements 11.1**

### Property 52: Split Point Balance
*For any* shard split, the split point should divide data approximately evenly between old and new shards
**Validates: Requirements 11.2**

### Property 53: Split Read Availability
*For any* shard being split, read queries should continue to succeed during the split operation
**Validates: Requirements 11.3**

### Property 54: Split Write Routing
*For any* write during shard split, the write should be routed to the correct shard based on new boundaries
**Validates: Requirements 11.4**

### Property 55: Split Completion Atomicity
*For any* shard split completion, the shard map update should be atomic
**Validates: Requirements 11.5**

### Property 56: Hot Spot Detection
*For any* shard with skewed access patterns, the system should detect the hot spot and trigger splitting
**Validates: Requirements 11.6**

### Property 57: Split Prioritization
*For any* multiple shards needing splits, the system should prioritize based on size, growth rate, and load
**Validates: Requirements 11.7**

## Error Handling

### Connection Errors
- **Shard Unreachable**: Retry with exponential backoff, mark node degraded after threshold
- **Connection Pool Exhausted**: Queue requests or return error based on configuration
- **Authentication Failure**: Propagate error to client with shard node information

### Transaction Errors
- **Prepare Failure**: Abort transaction on all participants
- **Commit Timeout**: Coordinator retries commit, maintains prepared state for recovery
- **Deadlock**: Abort one transaction, return deadlock error to client

### Data Errors
- **Shard Key Violation**: Reject operation, return error indicating immutable shard key
- **Constraint Violation**: Return error from shard node to client
- **Rebalancing Conflict**: Queue operation or reject based on configuration

## Testing Strategy

### Unit Testing
- Shard map lookup and routing logic
- Hash function consistency and distribution
- Connection pool management
- Two-phase commit protocol state machine

### Property-Based Testing
The system will use **pgTAP** for property-based testing in PostgreSQL. Each correctness property will be implemented as a property-based test that:

1. Generates random test data (tables, shard configurations, queries, transactions)
2. Executes operations on the distributed system
3. Verifies the property holds across all test cases
4. Runs a minimum of 100 iterations per property

Example property test structure:
```sql
-- Property 6: Insert Routing Correctness
CREATE OR REPLACE FUNCTION test_insert_routing_correctness()
RETURNS SETOF TEXT AS $$
DECLARE
    test_table TEXT;
    shard_key_val INTEGER;
    expected_shard INTEGER;
    actual_shard INTEGER;
BEGIN
    -- Generate random sharded table
    test_table := create_random_sharded_table();
    
    -- Run 100 iterations
    FOR i IN 1..100 LOOP
        -- Generate random shard key value
        shard_key_val := floor(random() * 1000000)::INTEGER;
        
        -- Calculate expected shard
        expected_shard := calculate_target_shard(test_table, shard_key_val);
        
        -- Insert row
        EXECUTE format('INSERT INTO %I (shard_key, data) VALUES ($1, $2)', 
                      test_table) USING shard_key_val, 'test_data';
        
        -- Verify row is on correct shard
        actual_shard := get_row_shard(test_table, shard_key_val);
        
        RETURN NEXT ok(expected_shard = actual_shard,
                      format('Row with key %s routed to correct shard', shard_key_val));
    END LOOP;
    
    RETURN;
END;
$$ LANGUAGE plpgsql;
```

### Integration Testing
- End-to-end query execution across shards
- Distributed transaction commit and rollback
- Shard rebalancing with concurrent queries
- Failover and recovery scenarios

### Performance Testing
- Query latency with varying shard counts
- Transaction throughput with distributed commits
- Rebalancing impact on query performance
- Connection pool scalability

## Implementation Phases

### Phase 1: Core Infrastructure (Months 1-3)
- Shard map catalog tables and management
- Basic hash-based sharding
- Single-shard query routing
- Connection pool manager

### Phase 2: Distributed Queries (Months 4-6)
- Multi-shard query execution
- Result merging (sorting, aggregation)
- Query pushdown optimization
- Basic monitoring and statistics

### Phase 3: Distributed Transactions (Months 7-9)
- Two-phase commit implementation
- Distributed snapshot isolation
- Deadlock detection across shards
- Transaction recovery

### Phase 4: Rebalancing (Months 10-12)
- Rebalancing plan generation
- Online data migration
- Shard splitting
- Hot spot detection

### Phase 5: High Availability (Months 13-15)
- Shard replication and failover
- Coordinator failover
- Split-brain prevention
- Backup and recovery coordination

### Phase 6: Production Hardening (Months 16-18)
- Performance optimization
- Comprehensive testing
- Documentation
- Migration tools from non-sharded tables
