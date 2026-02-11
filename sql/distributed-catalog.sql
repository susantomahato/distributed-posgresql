postgres=# \d pg_shard_node
                      Table "pg_catalog.pg_shard_node"
     Column      |           Type           | Collation | Nullable | Default
-----------------+--------------------------+-----------+----------+---------
 nodename        | name                     |           | not null |
 nodeconnstr     | text                     | C         | not null |
 nodestate       | "char"                   |           |          |
 shardcount      | integer                  |           |          |
 lasthealthcheck | timestamp with time zone |           |          |
 createdat       | timestamp with time zone |           |          |
Indexes:
    "pg_shard_node_nodename_index" PRIMARY KEY, btree (nodename)

postgres=# \d pg_shard_map
                     Table "pg_catalog.pg_shard_map"
   Column    |           Type           | Collation | Nullable | Default
-------------+--------------------------+-----------+----------+---------
 shardid     | integer                  |           | not null |
 relid       | oid                      |           | not null |
 nodename    | name                     |           | not null |
 shardmethod | "char"                   |           | not null |
 rangemin    | text                     | C         |          |
 rangemax    | text                     | C         |          |
 hashmin     | integer                  |           |          |
 hashmax     | integer                  |           |          |
 shardstate  | "char"                   |           |          |
 createdat   | timestamp with time zone |           |          |
Indexes:
    "pg_shard_map_shardid_index" PRIMARY KEY, btree (shardid)
    "pg_shard_map_nodename_index" btree (nodename)
    "pg_shard_map_relid_index" btree (relid)

postgres=# \d pg_sharded_table
                   Table "pg_catalog.pg_sharded_table"
   Column    |           Type           | Collation | Nullable | Default
-------------+--------------------------+-----------+----------+---------
 relid       | oid                      |           | not null |
 shardkey    | text[]                   | C         | not null |
 shardmethod | "char"                   |           | not null |
 shardcount  | integer                  |           |          |
 createdat   | timestamp with time zone |           |          |
Indexes:
    "pg_sharded_table_relid_index" PRIMARY KEY, btree (relid)

postgres=# \d pg_dist_placement
                    Table "pg_catalog.pg_dist_placement"
     Column     |           Type           | Collation | Nullable | Default
----------------+--------------------------+-----------+----------+---------
 placementid    | integer                  |           | not null |
 shardid        | integer                  |           | not null |
 nodename       | name                     |           | not null |
 raftgroupid    | integer                  |           | not null |
 raftrole       | "char"                   |           | not null |
 placementstate | "char"                   |           | not null |
 raftterm       | bigint                   |           | not null |
 createdat      | timestamp with time zone |           | not null |
Indexes:
    "pg_dist_placement_placementid_index" PRIMARY KEY, btree (placementid)
    "pg_dist_placement_nodename_index" btree (nodename)
    "pg_dist_placement_raftgroupid_index" btree (raftgroupid)
    "pg_dist_placement_shardid_index" btree (shardid)

postgres=# \d pg_distributed_transaction
              Table "pg_catalog.pg_distributed_transaction"
    Column    |           Type           | Collation | Nullable | Default
--------------+--------------------------+-----------+----------+---------
 gid          | text                     | C         | not null |
 participants | text[]                   | C         | not null |
 txnstate     | "char"                   |           | not null |
 startedat    | timestamp with time zone |           | not null |
 preparedat   | timestamp with time zone |           |          |
Indexes:
    "pg_distributed_transaction_gid_index" PRIMARY KEY, btree (gid)

