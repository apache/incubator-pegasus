<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Disk-Migrator

## Design Goals
Disk-Migrator is used for migrating data among different local disks within one node. This feature is different from node-rebalance that is for migrating data among different nodes. 

## Flow Process
Disk-Migrator operates by sending `RPC_REPLICA_DISK_MIGRATE` rpc to the targeted node that triggers the node to migrate the specified replica from one disk to another. The whole migration process is as follow: 

```
+---------------+      +---------------+       +--------------+
| Client(shell) +------+ replicaServer +-------+  metaServer  |
+------+--------+      +-------+-------+       +-------+------+
       |                       |                       |
       +------migrateRPC-----> +-----IDLE              |
       |                       |       | (validate rpc)|
       |                       |     MOVING            |
       |                       |       | (migrate data)|
       |                       |     MOVED             |
       |                       |       | (rename dir)  |
       |                       |     CLOSED            |
       |                       |       |               |
       |                +----- +<----LEARN<------------+
       |                |      |                       |
       |                |      |                       |
       |           LearnSuccess|                       |
       |                |      |                       |
       |                |      |                       |
       |                +----->+                       |
```

1. The targeted node receives the migrateRPC and starts validating the request arguments.
2. If the RPC is valid, node starts migrating the specified replica.
3. After replica migration finishes successfuly, the original replica will be closed and ReplicaServer re-opens the new replica.
4. If the new replica's data is inconsistent with its primary, MetaServer will automatically start to trigger replica-learn to catch up with the latest data.
5. After the learning process is completed, the entire disk-migration ends.

## Replica States
In the process of migration, the original replica and the new replica will have different states as follow:
| process  |origin replica status[dir name]  | new replica status[dir name]   |
|---|---|---|
|IDEL  |primary/secondary[gpid.pegasus]  |--[--]   |
|START   |secondary[gpid.pegasus]  |--[--]   |
|MOVING   |secondary[gpid.pegasus]   |--[gpid.pegasus.disk.migrate.tmp]   |
|MOVED   |secondary[gpid.pegasus]   |--[gpid.pegasus.disk.migrate.tmp]   | 
|CLOSED   |error[gpid.pegasus.disk.migrate.ori]   |--[gpid.pegasus]   |
|LEARNING   |error[gpid.pegasus.disk.migrate.ori]   |potential_secondary[gpid.pegasus] |
| COMPLETED  |error[gpid.pegasus.disk.migrate.ori]   |secondary[gpid.pegasus]   |

**Note:** 
* If replica status is `primary`, you need assign it as `secondary` manually via [propose](http://pegasus.apache.org/administration/rebalance).
* Once any process is failed, the operation will be failed and reverted the `IDEL` status.

## Client Command
The client sending migrateRPC is [admin-cli](https://github.com/pegasus-kv/admin-cli) which supports `disk-capacity`, `disk-replica` and `disk-migrate` commands. 

Use `help` to see the command manuals. For example:
```
# query replica capacity
disk-capacity -n node -d disk
# query replica count
disk-replica -n node -d disk
# migrate data
disk-migrate -n node -g gpid -f disk1 -t disk2 
```

It should be noticed that disk migration is currently a manual operation. It's in our future plan to design a disk-rebalance planner. It can generate a series of steps, which automatically migrate data and eventually make all disks balanced.
