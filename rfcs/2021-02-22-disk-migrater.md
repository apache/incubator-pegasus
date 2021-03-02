# Disk-Migrater

## Design Goals
Disk-Migrater is for migrating data among different local disks within one node. This feature is different from node-rebalance that is for migrating data among different nodes. 

## Flow Process
Disk-Migrater operates by sending `RPC_REPLICA_DISK_MIGRATE` rpc to the targeted node that triggers the node to migrate the specified `replica` from one disk to another. The whole migration process is as follow: 

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
2. If the RPC is valid, node starts migrating the specified `replica`.
3. After replica migration finishes successfuly, the original `replica` will be closed and ReplicaServer re-opens the new `replica`.
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
* If replica status is `primary`, you need assign it `secondary`  manually via [propose](http://pegasus.apache.org/administration/rebalance).
* Any process is failed, the operation will be failed and reverted the `IDEL` status.

## Client Command
The `client` sending rpc now is [admin-cli](https://github.com/pegasus-kv/admin-cli) which support `query disk info` and `migrate disk replica`, the command like this(`help` can see the detail command ):
```
# query replica capacity
disk-capacity -n node -d disk
# query replica count
disk-replica -n node -d disk
# migrate data
disk-migrate -n node -g gpid -f disk1 -t disk2 
```

It's noticed that the migration is manual, and  we hope  the future work is  `admin-cli` can create `whole disk balance plan/step` and then automatically migrate data to balance all disk as much as possible .
