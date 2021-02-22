# Disk-Migrater

## Overall
Disk-Migrater is for migrating data among different disk volumns at one node,  which is different from [node-balancer](http://pegasus.apache.org/administration/rebalance) that is for migrating data among different nodes. 

## Flow Process
Disk-Migrater operates by sending `RPC_REPLICA_DISK_MIGRATE` rpc to target node and goes on to trigger the node to migrate the `replica` from one disk to another. The whole migration process as follow: 

```
+---------------+      +---------------+       +--------------+
| client(shell) +------+ replicaServer +-------+  metaServer  |
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

* target node receive the migrate-rpc and start validate rpc arguments
* if the rpc is valid, node start migrate `replica` which contain `checkpoint`, `.init-info`,`.app-info`
* after data migrate successfuly, the origin `replica` will be closed and `replica-server` re-opens the new `replica`
* if the new `repica`  data is inconsistent with other replica(new write operation when migrating), it will be trigger to `learn` to catch up with the latest data by `meta-server`
* after the `learn` is completed, the `migration` is successful

## Replica States
In the process of migration, the `origin replica ` and `new replica` will have different states as follow
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
* if replica status is `primary`, you need assign `secondary`  manually via [propose](http://pegasus.apache.org/administration/rebalance)
* any process is failed, the operation will be failed and reverted the `IDEL` status

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

It's noticed that the migration is manual, and  we hope  the future work is  `admin-cli` can create `whole disk balance plan/step` and then automatically migrate data to balance all disk as much as possible 