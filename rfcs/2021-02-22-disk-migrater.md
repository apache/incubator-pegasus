Disk-Migrater is for migrating data among different disk volumns at one node,  which is different from [node-balancer](http://pegasus.apache.org/administration/rebalance) that is for migrating data among different nodes. 

Disk-Migrater operates by sending `RPC_REPLICA_DISK_MIGRATE` rpc to target node and goes on to trigger the node to migrate the `replica` from one disk to another. The whole migration process as follow: 

```
+---------------+      +---------------+       +--------------+
| client(shell) +------+ replicaServer +-------+  metaServer  |
+------+--------+      +-------+-------+       +-------+------+
       |                       |                       |
       |                       |                       |
       |                       |                       |
       |                       |                       |
       |                       |                       |
       |                       |                       |
       |                       |                       |
       |                       |                       |
       |                       |                       |
       |                       |                       |
       |                       |                       |
       |                       |                       |
       |                       |                       |
       |                       |                       |
       |                       |                       |
       v                       v                       v
```

* target node receive the migrate-rpc and start validate rpc arguments
* if the rpc is valid, node start migrate `replica` which contain `checkpoint`, `.init-info`,`.app-info`
* after data migrate successfuly, the origin `replica` will be closed and `replica-server` re-opens the new `replica`
* if the `repica`  data is inconsistent with other replica, it will be trigger the to `learn` to catch up with the latest data by `meta-server`
* after the `learn` is completed, the `migration` is successful



creating a plan and goes on to execute that plan on the datanode. A plan is a set of statements that describe how much data should move between two disks. A plan is composed of multiple move steps. A move step has source disk, destination disk and number of bytes to move. A plan can be executed against an operational data node. Disk balancer should not interfere with other processes since it throttles how much data is copied every second. Please note that disk balancer is not enabled by default on a cluster. To enable diskbalancer dfs.disk.balancer.enabled must be set to true in hdfs-site.xml.