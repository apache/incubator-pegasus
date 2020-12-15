# Meta Proxy

Previously, Pegasus offered every user with a list of MetaServer IP:Port addresses. Client queries MetaServer to locate the ReplicaServer where a specific data partition is hosted, and thereby it can read/write. 

But static IP is unsafe for a service, especially when we need to replace a MetaServer node. One solution is to use domain name to access MetaServer.

It's still unrealistic to manage one domain name per cluster. A better approach is to use one domain name for multiple clusters and hide the detail of clusters from users.

One pratical design for this idea is to implement a proxy service, that could "intelligently" routing the client queries to the matched MetaServer. This service we call it MetaProxy.

Another case of MetaProxy is the dynamic and senseless reconfiguration of the primary cluster during duplication. While the primary cluster is crashed, the proxy can forward client queries to the backup cluster for disaster recovery.

The implementation of MetaProxy is a service that could process Pegasus's RPC protocol. It could receive and parse the RPC request from clients. After forwarding the query to the corresponding MetaServer, it needs to return the response completely to the client.

Every client query, ie. QueryConfig RPC, needs to specify a table name in order to retrieve the addresses of partitions in the table. 

```thrift
struct query_config_request
{
    1:string app_name;
    2:list<i32> partition_indices;
}

struct query_cfg_response
{
    1:base.error_code err;
    2:i32 app_id;
    3:i32 partition_count;
    4:bool is_stateful;
    5:list<partition_configuration> partitions;
}
```

MetaProxy requires the exactly one-one mapping of table and cluster, i.e. one table can exist in only one globally unique cluster. Thus MetaProxy can certainly find the cluster using the table name.
The one-one mapping should be guaranteed by the Pegasus service administrator.

The cluster-to-table mapping is stored in MetaServer. To find cluster by table, we need to store the reversed mapping in Zookeeper.

The zk layout:

```
<root>/c3srv-pegasus/<table>

{
  "cluster" : "c3srv-ad",
  "meta-addrs" : ["127.0.0.1:34601"]
}
```

All tables under one MetaProxy deployment are unique. It's impossible for two tables with the same name. However, we can deploy two sets of MetaProxy in two isolated AZs, then they can both have the same table. And consequently, we can achieve double-alive with intra-AZs.
