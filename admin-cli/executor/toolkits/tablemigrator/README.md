# Table-Migrator

Table-Migrator helps users easily and automatically  online-migrate table from one cluster to another. The client side does not even need to restart and 
the server side can complete the migration of the entire table and continue to provide services to the client through the new cluster. There are a few points to note: 


- Table Migrator depends on the [duplication](https://pegasus.apache.org/administration/duplication) feature of the server, so please upgrade to pegasus-server 2.4
- Since the `duplication ` only supports pegasus-data with v1, only tables with v1 are supported table migration. Otherwise, an 'not support' error will be returned
- There will be a short write reject time (in minutes level) when migrating data. Please evaluate whether this restrictive measure is tolerated
- **After the table migration is completed, the tool supports triggering the client to automatically switch to a new cluster. However, this function requires that the client must 
   access the cluster through metaproxy. Of course, this function is optional. Users can also manually change the client configuration and restart the client after migration**

The entire table migration process includes the following steps:
- Check the data version number of the current table. Only v1 supports table data migration. Otherwise, an error is returned.
- Create `duplication` task for the target cluster. The task will first migrate the chekpoint data, and then start to migrate the incremental data via plog
- Block waiting until the unsynchronized incremental data drops to a lower level, and then prohibit the write request of the source cluster to fully synchronize all the remaining incremental data
- Continue to block and wait until the number of synchronization requests decreases to 0, indicating that all incremental data has been synchronized
- **If you configure metaproxy, the tool will automatically switch the target cluster of the client to a new cluster. Otherwise, it will end directly**


# Usage

`-t | --table`: name of the table to be migrated
`-n | --node`: the zookeeper address configured by the metaproxy. If it is not specified, the zookeeper address configured by the current cluster will be used by default. Please check the metaproxy service to confirm the correct address
`-r | -- root`: the zookeeper root path of the metaproxy configuration. If it is not specified, it means that you are not going to use metaproxy to complete the automatic switching of the client cluster
`-c | --cluster`: name of the target cluster
`-m | --meta`: meta address of the target cluster
`-p | --threshold`: the threshold value of the number of remaining incremental data pieces. When the threshold value is reached, write prohibition will be enabled for the source cluster. The default value is 10K
