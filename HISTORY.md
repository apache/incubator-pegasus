# Pegasus Change Log

## 2.5.0

### Behavior changes
* Use official releases of RocksDB for Pegasus, instead of the modified versions based on a fork of it due to some historical reasons. Since Pegasus 2.1.0, actually most of the modifications have been removed, while few of them were left to keep backward compatible. Pegasus 2.5.0 would turn to official release, thus it should be noted that to use 2.5.0, the server must be upgraded from 2.1, 2.2, 2.3 or 2.4, to ensure that in MANIFEST files there are no introduced tags by the modified versions which could not be recognized by the official releases. [#1048](https://github.com/apache/incubator-pegasus/pull/1048)
* Now the logs in servers and C++ clients are in increase severity order of `DEBUG`, `INFO`, `WARNING`, `ERROR` and `FATAL`, which means the inverse order between `DEBUG` and `INFO` has been corrected. [#1200](https://github.com/apache/incubator-pegasus/pull/1200)
* All shared log files would be flushed and removed for garbage collection, while before 2.5.0 there is at least 1 shared log file which is never removed, though long before that the private logs were written as WAL instead of shared logs. [#1594](https://github.com/apache/incubator-pegasus/pull/1594)
* No longer support EOL OS versions, including Ubuntu 16.04 and CentOS 6. [#1553](https://github.com/apache/incubator-pegasus/pull/1553), [#1557](https://github.com/apache/incubator-pegasus/pull/1557)

### New Features
* Add a new ACL based on Apache Ranger to provide fine-grained access control to global-level, database-level and table-level resources. On the other hand, it is also compatible with the old coarse-grained ACL. [#1054](https://github.com/apache/incubator-pegasus/issues/1054)
* Add support to query and update table-level RocksDB options at runtime, where currently only `num_levels` and `write_buffer_size` are supported; other options would be added gradually, if necessary. [#1511](https://github.com/apache/incubator-pegasus/pull/1511)
* Add a new `rename` command for cpp-shell, allowing users to rename a table. [#1272](https://github.com/apache/incubator-pegasus/pull/1272)
* Add a configuration `[network] enable_udp` to control if UDP service is started. The service would not be started when set to false. [#1132](https://github.com/apache/incubator-pegasus/pull/1132)
* Add support to dump the statistical information while using `jemalloc`. [#1133](https://github.com/apache/incubator-pegasus/pull/1133)
* Support `success_if_exist` option for the interface of creating table to cpp-shell, java and go clients. [#1148](https://github.com/apache/incubator-pegasus/pull/1148)
* Add a new interface `listApps` to the Java client to list all tables. [#1471](https://github.com/apache/incubator-pegasus/pull/1471) 
* Add a new option `[replication] crash_on_slog_error` to make it possible to exit the replica server if the shared log failed to be replayed, instead of trashing all the replicas on the server. [#1574](https://github.com/apache/incubator-pegasus/pull/1574)
* Pegasus could be built on more platforms: Ubuntu 22.04/Clang 14, M1 MacOS. [#1350](https://github.com/apache/incubator-pegasus/pull/1350), [#1094](https://github.com/apache/incubator-pegasus/pull/1094)
* Pegasus could be developed and built in a docker environment via VSCode(https://code.visualstudio.com/docs/devcontainers/containers), which is more friendly to newbies. [#1544](https://github.com/apache/incubator-pegasus/pull/1544)

### Performance Improvements
* Improve the performance of `count_data` of cpp-shell by only transferring the number of records rather than the real data. [#1091](https://github.com/apache/incubator-pegasus/pull/1091)

### Bug fixes
* Fix a bug that the RocksDB library is not built in **Release** version, which may cause terrible performance issues. [#1340](https://github.com/apache/incubator-pegasus/pull/1340)
* Fix a bug in the Go client that the `startScanPartition()` operation could not be performed correctly if some partitions was migrated. [#1106](https://github.com/apache/incubator-pegasus/pull/1106)
* Fix a bug that some RockDB options could not be loaded correctly if updating the config file and restarting the replica server. [#1108](https://github.com/apache/incubator-pegasus/pull/1108)
* Create a `stat` table to avoid errors reported from cpp-collector. [#1155](https://github.com/apache/incubator-pegasus/pull/1155)
* Fix bugs where wrong error code is passed to callback and directory is not rolled back correctly for failure while resetting the mutation log with log files of another directory. [#1208](https://github.com/apache/incubator-pegasus/pull/1208)
* Fix a bug in admin-cli that reports an incorrect error `doesn't have enough space` when executing the `partition-split start` command. [#1289](https://github.com/apache/incubator-pegasus/pull/1289)
* Fix a bug in Java client that the `batchGetByPartitions()` API may throw `IndexOutOfBoundsException` exception if partial partitions get response failed. [#1411](https://github.com/apache/incubator-pegasus/pull/1411)
* Trash the corrupted replica to the path `<app_id>.<pid>.pegasus.<timestamp>.err` when the RocksDB instance reports corruption error while executing write operations instead of leaving it in the same place, to avoid endless corruption errors. Also trash the corrupted replica to that path when corruption errors occur while executing read operations (instead of doing nothing). The trashed replica can be recovered automatically in a cluster deployment, or must be repaired manually in singleton deployment. [#1422](https://github.com/apache/incubator-pegasus/pull/1422)
* Fix a bug of replica server crash if `.init-info` file is missing. [#1428](https://github.com/apache/incubator-pegasus/pull/1428)
* Fix a bug of the Go client may hang if some replica servers are down. [#1444](https://github.com/apache/incubator-pegasus/pull/1444)
* Fix a bug that the replica server would crash when opening a replica with a corrupted RocksDB instance, whose directory will be marked as trash then. [#1450](https://github.com/apache/incubator-pegasus/issues/1450)
* Fix a bug that there is no interval between two attempts once `ERR_BUSY_CREATING` or `ERR_BUSY_DROPPING` error is received while creating or dropping a table by cpp-shell. [#1453](https://github.com/apache/incubator-pegasus/pull/1453)
* Fix a bug of the replica server crash in the Ingestion procedure of Bulk Load data. [#1563](https://github.com/apache/incubator-pegasus/pull/1563)
* Mark the data directory as failed and stop assigning new replicas to it once IO errors are found for it. [#1383](https://github.com/apache/incubator-pegasus/issues/1383)
