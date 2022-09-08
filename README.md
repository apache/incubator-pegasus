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

[github-release]: https://github.com/apache/incubator-pegasus/releases
<!-- markdown-link-check-disable -->
[PacificA]: https://www.microsoft.com/en-us/research/publication/pacifica-replication-in-log-based-distributed-storage-systems/
<!-- markdown-link-check-enable-->
[pegasus-rocksdb]: https://github.com/xiaomi/pegasus-rocksdb
[hbase]: https://hbase.apache.org/
[website]: https://pegasus.apache.org

![pegasus-logo](https://github.com/apache/incubator-pegasus-website/blob/master/assets/images/pegasus-logo-inv.png)

[![Lint and build regularly](https://github.com/apache/incubator-pegasus/actions/workflows/regular-build.yml/badge.svg)](https://github.com/apache/incubator-pegasus/actions/workflows/regular-build.yml)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Releases](https://img.shields.io/github/release/apache/incubator-pegasus.svg)][github-release]

**Note**: The `master` branch may be in an *unstable or even in a broken state* during development.
Please use [GitHub Releases][github-release] instead of the `master` branch in order to get stable binaries.

Apache Pegasus is a distributed key-value storage system which is designed to be:

- **horizontally scalable**: distributed using hash-based partitioning
- **strongly consistent**: ensured by [PacificA][PacificA] consensus protocol
- **high-performance**: using [RocksDB][pegasus-rocksdb] as underlying storage engine
- **simple**: well-defined, easy-to-use APIs

## Background

Pegasus targets to fill the gap between Redis and [HBase][hbase]. As the former
is in-memory, low latency, but does not provide a strong-consistency guarantee.
And unlike the latter, Pegasus is entirely written in C++ and its write-path
relies merely on the local filesystem.

Apart from the performance requirements, we also need a storage system
to ensure multiple-level data safety and support fast data migration
between data centers, automatic load balancing, and online partition split.

## Features

- **Persistence of data**: Each write is replicated three-way to different ReplicaServers before responding to the client. Using PacificA protocol, Pegasus has the ability for strong consistent replication and membership changes.

- **Automatic load balancing over ReplicaServers**: Load balancing is a builtin function of MetaServer, which manages the distribution of replicas. When the cluster is in an inbalance state, the administrator can invoke a simple rebalance command that automatically schedules the replica migration.

- **Cold Backup**: Pegasus supports an extensible backup and restore mechanism to ensure data safety. The location of snapshot could be a distributed filesystem like HDFS or local filesystem. The snapshot storing in the filesystem can be further used for analysis based on [pegasus-spark](https://github.com/pegasus-kv/pegasus-spark).

- **Eventually-consistent intra-datacenter replication**: This is a feature we called *duplication*. It allows a change made in the local cluster accesible after a short time period by the remote cluster. It help achieving higher availability of your service and gaining better performance by accessing only local cluster.

## To start using Pegasus

See our documentation on the [Pegasus Website][website].

## Client drivers

Pegasus has support for several languages:

- [Java](https://github.com/apache/incubator-pegasus/blob/master/java-client)
- [C++](https://github.com/apache/incubator-pegasus/blob/master/src/include/pegasus/client.h)
- [Go](https://github.com/apache/incubator-pegasus/blob/master/go-client)
- [Python](https://github.com/apache/incubator-pegasus/blob/master/python-client)
- [Node.js](https://github.com/apache/incubator-pegasus/blob/master/nodejs-client)
- [Scala](https://github.com/apache/incubator-pegasus/blob/master/scala-client)

## Contact us

- Send emails to the Apache Pegasus developer mailing list: `dev@pegasus.apache.org`. This is the place where topics around development, community, and problems are officially discussed. Please remember to subscribe to the mailing list via `dev-subscribe@pegasus.apache.org`.

- GitHub Issues: submit an issue when you have any idea to improve Pegasus, and when you encountered some bugs or problems.

## Related Projects

Test tools:

- [Java YCSB](https://github.com/xiaomi/pegasus-YCSB)

Data import/export tools:

- [DataX](https://github.com/xiaomi/pegasus-datax)

## License

Copyright 2022 The Apache Software Foundation. Licensed under the Apache License, Version 2.0:
[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)
