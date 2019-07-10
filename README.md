[github-release]: https://github.com/XiaoMi/pegasus/releases
[PacificA]: https://www.microsoft.com/en-us/research/publication/pacifica-replication-in-log-based-distributed-storage-systems/
[pegasus-rocksdb]: https://github.com/xiaomi/pegasus-rocksdb
[facebook-rocksdb]: https://github.com/facebook/rocksdb
[hbase]: https://hbase.apache.org/
[website]: https://pegasus-kv.github.io

![pegasus-logo](docs/media-img/pegasus-logo.png)

[![Build Status](https://travis-ci.org/XiaoMi/pegasus.svg?branch=master)](https://travis-ci.org/XiaoMi/pegasus)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Releases](https://img.shields.io/github/release/xiaomi/pegasus.svg)][github-release]

**Note**: The `master` branch may be in an *unstable or even broken state* during development.
Please use [releases][github-release] instead of the `master` branch in order to get stable binaries.

Pegasus is a distributed key-value storage system which is designed to be:

- **horizontally scalable** distributed using hash-based partitioning
- **strongly consistent**: ensured by [PacificA][PacificA] consensus protocol
- **high-performance**: using [RocksDB][pegasus-rocksdb] as underlying storage engine
- **simple**: well-defined, easy-to-use APIs

Pegasus has been widely-used in XiaoMi and serves millions of requests per second.
It is a mature, active project. We hope to build a diverse developer and user
community and attract contributions from more people.

## Background

HBase was recognized as the only large-scale KV store solution in XiaoMi
until Pegasus came out in 2015 to solve the problem of high latency
of HBase because of its Java GC and RPC overhead of the underlying distributed filesystem.

Pegasus targets to fill the gap between Redis and HBase. As the former
is in-memory, low latency, but does not provide a strong-consistency guarantee.
And unlike the latter, Pegasus is entirely written in C++ and its write-path
relies merely on the local filesystem.

Apart from the performance requirements, we also need a storage system
to ensure multiple-level data safety and support fast data migration
between data centers, automatic load balancing, and online partition split.

After investigating the existing storage systems in the open source world,
we could hardly find a suitable solution to satisfy all the requirements.
So the journey of Pegasus begins.

## To start using Pegasus

See our documentation on [Pegasus Website][website].

## Related Projects

Submodules:

- [rDSN](https://github.com/xiaomi/rdsn)
- [RocksDB](https://github.com/xiaomi/pegasus-rocksdb)

Client libs:

- [Java client](https://github.com/xiaomi/pegasus-java-client)
- [Python Client](https://github.com/xiaomi/pegasus-python-client)
- [Go Client](https://github.com/xiaomi/pegasus-go-client)
- [Node.js Client](https://github.com/xiaomi/pegasus-nodejs-client)
- [Scala Client](https://github.com/xiaomi/pegasus-scala-client)

Test tools:

- [Java YCSB](https://github.com/xiaomi/pegasus-YCSB)
- [Go YCSB](https://github.com/xiaomi/pegasus-YCSB-go)

Data import/export tools:

- [DataX](https://github.com/xiaomi/pegasus-datax)

## Contact

- Gitter: <https://gitter.im/XiaoMi/Pegasus>
- Issues: <https://github.com/XiaoMi/pegasus/issues>

## License

Copyright 2015-now Xiaomi, Inc. Licensed under the Apache License, Version 2.0:
<http://www.apache.org/licenses/LICENSE-2.0>
