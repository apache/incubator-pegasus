# Roadmap

This document defines the roadmap for Pegasus.

#### __API__

- [x] Rich APIs for single key access: set, get, del, exist, TTL
- [x] Batch APIs for multiple range keys access within a hashkey
- [x] Async APIs
- [x] Java Client
- [x] Scan operator within hashkey & across hashkey
- [ ] Native clients for other programming languages
- [ ] RESTful API
- [ ] Redis API support
- [ ] Quota
- [ ] Transactions across hashkeys
- [ ] Distributed SQL

#### __Replica Server__

- [x] PacificA consensus algorithm
- [x] Fast learning process for learners
- [x] Rocksdb storage engine
- [ ] Raft consensus algorithm
- [ ] Partition split

#### __Meta Server__

- [x] Table management: create/drop/recall
- [x] Persistence of metadata to zookeeper
- [x] Load balancer according to replica count & replica distribution on disks of nodes
- [ ] Load balancer according to replica size & capacity & rw load
- [ ] Persistence of metadata by raft consensus

#### __Data Security__

- [ ] Replication across data center
- [ ] Cold backup based on snapshots

#### __Admin Tools__

- [x] Command line tool
- [x] Rich metrics for monitoring
- [ ] Web admin
