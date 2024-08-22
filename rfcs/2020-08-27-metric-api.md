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

# Metric API

## Summary

This RFC proposes a new metric API in replace of the old perf-counter API.

## Motivation

The perf-counter API has bad naming convention to be parsed and queried over the external monitoring system like [Prometheus](https://prometheus.io/), or [open-falcon](https://github.com/open-falcon).

Here are some examples of the perf-counter it exposes:

- `replica*eon.nfs_client*recent_copy_data_size`
- `zion*profiler*RPC_RRDB_RRDB_MULTI_GET.latency.server.p999`
- `collector*app.pegasus*app.stat.put_qps#xiaomi_algo_browser`
- `replica*app.pegasus*check_and_mutate_qps@15.34`

Apart from the perf-counter name, there are 3 "tags" for each counter: 

```
replica*eon.nfs_client*recent_copy_data_size | service=pegasus, job=replica, port=35801
```

From our experiences, this naming rules have the problems listed below:

- The name includes invalid characters for Prometheus: '*', '.', '@', '#'. While they are legal in open-falcon, in order to support Prometheus we must convert all these characters to '_'. It causes that Pegasus has to expose different styles of counter name to the two monitoring systems.

- Strange and meaningless words like "zion", "eon" are included in the counter name. 

- Information like "table name", "replica id" are weirdly encoded into the counter name following some obsecure rules, such as "`#<table name>`" and "`@<replica id>`". This increases the difficulty on the engineering of perf-counters, that needs additional parsing.

## Design

### Naming

Firstly, let's take a look on the new naming. For the above perf-counters:

- `​nfs_client_recent_copy_data_size | entity=server`
- `​RPC_RRDB_RRDB_MULTI_GET_server_latency | ​p=999,entity=server`
- `​put_qps | table=xiaomi_algo_browser,entity=table`
- `​check_and_mutate_qps | table=13,partition=34,entity=replica`

What's changed:

- I apply [the naming rules of prometheus](https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels), only use underscores '_' for word separator.

- Better utilization of "tags"/"labels" (optional key-value pairs for attributes of each metric).
  - "`#<table name>`" is replaced with tag "`table=<table name>`"
  - "`@<replica id>`" is replaced with tags "`table=<table id>,partition=<partition id>`".

- I introduce a new tag called "entity", which is used to identify the level of the perf-counter. There are 3 types of entity in Pegasus:
  - entity=server
  - entity=table
  - entity=replica

### API

As the naming is changed now, the perf-counter API must be correspondingly evolved. That is the new metric API.

To declare a perf-counter, take `get_qps` of a replica as an example, what was it like:

```cpp
class rocksdb_store {
private:
  perf_counter_wrapper _get_qps;
};

rocksdb_store::rocksdb_store() {
  _get_qps = init_app_counter(
        "app.pegasus",
        fmt::format("get_qps@{}", str_gpid),
        COUNTER_TYPE_RATE,
        "statistic the qps of GET request");
}
```

After using the new API:

```cpp
​METRIC_DEFINE_meter(server, get_qps, kRequestsPerSecond, "the qps of GET requests")

METRIC_DEFINE_entity(replica);

class rocksdb_store {
private:
    meter_ptr _get_qps;
};

rocksdb_store::rocksdb_store() {
  _replica_entity = METRIC_ENTITY_replica.instantiate(str_gpid, {
      { "table" : get_gpid().app_id() },
      { "partition" : get_gpid().partition_index() },
  });

  _get_qps = ​METRIC_get_qps.instantiate(replica_entity);
}
```

The code might be seemingly more complicated, but in fact it's simpler:

1. Each instance of `METRIC_ENTITY_replica` is attributed with a replica's ID. Instantiated once, the instance (`_replica_entity`) can be used to create every metric belongs to this replica, compared to old API, where every perf-counter needs to encode ID into its name (`fmt::format("get_qps@{}", str_gpid)`).

2. "app.pegasus" in the old API is called a "section". We now use entity instead. Remembering what a section name means is not friendly to new-comers.

3. Metric definition and metric instantiation is decoupled. A metric is defined in global scope, its instantiation in the constructor of `rocksdb_store` is reduced to one line of code.

The API is inspired by [kudu metrics](https://github.com/apache/kudu/blob/master/src/kudu/util/metrics.h), which is a well-documented and mature implementation of metrics in C++.

## Notes

The documentations and monitoring templates must adapt to the latest metric name when this refactoring is released.
