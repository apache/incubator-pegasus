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

# Pegasus Java Client

## Build

```
cd scripts && sh recompile_thrift.sh && cd -
mvn spotless:apply
mvn clean package -DskipTests
```

## Install

```
cd scripts && sh recompile_thrift.sh && cd -
mvn spotless:apply
mvn clean install -DskipTests
```

## Test

To run test, you should start pegasus onebox firstly, and run test as:

```
cd scripts && sh recompile_thrift.sh && cd -
mvn spotless:apply
mvn clean package
```

or specify one test:

```
cd scripts && sh recompile_thrift.sh && cd -
mvn spotless:apply
mvn clean package -Dtest=TestPing
```

## Configuration

Configure client by "pegasus.properties", for example:

```
meta_servers = 127.0.0.1:34601,127.0.0.1:34602,127.0.0.1:34603
operation_timeout = 1000
async_workers = 4
enable_perf_counter = true
perf_counter_tags = k1=v1,k2=v2,k3=v3
push_counter_interval_secs = 10
```

You can provide a parameter of 'configPath' when creating a client instance.

The format of 'configPath' should be one of these:
* zk path: zk://host1:port1,host2:port2,host3:port3/path/to/config
* local file path: file:///path/to/config
* resource path: resource:///path/to/config

## PerfCounter(Metrics)

Pegasus Java Client supports QPS and latency statistics of requests.

The related configurations are:

```
enable_perf_counter = true
perf_counter_tags = k1=v1,k2=v2,k3=v3
push_counter_interval_secs = 10
```

For each type of request(get, set, multiset, etc.), we collect 8 metrics:
1. cps-1sec: the request's qps
2. cps-1min: the request's queries per 1 minute
3. cps-5min: the request's queries per 5 minutes
4. cps-15min: the request's queries per 15 minutes
5. latency-p50: the moving median of request's queries
6. latency-p99: the moving p99 of request's queries
7. lantecy-p999: the moving p999 of request's queries
8: latency-max: the moving max of request's queries

We use io.dropwizard.metrics library to calculate the request count.

Currently, metrics are integrated with open-falcon(https://open-falcon.org/),
<!-- markdown-link-check-disable -->
which push counters to local http agent http://127.0.0.1:1988/push/v1.
<!-- markdown-link-check-enable-->

If you'd like to integrate pegasus client with other monitor system, please let us know ASAP.
