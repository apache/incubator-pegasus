# Pegasus Java Client

[![Build Status](https://travis-ci.org/XiaoMi/pegasus-java-client.svg?branch=thrift-0.11.0-inlined)](https://travis-ci.org/XiaoMi/pegasus-java-client)

## Build

```
mvn clean package -DskipTests
```

## Install

```
mvn clean install -DskipTests
```

## Test

To run test, you should start pegasus onebox firstly, and run test as:

```
mvn clean package
```

or specify one test:

```
mvn clean package -Dtest=TestPing
```

## PegasusCli

PegasusCli is a tool to set/get/del data from pegasus.

This tool is also deployed to Nexus, from which you can download it.

Or you can build it from source as following:

```
mvn clean package -DskipTests
cd target/
tar xfz pegasus-client-${VERSION}-thrift-0.11.0-inlined-bin.tar.gz
cd pegasus-client-${VERSION}-thrift-0.11.0-inlined
./PegasusCli
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

Currently, metrics are integrated with open-falcon(http://open-falcon.com/), 
which push counters to local http agent http://127.0.0.1:1988/push/v1. 

If you'd like to integrate pegasus client with other monitor system, please let us know ASAP.

## Document

For Pegasus Java API document, please refer to: https://github.com/XiaoMi/pegasus/wiki/Java%E5%AE%A2%E6%88%B7%E7%AB%AF%E6%96%87%E6%A1%A3
