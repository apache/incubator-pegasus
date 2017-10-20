# Pegasus Java Client

## Build

```
mvn clean package -DskipTests
```

Currently libthrift-0.9.3 is a dependency of Java client. If it conflicts with your version, only modification on pom.xml is NOT ENOUGH. You may
need to regenerate java source files in src/main/java/dsn/{apps,replication}.

Please refer to [idl/recompile\_thrift.sh](idl/recompile_thrift.sh) for help.

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
tar xfz pegasus-client-${VERSION}-bin.tar.gz
cd pegasus-client-${VERSION}
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
```

You can provide a parameter of 'configPath' when creating a client instance.

The format of 'configPath' should be one of these:
* zk path: zk://host1:port1,host2:port2,host3:port3/path/to/config
* local file path: file:///path/to/config
* resource path: resource:///path/to/config

## PerfCounter

Custom tags can be configured for performance metrics monitor. You may add your favourite monitor system's client lib to pegasus java client.

The related configurations are:

```
enable_perf_counter = true
perf_counter_tags = k1=v1,k2=v2,k3=v3
```

## Document

For Pegasus Java API document, please refer to: [Java Document](https://github.com/XiaoMi/pegasus/wiki/Java%E5%AE%A2%E6%88%B7%E7%AB%AF%E6%96%87%E6%A1%A3)
