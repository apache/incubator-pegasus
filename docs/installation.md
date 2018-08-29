Installing Pegasus
===========

Currently Pegasus can only be installed from source. Binary package and docker image is coming soon.

## Prerequisites and Requirements

### Hardware

Pegasus can be deployed in standalone mode which runs all the pegasus jobs as different processes on a single host, or in distributed mode which runs different jobs on different hosts. For distributed mode, more than one hosts are need:

* One or more hosts to run zookeeper. We need zookeeper to make the cluster metadata persistence. For a fault tolerant zookeeper cluster, at least 3 are necessary.
* One or more hosts to run Pegasus meta-server. At least 2 are needed for fault tolerance. You may share hosts between meta-server and zookeeper.
* At least 3 hosts to run Pegasus replica-server.

### Operating System requirements

* Linux: CentOS 7, Ubuntu 14.04(Trusty)
* macOS: not supported
* Microsoft Windows: not supported

## Build Pegasus from source

Please notice that Pegasus can not be built until the following packages meet the version requirements:

* Compiler: the gcc version must >= 4.8. If you use other compilers, please make sure that C++14 is supported.
* CMake: must >= 2.8.12
* boost: must >= 1.58

#### Install the dependencies

For Ubuntu:

```
sudo apt-get install build-essential cmake libboost-all-dev libaio-dev libsnappy-dev libbz2-dev libgflags-dev patch
```

For CentOS:
```
yum -y install cmake boost-devel libaio-devel snappy-devel bzip2-devel patch
```

Please make sure you install the proper version of GCC, CMake and Boost.

#### Build

1. clone Pegasus and its all subprojects:

   ```
   git clone https://github.com/xiaomi/pegasus.git --recursive
   ```
2. build

   ```
   cd pegasus && ./run.sh build
   ```
   if Boost is installed at some custom path , please tell the build script the install path of Boost:

   ```
   ./run.sh build -b /your/boost/installation/path
   ```

Generally, the build process of Pegasus consists of 4 parts:

1. Download the thirdparty dependencies to "rdsn/thirdparty" and build them, and the output headers/libraries/binaries are installed in "rdsn/thirdparty/output".
2. build the rdsn project, which is a dependency of our KV storage. The output headers/libraries/binaries are installed in "rdsn/builder/output". The build script will create a symbolic link "DSN_ROOT" in the pegasus project root.
3. build rocksdb in "rocksdb" dir, which is modified from [facebook/rocksdb](https://github.com/facebook/rocksdb)
4. build pegasus's KV-layer in "src".

**Please make sure the thirdparty are successfully downloaded and built before subsequent parts**.

## Run in standalone mode

You can play with pegasus with a **onebox** cluster:

```
./run.sh start_onebox
```

When running onebox cluster, all pegasus jobs are running in local host: 

* zookeeper: start a zookeeper process with port 22181
* meta-server: start several meta-server processes
* replica-server: start several replica-server processes

You can also check the state of onebox cluster:

```
./run.sh list_onebox
```

Stop the onebox cluster:

```
./run.sh stop_onebox
```

Clean the onebox cluster:

```
./run.sh clear_onebox
```

## Interactive shell

Pegasus provides a shell tool to interact with the cluster. To start the shell, run:
```
./run.sh shell
```

or specify different cluster meta-server address list:
```
./run.sh shell --cluster 127.0.0.1:34601,127.0.0.1:34602
```

Using the shell, you can do lots of things including:

* get cluster information and statistics
* create/drop/list tables (we name it app)
* get/set/del/scan data
* migrate replicas from node to node
* send remote commands to server
* dump mutation log and sstable files

## Running test

Run the unit test by:
```
./run.sh test
```

or run the killing test by:
```
./run.sh start_kill_test
```

## Benchmark

You can start a benchmark client quickly (needs onebox cluster already started):
```
./run.sh bench
```

or start a series of benchmark tests:
```
./scripts/pegasus_bench_run.sh
```

Attention: this bench tool depends on gflags, so it would not work if gflags library
is not installed when building Pegasus.

