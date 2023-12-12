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
# Pegasus with Docker Compose

This is a docker-compose solution to bootstrap a Pegasus cluster
on your machine.

## HOW TO

### Docker image

Before setting up the dockers, you should first have a docker image of a specfic version of Pegasus.

Run compilation and package your binaries:

```sh
cd /your/local/apache-pegasus-source
./run.sh build -c # Compiling the code.
./run.sh pack_server # Packaging the binaries of Pegasus server.
```

Then build the image:

```
cd /your/local/pegasus-docker/pegasus-docker-compose
./build_docker.sh /your/local/apache-pegasus-source github-branch-for-build-env-image(default: master)
```

You will have a docker image called "pegasus:latest" right now built on you machine. Check it out:

```sh
docker images
```

### Set up a docker cluster

To start a cluster:

```sh
./start_onebox.sh
```

To start multiple clusters, you should reconfigure the `cluster_args.sh` by using a different set of:

- `CLUSTER_NAME`
- `META_IP_PREFIX`
- `META_PORT`.

For example `onebox2`, `173.21.0` and `35601`.

Use `docker ps` command to show all the running dockers. And if some docker failed unexpectedly,
use `docker logs {DOCKER_ID}` to dump the error logs of it.

### Configuration

`pegasus-docker/pegasus-docker-compose/config.min.ini` is the master-branch-Pegasus config. It may be not available
for Pegasus in a previously released version.

```sh
cd /your/local/pegasus-docker
git checkout 2.1.0
```

If you want to set up a previously released Pegasus cluster, please use the scripts from the specific branch.

### Cleanup a docker cluster

```sh
./clear_onebox.sh
```

This script will kill the cluster processes, remove the virtual network and finally remove the data directory.

## Using pumba to inject faults to Pegasus

[pumba](https://github.com/alexei-led/pumba) is a chaos testing and network emulation tool for Docker. We can use it
for fault injection of Pegasus, testing Pegasus's stability under various conditions
that pumba provides, including:

- docker pause
- network loss
- network corrupt
- network delay

To establish the testing environment, first set up a 3-replica, 2-meta onebox cluster. Modify your cluster_arg.sh file like this:

```sh
export META_COUNT=2
export REPLICA_COUNT=3
```

```sh
> ./start_onebox.sh

          Name                         Command                 State                         Ports
---------------------------------------------------------------------------------------------------------------------
onebox-docker_meta1_1       /entrypoint.sh meta              Up           0.0.0.0:34601->34601/tcp
onebox-docker_meta2_1       /entrypoint.sh meta              Up           0.0.0.0:34602->34601/tcp
onebox-docker_replica1_1    /entrypoint.sh replica           Up           0.0.0.0:32774->34801/tcp
onebox-docker_replica2_1    /entrypoint.sh replica           Up           0.0.0.0:32776->34801/tcp
onebox-docker_replica3_1    /entrypoint.sh replica           Up           0.0.0.0:32775->34801/tcp
onebox-docker_zookeeper_1   /docker-entrypoint.sh zkSe ...   Up           0.0.0.0:32777->2181/tcp, 2888/tcp, 3888/tcp
```

Run your client-side tool (Pegasus-YCSB, e.g.) to keep requesting to Pegasus service.

```sh
~/pegasus-YCSB/pegasus-YCSB-0.12.0-SNAPSHOT

> ./start.sh load
```

Begin injecting faults to onebox-docker:

```sh
pumba pause --duration=1h onebox-docker_replica1_1

pumba netem --duration=1h --tc-image 'gaiadocker/iproute2' loss --percent 100 onebox-docker_replica1_1
```

Ensure your Pegasus service failovers normally, with no unexpectedly failed requests and error logs from your client.
