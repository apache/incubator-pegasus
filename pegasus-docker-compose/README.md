# Pegasus with Docker Compose

This is a docker-compose solution to bootstrap a Pegasus cluster
on your machine.

## HOW TO

### Docker image

Before setting up the dockers, you should first have a docker image of a specfic version of Pegasus.
Check out the latest version 

```sh
git clone git@github.com:XiaoMi/pegasus.git

cd pegasus
./run.sh build -c # Compiling the code.
./run.sh pack_server # Packaging the binaries of Pegasus server.

./docker/build_docker.sh
```

> We will soon publish a docker image of Pegasus server 2.0.0.

### Set up a docker cluster

To start a cluster:

```sh
./start_onebox.sh
```

To start multiple clusters, you should reconfigure the `cluster_args.sh` by
using a different `CLUSTER_NAME` and `META_IP_PREFIX`. For example `onebox2` and `172.22.0`.

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

Ensure your Pegasus service failovers normally and no more failed request through Pegasus-Shell and error logs from your client tool.
