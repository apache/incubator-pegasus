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
# admin-cli

The command-line tool for the administration of Pegasus.

Thanks to the portability of Go, we have provided binaries of admin-cli for multiple platforms. This is a tremendous advantage
compared to the old "Pegasus-Shell" which is previously written in C++. If you are using the Pegasus-Shell,
we highly recommend you taking a try on the brand new, more user-friendly admin-cli.

## Manual Installation

```sh
make
```

The executable will reside in ./bin/admin-cli. Checkout the usage via `--help`.

```sh
./bin/admin-cli --help
```

## Commands

This is an overview of the commands that admin-cli provides.

```
Pegasus administration command line tool

Commands:
  clear             clear the screen
  cluster-info      displays the overall cluster information
  create            create a table
  disk-balance      auto-migrate replica to let the disks space balance within the given ReplicaServer
  disk-capacity     query disk capacity info
  disk-migrate      migrate replica between the two disks within a specified ReplicaServer
  disk-replica      query disk replica count info
  drop              drop a table
  duplication, dup  duplication related control commands
  exit              exit the shell
  help              use 'help [command]' for command help
  list-tables, ls   list all tables in the cluster
  meta-level        Get the current meta function level
  node-stat         query all nodes perf stat in the cluster
  nodes             displays the nodes overall status
  partition-stat    displays the metrics of partitions within a table
  recall            recall the dropped table
  remote-command    send remote command, for example, remote-command meta or replica
  server-info       displays the overall server information
  table-env         table environments related commands
  table-partitions  show how the partitions distributed in the cluster
  table-stat        displays tables performance metrics
  use               select a table
```

To take a quick view of your Pegasus cluster, usually, you can use the following commands:

- `list-tables / ls`: the tables that I have.
- `nodes`: the nodes within the cluster.
- `cluster-info`: miscellaneous information, including the potential rebalancer plan.
- `server-info`: tells the versions of each server.

To diagnose into the system metrics, which may reveal the unhealthy resources conssumption,
unexpectedly high request volume, and possible hotspot partition...etc, you can use the "_stat" commands:

- `table-stat`
- `partition-stat`
- `node-stat`

`create`, `drop`, `recall` are the table operations. `meta-level` is for rebalancing.
`table-env set` is an advanced operation that controls table behavior.
To know more about a specific command usage, please type:

```
<CMD> -h
```

## Developer Guide

This tool uses https://github.com/desertbit/grumble for interactive command line parsing.
Please read the details from the library to learn how to develop a new command.
