#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Check offline_node_list.sh and add_node_list.sh arguments.
#


# different pegasus versions have different remote command, this script init it base version
PID=$$

if [ $# -le 3 ]; then
  echo "USAGE: $0 <cluster-name> <cluster-meta-list> <version>"
  echo
  echo "init pegasus remote command base version, for example:"
  echo "  $0 onebox 127.0.0.1:34601,127.0.0.1:34602"
  echo
  exit 1
fi

meta_list=$2
version=$3

if [ "$version" = "" ]; then
  echo "check pegasus server version"
  echo "server_info" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.command.version
  version=`grep 'Pegasus Server' /tmp/$UID.$PID.pegasus.command.version | head -n 1`
  if [ "$version" = "" ]; then
    echo "check pegasus server version failed"
    exit 1
  fi
  echo "current cluster version is $version"
fi

# list all remote command, default `server-info` that is supported by all version to avoid some version set no supported command and return error
meta.lb.assign_secondary_black_list="server-info"
meta.live_percentage="server-info"
meta.lb.assign_delay_ms="server-info"
meta.lb.only_move_primary="server-info"
meta.lb.add_secondary_max_count_for_one_node="server-info"

replica.kill_partition="server-info"
replica.query-app-envs="server-info"
replica.query-compact="server-info"
replica.trigger-checkpoint="server-info"

nfs.max_copy_rate_megabytes="server-info"
nfs.max_send_rate_megabytes="server-info"

if [[ ($version = ~"1.1.6") || ($version = ~"1.12.2") || ($version = ~"1.12.3") || ($version = ~"2.0.0") || ($version = ~"2.0-write-optm") ]];then
  meta.lb.assign_secondary_black_list="meta.lb.assign_secondary_black_list"
  meta.live_percentage="meta.live_percentage"
  meta.lb.assign_delay_ms="meta.lb.assign_delay_ms"
  meta.lb.only_move_primary="meta.lb.only_move_primary"
  meta.lb.add_secondary_max_count_for_one_node="meta.lb.add_secondary_max_count_for_one_node"

  replica.kill_partition="replica.kill_partition"
  replica.query-app-envs="replica.query-app-envs"
  replica.query-compact="replica.query-compact"
  replica.trigger-checkpoint="replica.trigger-checkpoint"
elif [[ ($version = ~"2.1.1") ]];then
  meta.lb.assign_secondary_black_list="lb.assign_secondary_black_list"
  meta.live_percentage="live_percentage"
  meta.lb.assign_delay_ms="lb.assign_delay_ms"
  meta.lb.only_move_primary="lb.only_move_primary"
  meta.lb.add_secondary_max_count_for_one_node="lb.add_secondary_max_count_for_one_node"

  replica.kill_partition="kill_partition"
  replica.query-app-envs="query-app-envs"
  replica.query-compact="query-compact"
  replica.trigger-checkpoint="trigger-checkpoint"

  nfs.max_copy_rate_megabytes="nfs.max_copy_rate_megabytes"
elif [[ ($version = ~"2.2.1") || ($version = ~"2.2.2") || ($version = ~"2.2.3") ]];then
  meta.lb.assign_secondary_black_list="meta.lb.assign_secondary_black_list"
  meta.live_percentage="meta.live_percentage"
  meta.lb.assign_delay_ms="meta.lb.assign_delay_ms"
  meta.lb.only_move_primary="meta.lb.only_move_primary"
  meta.lb.add_secondary_max_count_for_one_node="meta.lb.add_secondary_max_count_for_one_node"

  replica.kill_partition="replica.kill_partition"
  replica.query-app-envs="replica.query-app-envs"
  replica.query-compact="replica.query-compact"
  replica.trigger-checkpoint="replica.trigger-checkpoint"

  nfs.max_copy_rate_megabytes="nfs.max_copy_rate_megabytes"
elif [[ ($version = ~"2.3.0") || ($version = ~"2.3.1") ]];then
  meta.lb.assign_secondary_black_list="meta.lb.assign_secondary_black_list"
  meta.live_percentage="meta.live_percentage"
  meta.lb.assign_delay_ms="meta.lb.assign_delay_ms"
  meta.lb.only_move_primary="meta.lb.only_move_primary"
  meta.lb.add_secondary_max_count_for_one_node="meta.lb.add_secondary_max_count_for_one_node"

  replica.kill_partition="replica.kill_partition"
  replica.query-app-envs="replica.query-app-envs"
  replica.query-compact="replica.query-compact"
  replica.trigger-checkpoint="replica.trigger-checkpoint"

  nfs.max_copy_rate_megabytes="nfs.max_copy_rate_megabytes"
  nfs.max_send_rate_megabytes="nfs.max_send_rate_megabytes"
else
  meta.lb.assign_secondary_black_list="meta.lb.assign_secondary_black_list"
  meta.live_percentage="meta.live_percentage"
  meta.lb.assign_delay_ms="meta.lb.assign_delay_ms"
  meta.lb.only_move_primary="meta.lb.only_move_primary"
  meta.lb.add_secondary_max_count_for_one_node="meta.lb.add_secondary_max_count_for_one_node"

  replica.kill_partition="replica.kill_partition"
  replica.query-app-envs="replica.query-app-envs"
  replica.query-compact="replica.query-compact"
  replica.trigger-checkpoint="replica.trigger-checkpoint"

  nfs.max_copy_rate_megabytes="nfs.max_copy_rate_megabytes_per_disk"
  nfs.max_send_rate_megabytes="nfs.max_send_rate_megabytes_per_disk"
fi

