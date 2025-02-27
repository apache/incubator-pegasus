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

set -e

# Offline replica server using minos.

PID=$$

if [ $# -le 2 ]; then
  echo "USAGE: $0 <cluster-name> <cluster-meta-list> <replica-task-id>"
  echo
  echo "For example:"
  echo "  $0 onebox 127.0.0.1:34601,127.0.0.1:34602 0"
  echo
  exit 1
fi

cluster=$1
meta_list=$2
replica_task_id=$3

pwd="$( cd "$( dirname "$0"  )" && pwd )"
shell_dir="$( cd $pwd/.. && pwd )"
cd $shell_dir

source ./admin_tools/minos_common.sh
find_cluster $cluster
if [ $? -ne 0 ]; then
  echo "ERROR: cluster \"$cluster\" not found"
  exit 1
fi

echo "UID=$UID"
echo "PID=$PID"
echo "Start time: `date`"
all_start_time=$((`date +%s`))
echo

rs_list_file="/tmp/$UID.$PID.pegasus.rolling_update.rs.list"
echo "Generating $rs_list_file..."
minos_show_replica $cluster $rs_list_file
replica_server_count=`cat $rs_list_file | wc -l`
if [ $replica_server_count -eq 0 ]; then
  echo "ERROR: replica server count is 0 by minos show"
  exit 1
fi

echo "Generating /tmp/$UID.$PID.pegasus.offline_node.cluster_info..."
echo cluster_info | ./run.sh shell --cluster $meta_list 2>&1 | sed 's/ *$//' >/tmp/$UID.$PID.pegasus.offline_node.cluster_info
cname=`grep zookeeper_root /tmp/$UID.$PID.pegasus.offline_node.cluster_info | grep -o '/[^/]*$' | grep -o '[^/]*$'`
if [ "$cname" != "$cluster" ]; then
  echo "ERROR: cluster name and meta list not matched"
  exit 1
fi
pmeta=`grep primary_meta_server /tmp/$UID.$PID.pegasus.offline_node.cluster_info | grep -o '[0-9.:]*$'`
if [ "$pmeta" == "" ]; then
  echo "ERROR: extract primary_meta_server by shell failed"
  exit 1
fi

echo "Generating /tmp/$UID.$PID.pegasus.offline_node.nodes..."
echo nodes | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.offline_node.nodes
rs_port=`grep '^[0-9.]*:' /tmp/$UID.$PID.pegasus.offline_node.nodes | head -n 1 | grep -o ':[0-9]*' | grep -o '[0-9]*'`
if [ "$rs_port" == "" ]; then
  echo "ERROR: extract replica server port by shell failed"
  exit 1
fi

echo "Set meta level to steady..."
echo "set_meta_level steady" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.offline_node.set_meta_level
set_ok=`grep 'control meta level ok' /tmp/$UID.$PID.pegasus.offline_node.set_meta_level | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: set meta level to steady failed"
  exit 1
fi

echo "Set lb.assign_delay_ms to 10..."
echo "remote_command -l $pmeta meta.lb.assign_delay_ms 10" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.offline_node.assign_delay_ms
set_ok=`grep OK /tmp/$UID.$PID.pegasus.offline_node.assign_delay_ms | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: set lb.assign_delay_ms to 10 failed"
  exit 1
fi

echo
while read line
do
  task_id=`echo $line | awk '{print $1}'`
  if [ $task_id -ne $replica_task_id ]; then
    continue
  fi
  node_str=`echo $line | awk '{print $2}'`
  node_ip=`getent hosts $node_str | awk '{print $1}'`
  node_name=`getent hosts $node_str | awk '{print $2}'`
  node=${node_ip}:${rs_port}
  echo "=================================================================="
  echo "=================================================================="
  echo "Offline replica server task $task_id of [$node_name] [$node]..."
  echo

  echo "Getting serving replica count..."
  serving_replica_count=`echo 'nodes -d' | ./run.sh shell --cluster $meta_list | grep $node | awk '{print $3}'`
  echo "servicing_replica_count=$serving_replica_count"
  echo

  echo "Migrating primary replicas out of node..."
  ./run.sh migrate_node -c $meta_list -n $node -t run &>/tmp/$UID.$PID.pegasus.offline_node.migrate_node
  echo "Wait [$node] to migrate done..."
  echo "Refer to /tmp/$UID.$PID.pegasus.offline_node.migrate_node for details"
  while true
  do
    pri_count=`echo 'nodes -d' | ./run.sh shell --cluster $meta_list | grep $node | awk '{print $4}'`
    if [ $pri_count -eq 0 ]; then
      echo "Migrate done."
      break
    else
      echo "Still $pri_count primary replicas left on $node"
      sleep 1
    fi
  done
  echo
  sleep 1

  echo "Downgrading replicas on node..."
  ./run.sh downgrade_node -c $meta_list -n $node -t run &>/tmp/$UID.$PID.pegasus.offline_node.downgrade_node
  echo "Wait [$node] to downgrade done..."
  echo "Refer to /tmp/$UID.$PID.pegasus.offline_node.downgrade_node for details"
  while true
  do
    rep_count=`echo 'nodes -d' | ./run.sh shell --cluster $meta_list | grep $node | awk '{print $3}'`
    if [ $rep_count -eq 0 ]; then
      echo "Downgrade done."
      break
    else
      echo "Still $rep_count replicas left on $node"
      sleep 1
    fi
  done
  echo
  sleep 1

  echo "Send kill_partition to node..."
  grep '^propose ' /tmp/$UID.$PID.pegasus.offline_node.downgrade_node >/tmp/$UID.$PID.pegasus.offline_node.downgrade_node.propose
  while read line2 
  do
    gpid=`echo $line2 | awk '{print $3}' | sed 's/\./ /'`
    echo "remote_command -l $node replica.kill_partition $gpid" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.offline_node.kill_partition
  done </tmp/$UID.$PID.pegasus.offline_node.downgrade_node.propose
  echo "Sent kill_partition to `cat /tmp/$UID.$PID.pegasus.offline_node.downgrade_node.propose | wc -l` partitions"
  echo
  sleep 1

  echo "Stop node by minos..."
  minos_stop $cluster replica $task_id
  echo "Stop node by minos done."
  echo
  sleep 1

  echo "Wait cluster to become healthy..."
  while true
  do
    unhealthy_count=`echo "ls -d" | ./run.sh shell --cluster $meta_list | awk 'BEGIN{s=0} f{ if(NF<7){f=0} else if($3!=$4){s=s+$5+$6} } / fully_healthy /{f=1} END{print s}'`
    if [ $unhealthy_count -eq 0 ]; then
      echo "Cluster becomes healthy"
      break
    else
      echo "Cluster not healthy, unhealthy_partition_count = $unhealthy_count"
      sleep 10
    fi
  done
  echo
  sleep 1
done <$rs_list_file

echo "Set lb.assign_delay_ms to DEFAULT..."
echo "remote_command -l $pmeta meta.lb.assign_delay_ms DEFAULT" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.offline_node.assign_delay_ms
set_ok=`grep OK /tmp/$UID.$PID.pegasus.offline_node.assign_delay_ms | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: set lb.assign_delay_ms to DEFAULT failed"
  exit 1
fi
echo

all_finish_time=$((`date +%s`))
echo "Offline replica server task $replica_task_id done."
echo "Elapsed time is $((all_finish_time - all_start_time)) seconds."

rm -f /tmp/$UID.$PID.pegasus.* &>/dev/null
