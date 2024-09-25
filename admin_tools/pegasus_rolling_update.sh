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
# Rolling update pegasus cluster using minos.

PID=$$

if [ $# -le 3 ]; then
  echo "USAGE: $0 <cluster-name> <cluster-meta-list> <type> <start_task_id> [rebalance] [only_move_pri]"
  echo
  echo "The type may be 'one' or 'all':"
  echo "  - one: rolling update only one task of replica server."
  echo "  - all: rolling update all replica servers, meta servers and collectors."
  echo
  echo "rebalance: default value is false"
  echo "  - if rebalance cluster after rolling update"
  echo
  echo "only_move_pri: default value is true"
  echo "  - if only move primary while rebalance"
  echo "  - this option will only be usefule when rebalance = true"
  echo
  echo "For example:"
  echo "  $0 onebox 127.0.0.1:34601,127.0.0.1:34602 one 0"
  echo "  $0 onebox 127.0.0.1:34601,127.0.0.1:34602 all 1 true false"
  echo
  exit 1
fi

if [ -z ${TMUX} ]; then
  echo "ERROR: This script must be run in a tmux session"
  exit 1
fi

cluster=$1
meta_list=$2
type=$3
start_task_id=$4
if [ "$type" != "one" -a "$type" != "all" ]; then
  echo "ERROR: invalid type, should be one or all"
  exit 1
fi

if [ -z $5 ]; then
  rebalance_cluster_after_rolling=false
else
  rebalance_cluster_after_rolling=$5
fi

if [ -z $6 ]; then
  rebalance_only_move_primary=true
else
  rebalance_only_move_primary=$6
fi

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
rolling_start_time=$((`date +%s`))
echo

rs_list_file="/tmp/$UID.$PID.pegasus.rolling_update.rs.list"
echo "Generating $rs_list_file..."
minos_show_replica $cluster $rs_list_file
replica_server_count=`cat $rs_list_file | wc -l`
if [ $replica_server_count -eq 0 ]; then
  echo "ERROR: replica server count is 0 by minos show"
  exit 1
fi

echo "Generating /tmp/$UID.$PID.pegasus.rolling_update.cluster_info..."
echo cluster_info | ./run.sh shell --cluster $meta_list 2>&1 | sed 's/ *$//' >/tmp/$UID.$PID.pegasus.rolling_update.cluster_info
cname=`grep zookeeper_root /tmp/$UID.$PID.pegasus.rolling_update.cluster_info | grep -o '/[^/]*$' | grep -o '[^/]*$'`
if [ "$cname" != "$cluster" ]; then
  echo "ERROR: cluster name and meta list not matched"
  exit 1
fi
pmeta=`grep primary_meta_server /tmp/$UID.$PID.pegasus.rolling_update.cluster_info | grep -o '[0-9.:]*$'`
if [ "$pmeta" == "" ]; then
  echo "ERROR: extract primary_meta_server by shell failed"
  exit 1
fi

echo "Generating /tmp/$UID.$PID.pegasus.rolling_update.nodes..."
echo nodes | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rolling_update.nodes
rs_port=`grep '^[0-9.]*:' /tmp/$UID.$PID.pegasus.rolling_update.nodes | head -n 1 | grep -o ':[0-9]*' | grep -o '[0-9]*'`
if [ "$rs_port" == "" ]; then
  echo "ERROR: extract replica server port by shell failed"
  exit 1
fi

echo "Set meta level to steady..."
echo "set_meta_level steady" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rolling_update.set_meta_level
set_ok=`grep 'control meta level ok' /tmp/$UID.$PID.pegasus.rolling_update.set_meta_level | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: set meta level to steady failed"
  exit 1
fi

echo "Set lb.assign_delay_ms to 30min..."
echo "remote_command -l $pmeta meta.lb.assign_delay_ms 180000000" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rolling_node.assign_delay_ms
set_ok=`grep OK /tmp/$UID.$PID.pegasus.rolling_node.assign_delay_ms | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: set lb.assign_delay_ms to 30min failed"
  exit 1
fi

echo
while read line
do
  task_id=`echo $line | awk '{print $1}'`
  if [ $task_id -lt $start_task_id ]; then
    continue
  fi
  start_time=$((`date +%s`))
  node_str=`echo $line | awk '{print $2}'`
  node_ip=`getent hosts $node_str | awk '{print $1}'`
  node_name=`getent hosts $node_str | awk '{print $2}'`
  node=${node_ip}:${rs_port}
  echo "=================================================================="
  echo "=================================================================="
  echo "Rolling update replica server task $task_id of [$node_name] [$node]..."
  echo

  echo "Getting serving replica count..."
  serving_replica_count=`echo 'nodes -d' | ./run.sh shell --cluster $meta_list | grep $node | awk '{print $3}'`
  echo "servicing_replica_count=$serving_replica_count"
  echo

  echo "Set lb.add_secondary_max_count_for_one_node to 0..."
  echo "remote_command -l $pmeta meta.lb.add_secondary_max_count_for_one_node 0" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rolling_update.add_secondary_max_count_for_one_node
  set_ok=`grep OK /tmp/$UID.$PID.pegasus.rolling_update.add_secondary_max_count_for_one_node | wc -l`
  if [ $set_ok -ne 1 ]; then
    echo "ERROR: set lb.add_secondary_max_count_for_one_node to 0 failed"
    exit 1
  fi
  echo

  echo "Migrating primary replicas out of node..."
  sleeped=0
  # Migration timeout 30 seconds
  while true
  do
    if [ $((sleeped%10)) -eq 0 ]; then
      ./run.sh migrate_node -c $meta_list -n $node -t run &>/tmp/$UID.$PID.pegasus.rolling_update.migrate_node
      echo "Send migrate propose, refer to /tmp/$UID.$PID.pegasus.rolling_update.migrate_node for details"
    fi
    pri_count=`echo 'nodes -d' | ./run.sh shell --cluster $meta_list | grep $node | awk '{print $4}'`
    if [ $pri_count -eq 0 ]; then
      echo "Migrate done."
      break
    elif [ $sleeped -gt 28 ]; then
      echo "Migrate timeout."
      break
    else
      echo "Still $pri_count primary replicas left on $node"
      sleep 1
      sleeped=$((sleeped+1))
    fi
  done
  echo
  sleep 1

  echo "Downgrading replicas on node..."
  sleeped=0
  # Downgrade timeout 90 seconds
  while true
  do
    if [ $((sleeped%50)) -eq 0 ]; then
      ./run.sh downgrade_node -c $meta_list -n $node -t run &>/tmp/$UID.$PID.pegasus.rolling_update.downgrade_node
      echo "Send downgrade propose, refer to /tmp/$UID.$PID.pegasus.rolling_update.downgrade_node for details"
    fi
    rep_count=`echo 'nodes -d' | ./run.sh shell --cluster $meta_list | grep $node | awk '{print $3}'`
    if [ $rep_count -eq 0 ]; then
      echo "Downgrade done."
      break
    elif [ $sleeped -gt 88 ]; then
      echo "Downgrade timeout."
      break
    else
      echo "Still $rep_count replicas left on $node"
      sleep 1
      sleeped=$((sleeped+1))
    fi
  done
  echo
  sleep 1

  echo "Checking replicas closed on node..."
  sleeped=0
  # Close timeout 90 seconds
  while true
  do
    if [ $((sleeped%50)) -eq 0 ]; then
      echo "Send kill_partition commands to node..."
      grep '^propose ' /tmp/$UID.$PID.pegasus.rolling_update.downgrade_node >/tmp/$UID.$PID.pegasus.rolling_update.downgrade_node.propose
      while read line2
      do
        gpid=`echo $line2 | awk '{print $3}' | sed 's/\./ /'`
        echo "remote_command -l $node replica.kill_partition $gpid" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rolling_update.kill_partition
      done </tmp/$UID.$PID.pegasus.rolling_update.downgrade_node.propose
      echo "Sent to `cat /tmp/$UID.$PID.pegasus.rolling_update.downgrade_node.propose | wc -l` partitions."
    fi
    echo "remote_command -l $node perf-counters '.*replica(Count)'" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rolling_update.replica_count_perf_counters
    serving_count=`grep -o 'replica_stub.replica(Count)","type":"NUMBER","value":[0-9]*' /tmp/$UID.$PID.pegasus.rolling_update.replica_count_perf_counters | grep -o '[0-9]*$'`
    opening_count=`grep -o 'replica_stub.opening.replica(Count)","type":"NUMBER","value":[0-9]*' /tmp/$UID.$PID.pegasus.rolling_update.replica_count_perf_counters | grep -o '[0-9]*$'`
    closing_count=`grep -o 'replica_stub.closing.replica(Count)","type":"NUMBER","value":[0-9]*' /tmp/$UID.$PID.pegasus.rolling_update.replica_count_perf_counters | grep -o '[0-9]*$'`
    if [ "$serving_count" = "" -o "$opening_count" = "" -o "$closing_count" = "" ]; then
      echo "ERROR: extract replica count from perf counters failed"
      exit 1
    fi
    rep_count=$((serving_count + opening_count + closing_count))
    if [ $rep_count -eq 0 ]; then
      echo "Close done."
      break
    elif [ $sleeped -gt 88 ]; then
      echo "Close timeout."
      break
    else
      echo "Still $rep_count replicas not closed on $node"
      sleep 1
      sleeped=$((sleeped+1))
    fi
  done
  echo
  sleep 1

  echo "remote_command -l $node flush-log" | ./run.sh shell --cluster $meta_list &>/dev/null

  echo "Rolling update by minos..."
  minos_rolling_update $cluster replica $task_id
  echo "Rolling update by minos done."
  echo
  sleep 1

  echo "Wait [$node] to become alive..."
  while true
  do
    node_status=`echo 'nodes -d' | ./run.sh shell --cluster $meta_list | grep $node | awk '{print $2}'`
    if [ $node_status = "ALIVE" ]; then
      echo "Node becomes alive."
      break
    else
      sleep 1
    fi
  done
  echo
  sleep 1

  echo "Set lb.add_secondary_max_count_for_one_node to 100..."
  echo "remote_command -l $pmeta meta.lb.add_secondary_max_count_for_one_node 100" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rolling_update.add_secondary_max_count_for_one_node
  set_ok=`grep OK /tmp/$UID.$PID.pegasus.rolling_update.add_secondary_max_count_for_one_node | wc -l`
  if [ $set_ok -ne 1 ]; then
    echo "ERROR: set lb.add_secondary_max_count_for_one_node to 100 failed"
    exit 1
  fi

  echo "Wait cluster to become healthy..."
  while true
  do
    unhealthy_count=`echo "ls -d" | ./run.sh shell --cluster $meta_list | awk 'f{ if(NF<7){f=0} else if($3!=$4){print} } / fully_healthy /{f=1}' | wc -l`
    if [ $unhealthy_count -eq 0 ]; then
      echo "Cluster becomes healthy."
      break
    else
      sleep 1
    fi
  done
  echo
  sleep 1

  finish_time=$((`date +%s`))
  echo "Rolling update replica server task $task_id of [$node_name] [$node] done."
  echo "Elapsed time is $((finish_time - start_time)) seconds."
  echo

  if [ "$type" = "one" ]; then
    break
  fi
done <$rs_list_file

echo "Set lb.add_secondary_max_count_for_one_node to DEFAULT..."
echo "remote_command -l $pmeta meta.lb.add_secondary_max_count_for_one_node DEFAULT" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rolling_update.add_secondary_max_count_for_one_node
set_ok=`grep OK /tmp/$UID.$PID.pegasus.rolling_update.add_secondary_max_count_for_one_node | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: set lb.add_secondary_max_count_for_one_node to DEFAULT failed"
  exit 1
fi

echo "Set lb.assign_delay_ms to DEFAULT..."
echo "remote_command -l $pmeta meta.lb.assign_delay_ms DEFAULT" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rolling_node.assign_delay_ms
set_ok=`grep OK /tmp/$UID.$PID.pegasus.rolling_node.assign_delay_ms | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: set lb.assign_delay_ms to DEFAULT failed"
  exit 1
fi
echo

if [ "$type" = "all" ]; then
  echo "=================================================================="
  echo "=================================================================="
  echo "Rolling update meta servers..."
  minos_rolling_update $cluster meta
  echo "Rolling update meta servers done."
  echo

  echo "Rolling update collectors..."
  minos_rolling_update $cluster collector
  echo "Rolling update collectors done."
  echo
fi

if [ "$rebalance_cluster_after_rolling" == "true" ]; then
  echo "Start to rebalance cluster..."
  ./admin_tools/pegasus_rebalance_cluster.sh $cluster $meta_list $rebalance_only_move_primary
fi

echo "Finish time: `date`"
rolling_finish_time=$((`date +%s`))
echo "Rolling update $type done, elasped time is $((rolling_finish_time - rolling_start_time)) seconds."

rm -f /tmp/$UID.$PID.pegasus.* &>/dev/null
