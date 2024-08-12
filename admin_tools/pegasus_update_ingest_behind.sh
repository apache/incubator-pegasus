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

# Update app ingestion_behind envs using minos.

PID=$$

if [ $# -le 3 ]; then
  echo "USAGE: $0 <cluster-name> <cluster-meta-list> <app_name> <ingestion_behind>"
  echo "For example:"
  echo "  $0 onebox 127.0.0.1:34601,127.0.0.1:34602 temp false"
  echo
  exit 1
fi

function find_app()
{
  meta_list=$1
  app_name=$2
  find_app="false"

  echo ls | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.update_ingestion_behind.ls
  while read app_line
  do
    status=`echo ${app_line} | awk '{print $2}'`
    name=`echo ${app_line} | awk '{print $3}'`
    if [ "${app_name}" != "$name" ]; then
        continue
    fi
    if [ "$status" == "AVAILABLE" ]; then
        find_app="true"
        break
    fi
  done </tmp/$UID.$PID.pegasus.update_ingestion_behind.ls
  echo "$find_app"
}


function get_app_node_count()
{
  meta_list=$1
  app_name=$2
  node=$3
  primary=$4
  
  pri_count=0
  total_count=0

  echo "app $app_name -d" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.update_ingestion_behind.$app
  while read line
  do
    node_addr=`echo $line | awk '{print $1}'`
    if [ "$node_addr" = "$node" ]; then
      pri_count=`echo $line | awk '{print $2}'`
      total_count=`echo $line | awk '{print $4}'`
      break
    fi
  done </tmp/$UID.$PID.pegasus.update_ingestion_behind.$app

  if [ "${primary}" == "true" ]; then
    echo "$pri_count"
  else
    echo "$total_count"
  fi
}

cluster=$1
meta_list=$2
app_name=$3
ingestion_behind=$4
if [ "$ingestion_behind" != "true" -a "$ingestion_behind" != "false" ]; then
  echo "ERROR: invalid ingestion_behind, should be true or false"
  exit 1
fi

start_task_id=0
rebalance_cluster_after_rolling=true
rebalance_only_move_primary=false

echo "UID=$UID"
echo "PID=$PID"
echo "Start time: `date`"
total_start_time=$((`date +%s`))
echo

echo "Generating /tmp/$UID.$PID.pegasus.update_ingestion_behind.cluster_info..."
echo cluster_info | ./run.sh shell --cluster $meta_list 2>&1 | sed 's/ *$//' >/tmp/$UID.$PID.pegasus.update_ingestion_behind.cluster_info
cname=`grep zookeeper_root /tmp/$UID.$PID.pegasus.update_ingestion_behind.cluster_info | grep -o '/[^/]*$' | grep -o '[^/]*$'`
if [ "$cname" != "$cluster" ]; then
  echo "ERROR: cluster name and meta list not matched"
  exit 1
fi

app_found=`find_app ${meta_list} ${app_name}`
if [ "$app_found" != "true" ]; then
  echo "ERROR: can't find app $app_name in cluster $cluster"
  exit 1
fi

pmeta=`grep primary_meta_server /tmp/$UID.$PID.pegasus.update_ingestion_behind.cluster_info | grep -o '[0-9.:]*$'`
if [ "$pmeta" == "" ]; then
  echo "ERROR: extract primary_meta_server by shell failed"
  exit 1
fi

rs_list_file="/tmp/$UID.$PID.pegasus.update_ingestion_behind.rs.list"
echo "Generating $rs_list_file..."
echo nodes | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.update_ingestion_behind.nodes
while read line
do
  node_status=`echo $line | awk '{print $2}'`
  if [ "$node_status" != "ALIVE" ]; then
    continue
  else
    echo $line | awk '{print $1}' >>$rs_list_file
  fi
done </tmp/$UID.$PID.pegasus.update_ingestion_behind.nodes

echo "Set meta level to steady..."
echo "set_meta_level steady" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.update_ingestion_behind.set_meta_level
set_ok=`grep 'control meta level ok' /tmp/$UID.$PID.pegasus.update_ingestion_behind.set_meta_level | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: set meta level to steady failed"
  exit 1
fi

echo "Set app envs rocksdb.allow_ingest_behind=${ingestion_behind}"
log_file="/tmp/$UID.$PID.pegasus.set_app_envs.${app_name}"
echo -e "use ${app_name}\n set_app_envs rocksdb.allow_ingest_behind ${ingestion_behind}" | ./run.sh shell --cluster $meta_list &>${log_file}
set_fail=`grep 'set app env failed' ${log_file} | wc -l`
if [ ${set_fail} -eq 1 ]; then
    echo "ERROR: set app envs failed, refer to ${log_file}"
    exit 1
fi
echo "Sleep 30 seconds to wait app envs working..."
sleep 30

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
  start_time=$((`date +%s`))
  node=$line
  node_ip=`echo ${line} | cut -d ':' -f1`
  node_name=`getent hosts $node_ip | awk '{print $2}'`

  echo "=================================================================="
  echo "=================================================================="
  echo "Closing partitions of [$app_name] on [$node_name] [$node]..."
  echo

  echo "Getting serving replica count..."
  serving_replica_count=`echo 'nodes -d' | ./run.sh shell --cluster $meta_list | grep $node | tail -n 1 | awk '{print $3}'`
  app_serving_replica_count=`get_app_node_count ${meta_list} ${app_name} ${node} 'false'`
  echo "serving_replica_count=$serving_replica_count"
  echo "app_serving_replica_count=$app_serving_replica_count"
  echo

  echo "Set lb.add_secondary_max_count_for_one_node to 0..."
  echo "remote_command -l $pmeta meta.lb.add_secondary_max_count_for_one_node 0" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.update_ingestion_behind.add_secondary_max_count_for_one_node
  set_ok=`grep OK /tmp/$UID.$PID.pegasus.update_ingestion_behind.add_secondary_max_count_for_one_node | wc -l`
  if [ $set_ok -ne 1 ]; then
    echo "ERROR: set lb.add_secondary_max_count_for_one_node to 0 failed"
    exit 1
  fi
  echo

  echo "Migrating primary replicas out of node..."
  sleeped=0
  while true
  do
    if [ $((sleeped%10)) -eq 0 ]; then
      ./run.sh migrate_node -c $meta_list -n $node -a $app_name -t run &>/tmp/$UID.$PID.pegasus.update_ingestion_behind.migrate_node
      echo "Send migrate propose, refer to /tmp/$UID.$PID.pegasus.update_ingestion_behind.migrate_node for details"
    fi
    pri_count=`get_app_node_count ${meta_list} ${app_name} ${node} 'true'`
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
  while true
  do
    if [ $((sleeped%50)) -eq 0 ]; then
      ./run.sh downgrade_node -c $meta_list -n $node -a $app_name -t run &>/tmp/$UID.$PID.pegasus.update_ingestion_behind.downgrade_node
      echo "Send downgrade propose, refer to /tmp/$UID.$PID.pegasus.update_ingestion_behind.downgrade_node for details"
    fi
    rep_count=`get_app_node_count ${meta_list} ${app_name} ${node} 'false'`
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
  while true
  do
    if [ $((sleeped%50)) -eq 0 ]; then
      echo "Send kill_partition commands to node..."
      grep '^propose ' /tmp/$UID.$PID.pegasus.update_ingestion_behind.downgrade_node >/tmp/$UID.$PID.pegasus.update_ingestion_behind.downgrade_node.propose
      while read line2
      do
        gpid=`echo $line2 | awk '{print $3}' | sed 's/\./ /'`
        echo "remote_command -l $node replica.kill_partition $gpid" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.update_ingestion_behind.kill_partition
      done </tmp/$UID.$PID.pegasus.update_ingestion_behind.downgrade_node.propose
      echo "Sent to `cat /tmp/$UID.$PID.pegasus.update_ingestion_behind.downgrade_node.propose | wc -l` partitions."
    fi
    echo "remote_command -l $node perf-counters '.*replica(Count)'" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.update_ingestion_behind.replica_count_perf_counters
    serving_count=`grep -o 'replica_stub.replica(Count)","type":"NUMBER","value":[0-9]*' /tmp/$UID.$PID.pegasus.update_ingestion_behind.replica_count_perf_counters | grep -o '[0-9]*$'`
    opening_count=`grep -o 'replica_stub.opening.replica(Count)","type":"NUMBER","value":[0-9]*' /tmp/$UID.$PID.pegasus.update_ingestion_behind.replica_count_perf_counters | grep -o '[0-9]*$'`
    closing_count=`grep -o 'replica_stub.closing.replica(Count)","type":"NUMBER","value":[0-9]*' /tmp/$UID.$PID.pegasus.update_ingestion_behind.replica_count_perf_counters | grep -o '[0-9]*$'`
    if [ "$serving_count" = "" -o "$opening_count" = "" -o "$closing_count" = "" ]; then
      echo "ERROR: extract replica count from perf counters failed"
      exit 1
    fi
    rep_count=$((serving_count + opening_count + closing_count + app_serving_replica_count - serving_replica_count))
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

  echo "Set lb.add_secondary_max_count_for_one_node to 100..."
  echo "remote_command -l $pmeta meta.lb.add_secondary_max_count_for_one_node 100" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.update_ingestion_behind.add_secondary_max_count_for_one_node
  set_ok=`grep OK /tmp/$UID.$PID.pegasus.update_ingestion_behind.add_secondary_max_count_for_one_node | wc -l`
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
  echo "Updating ingestion_behind=$ingestion_behind for all partitions of app [$app_name] on [$node_name] [$node] done."
  echo "Elapsed time is $((finish_time - start_time)) seconds."
  echo
done <$rs_list_file

echo "=================================================================="
echo "=================================================================="

echo "Set lb.add_secondary_max_count_for_one_node to DEFAULT..."
echo "remote_command -l $pmeta meta.lb.add_secondary_max_count_for_one_node DEFAULT" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.update_ingestion_behind.add_secondary_max_count_for_one_node
set_ok=`grep OK /tmp/$UID.$PID.pegasus.update_ingestion_behind.add_secondary_max_count_for_one_node | wc -l`
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

if [ "$rebalance_cluster_after_rolling" == "true" ]; then
  echo "Start to rebalance cluster..."
  ./admin_tools/pegasus_rebalance_cluster.sh $cluster $meta_list $rebalance_only_move_primary
fi

echo "Finish time: `date`"
total_finish_time=$((`date +%s`))
echo "Update $app_name ingestion_behind=$ingestion_behind, elasped time is $((total_finish_time - total_start_time)) seconds."

rm -f /tmp/$UID.$PID.pegasus.* &>/dev/null

