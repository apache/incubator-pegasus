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

# Pegasus cluster rebalance

PID=$$

if [ $# -le 1 ]; then
  echo "USAGE: $0 <cluster-name> <cluster-meta-list> <only-move-primary>(default false) <nfs_rate_megabytes_per_disk>(default 100)"
  echo 
  echo "for example:"
  echo "  $0 onebox 127.0.0.1:34601,127.0.0.1:34602 true 100"
  echo
  exit 1
fi

cluster=$1
meta_list=$2

if [ -z $3 ]; then
  only_move_primary=false
else
  only_move_primary=$3
fi

if [ -z $4 ]; then
  nfs_rate_megabytes_per_disk=100
else
  nfs_rate_megabytes_per_disk=$4
fi

pwd="$( cd "$( dirname "$0"  )" && pwd )"
shell_dir="$( cd $pwd/.. && pwd )"
cd $shell_dir

echo "UID=$UID"
echo "PID=$PID"
echo "Start time: `date`"
rebalance_start_time=$((`date +%s`))
echo

echo "Generating /tmp/$UID.$PID.pegasus.rebalance.cluster_info..."
echo cluster_info | ./run.sh shell --cluster $meta_list 2>&1 | sed 's/ *$//' >/tmp/$UID.$PID.pegasus.rebalance.cluster_info
cname=`grep zookeeper_root /tmp/$UID.$PID.pegasus.rebalance.cluster_info | grep -o '/[^/]*$' | grep -o '[^/]*$'`
if [ "$cname" != "$cluster" ]; then
  echo "ERROR: cluster name and meta list not matched"
  exit 1
fi
pmeta=`grep primary_meta_server /tmp/$UID.$PID.pegasus.rebalance.cluster_info | grep -o '[0-9.:]*$'`
if [ "$pmeta" == "" ]; then
  echo "ERROR: extract primary_meta_server by shell failed"
  exit 1
fi

if [ "$only_move_primary" == "true" ]; then
  echo "Set meta.lb.only_move_primary true"
  echo "This remote-command tells the meta-server to ignore copying primaries during rebalancing."
  echo "So the following steps only include move_primary and copy_secondary."
  echo "remote_command -l $pmeta meta.lb.only_move_primary true" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rebalance.only_move_primary
  set_ok=`grep OK /tmp/$UID.$PID.pegasus.rebalance.only_move_primary | wc -l`
  if [ $set_ok -ne 1 ]; then
    echo "ERROR: meta.lb.only_move_primary true"
    exit 1
  fi
fi
echo

echo "Set nfs_copy/send_rate_megabytes_per_disk $nfs_rate_megabytes_per_disk"
echo "remote_command -t replica-server nfs.max_copy_rate_megabytes_per_disk $nfs_rate_megabytes_per_disk" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rebalance_cluster.set_nfs_copy_rate_megabytes_per_disk
set_ok=`grep 'succeed: OK' /tmp/$UID.$PID.pegasus.rebalance_cluster.set_nfs_copy_rate_megabytes_per_disk | wc -l`
if [ $set_ok -le 0 ]; then
  echo "ERROR: set nfs_copy_rate_megabytes_per_disk failed"
  exit 1
fi
echo "remote_command -t replica-server nfs.max_send_rate_megabytes_per_disk $nfs_rate_megabytes_per_disk" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rebalance_cluster.set_nfs_send_rate_megabytes_per_disk
set_ok=`grep 'succeed: OK' /tmp/$UID.$PID.pegasus.rebalance_cluster.set_nfs_send_rate_megabytes_per_disk | wc -l`
if [ $set_ok -le 0 ]; then
  echo "ERROR: set nfs_send_rate_megabytes_per_disk failed"
  exit 1
fi

echo "Set meta level to lively..."
echo "set_meta_level lively" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rebalance.set_meta_level
set_ok=`grep 'control meta level ok' /tmp/$UID.$PID.pegasus.rebalance.set_meta_level | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: set meta level to lively failed"
  exit 1
fi

echo "Wait cluster to become balanced..."
echo "Wait for 3 minutes to do load balance..."
sleep 180
## Number of check times for balanced state, in case that op_count is 0 but
## the cluster is in fact unbalanced. Each check waits for 30 secs.
op_count_check_remain_times=3
while true; do
    op_count=$(echo "cluster_info" | ./run.sh shell --cluster $meta_list | grep balance_operation_count | grep -o 'total=[0-9][0-9]*' | cut -d= -f2)
    if [ -z $op_count ]; then
        break
    fi

    if [ $op_count -eq 0 ]; then
        if [ $op_count_check_remain_times -eq 0 ]; then
          break
        else
           echo "Cluster may be balanced, try wait 30 seconds..."
           ((op_count_check_remain_times--))
           sleep 30
        fi
    else
        echo "Still $op_count balance operations to do..."
        sleep 10
    fi
done
echo

echo "Set meta level to steady..."
echo "set_meta_level steady" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rebalance.set_meta_level
set_ok=`grep 'control meta level ok' /tmp/$UID.$PID.pegasus.rebalance.set_meta_level | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: set meta level to steady failed"
  exit 1
fi

if [ "$only_move_primary" == "true" ]; then
  echo "Set meta.lb.only_move_primary false"
  echo "This remote-command tells the meta-server to rebalance with copying primaries."
  echo "remote_command -l $pmeta meta.lb.only_move_primary false" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rebalance.only_move_primary
  set_ok=`grep OK /tmp/$UID.$PID.pegasus.rebalance.only_move_primary | wc -l`
  if [ $set_ok -ne 1 ]; then
    echo "ERROR: meta.lb.only_move_primary false"
    exit 1
  fi
  echo
fi

echo "Set nfs_copy/send_rate_megabytes_per_disk 500"
echo "remote_command -t replica-server nfs.max_copy_rate_megabytes_per_disk 500" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rebalance_cluster.set_nfs_copy_rate_megabytes_per_disk
set_ok=`grep 'succeed: OK' /tmp/$UID.$PID.pegasus.rebalance_cluster.set_nfs_copy_rate_megabytes_per_disk | wc -l`
if [ $set_ok -le 0 ]; then
  echo "ERROR: set nfs_copy_rate_megabytes_per_disk failed"
  exit 1
fi
echo "remote_command -t replica-server nfs.max_send_rate_megabytes_per_disk 500" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rebalance_cluster.set_nfs_send_rate_megabytes_per_disk
set_ok=`grep 'succeed: OK' /tmp/$UID.$PID.pegasus.rebalance_cluster.set_nfs_send_rate_megabytes_per_disk | wc -l`
if [ $set_ok -le 0 ]; then
  echo "ERROR: set nfs_send_rate_megabytes_per_disk failed"
  exit 1
fi

echo "Finish time: `date`"
rebalance_finish_time=$((`date +%s`))
echo "rebalance done, elasped time is $((rebalance_finish_time - rebalance_start_time)) seconds."

rm -f /tmp/$UID.$PID.pegasus.* &>/dev/null
