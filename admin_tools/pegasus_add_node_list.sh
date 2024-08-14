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

# Add replica servers using minos.

PID=$$

if [ $# -le 2 ]; then
  echo "USAGE: $0 <cluster-name> <cluster-meta-list> <replica-task-id-list> <nfs_rate_megabytes_per_disk>(default 100)"
  echo
  echo "For example:"
  echo "  $0 onebox 127.0.0.1:34601,127.0.0.1:34602 1,2,3 100"
  echo
  exit 1
fi

echo "UID=$UID"
echo "PID=$PID"
echo "Start time: `date`"
add_node_start_time=$((`date +%s`))
echo

cluster=$1
meta_list=$2
replica_task_id_list=$3

if [ -z $4 ]; then
  nfs_rate_megabytes_per_disk=100
else
  nfs_rate_megabytes_per_disk=$4
fi

pwd="$( cd "$( dirname "$0"  )" && pwd )"
shell_dir="$( cd $pwd/.. && pwd )"
cd $shell_dir

echo "Check the argument..."
source ./admin_tools/pegasus_check_arguments.sh add_node_list $cluster $meta_list $replica_task_id_list

if [ $? -ne 0 ]; then
    echo "ERROR: the argument check failed"
    exit 1
fi

echo "Set meta level to steady..."
echo "set_meta_level steady" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.add_node_list.set_meta_level
set_ok=`grep 'control meta level ok' /tmp/$UID.$PID.pegasus.add_node_list.set_meta_level | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: set meta level to steady failed"
  exit 1
fi

for id in $task_id_list
do
  echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
  minos_bootstrap $cluster replica $id
  if [ $? -ne 0 ]; then
    echo "ERROR: online replica task $id failed"
    exit 1
  fi
  echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
done

./admin_tools/pegasus_rebalance_cluster.sh $cluster $meta_list true $nfs_rate_megabytes_per_disk

echo "Finish time: `date`"
add_node_finish_time=$((`date +%s`))
echo "add node list done, elasped time is $((add_node_finish_time - add_node_start_time)) seconds."

rm -f /tmp/$UID.$PID.pegasus.* &>/dev/null
