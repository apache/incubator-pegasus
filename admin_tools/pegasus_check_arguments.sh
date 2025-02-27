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

# Check offline_node_list.sh and add_node_list.sh arguments.

PID=$$

if [ $# -le 3 ]; then
  echo "USAGE: $0 <check_type> <cluster-name> <cluster-meta-list> <replica-task-id-list>"
  echo
  echo "check_type includes: add_node_list, offline_node_list, for example:"
  echo "  $0 add_node_list onebox 127.0.0.1:34601,127.0.0.1:34602 1,2,3"
  echo
  exit 1
fi

check_type=$1
cluster=$2
meta_list=$3
replica_task_id_list=$4

if [ "$check_type" != "add_node_list" -a "$check_type" != "offline_node_list" ]; then
  echo "ERROR: $check_type is invalid, only support \"add_node_list\" and \"offline_node_list\""
  exit 1
fi

source ./admin_tools/minos_common.sh
find_cluster $cluster
if [ $? -ne 0 ]; then
  echo "ERROR: cluster \"$cluster\" not found"
  exit 1
fi

id_list_file="/tmp/$UID.$PID.pegasus.$check_type.id_list"
echo "Generating $id_list_file..."
minos_show_replica $cluster $id_list_file
replica_server_count=`cat $id_list_file | wc -l`
if [ $replica_server_count -eq 0 ]; then
  echo "ERROR: replica server count is 0 by minos show"
  exit 1
fi


echo "Generating /tmp/$UID.$PID.pegasus.$check_type.cluster_info..."
echo cluster_info | ./run.sh shell --cluster $meta_list 2>&1 | sed 's/ *$//' >/tmp/$UID.$PID.pegasus.$check_type.cluster_info
cname=`grep zookeeper_root /tmp/$UID.$PID.pegasus.$check_type.cluster_info | grep -o '/[^/]*$' | grep -o '[^/]*$'`
if [ "$cname" != "$cluster" ]; then
  echo "ERROR: cluster name and meta list not matched"
  exit 1
fi
pmeta=`grep primary_meta_server /tmp/$UID.$PID.pegasus.$check_type.cluster_info | grep -o '[0-9.:]*$'`
if [ "$pmeta" == "" ]; then
  echo "ERROR: extract primary_meta_server by shell failed"
  exit 1
fi

echo "Generating /tmp/$UID.$PID.pegasus.$check_type.nodes_list..."
echo nodes | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.$check_type.nodes_list
rs_port=`grep '^[0-9.]*:' /tmp/$UID.$PID.pegasus.$check_type.nodes_list | head -n 1 | grep -o ':[0-9]*' | grep -o '[0-9]*'`
if [ "$rs_port" == "" ]; then
  echo "ERROR: extract replica server port by shell failed"
  exit 1
fi

echo "Checking replica task id list..."
address_list=""
id_list=""
for id in `echo $replica_task_id_list | sed 's/,/ /g'` ; do
  if [ "$id_list" != "" ]; then
    if echo "$id_list" | grep -q "\<$id\>" ; then
      echo "ERROR: duplicate replica task id $id"
      exit 1;
    fi
  fi
  pair=`grep "^$id " $id_list_file`
  if [ "$pair" == "" ]; then
    echo "ERROR: replica task id $id not found, refer to $id_list_file"
    exit 1;
  fi
  node_str=`echo $pair | awk '{print $2}'`
  node_ip=`getent hosts $node_str | awk '{print $1}'`
  node=${node_ip}:${rs_port}
  if [ "$id_list" != "" ]; then
    id_list="$id_list $id"
    address_list="$address_list,$node"
  else
    id_list="$id"
    address_list="$node"
  fi
done

export task_id_list=$id_list
