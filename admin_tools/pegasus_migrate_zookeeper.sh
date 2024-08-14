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

# Migrate zookeeper using minos.

PID=$$

if [ $# -le 2 ]; then
  echo "USAGE: $0 <cluster-name> <cluster-meta-list> <target-zookeeper-hosts>"
  echo
  echo "For example:"
  echo "  $0 onebox 127.0.0.1:34601,127.0.0.1:34602 127.0.0.1:2181"
  echo
  exit 1
fi

cluster=$1
meta_list=$2
target_zk=$3

pwd="$( cd "$( dirname "$0"  )" && pwd )"
shell_dir="$( cd $pwd/.. && pwd )"
cd $shell_dir

source ./admin_tools/minos_common.sh
find_cluster $cluster
if [ $? -ne 0 ]; then
  echo "ERROR: cluster \"$cluster\" not found"
  exit 1
fi

if [ `cat $minos_config | grep " recover_from_replica_server = " | wc -l` -ne 1 ] ; then
  echo "ERROR: no [recover_from_replica_server] entry in minos config \"$minos_config\""
  exit 1
fi
if [ `cat $minos_config | grep " hosts_list = " | wc -l` -ne 1 ] ; then
  echo "ERROR: no [hosts_list] entry in minos config \"$minos_config\""
  exit 1
fi

low_version_count=`echo server_info | ./run.sh shell --cluster $meta_list 2>&1 | grep 'Pegasus Server' | grep -v 'Pegasus Server 1.5' | wc -l`
if [ $low_version_count -gt 0 ]; then
  echo "ERROR: cluster should upgrade to v1.5.0"
  exit 1
fi

echo "UID=$UID"
echo "PID=$PID"
echo

echo ">>>> Backuping app list..."
echo "ls -o ${cluster}.apps" | ./run.sh shell --cluster $meta_list &>/dev/null
if [ `cat ${cluster}.apps | wc -l` -eq 0 ]; then
  echo "ERROR: backup app list failed"
  exit 1
fi
cat ${cluster}.apps

echo ">>>> Backuping node list..."
echo "nodes -d -o ${cluster}.nodes" | ./run.sh shell --cluster $meta_list &>/dev/null
if [ `cat ${cluster}.nodes | wc -l` -eq 0 ]; then
  echo "ERROR: backup node list failed"
  exit 1
fi
if [ `grep UNALIVE ${cluster}.nodes | wc -l` -gt 0 ]; then
  echo "ERROR: unalive nodes found in \"${cluster}.nodes\""
  exit 1
fi
cat ${cluster}.nodes | grep " ALIVE" | awk '{print $1}' >${cluster}.recover.nodes
if [ `cat ${cluster}.recover.nodes | wc -l` -eq 0 ]; then
  echo "ERROR: no node found in \"${cluster}.recover.nodes\""
  exit 1
fi

echo ">>>> Generating recover config..."
cp -f $minos_config ${minos_config}.bak
sed -i "s/ recover_from_replica_server = .*/ recover_from_replica_server = true/" $minos_config
sed -i "s/ hosts_list = .*/ hosts_list = ${target_zk}/" $minos_config

echo ">>>> Stopping all meta-servers..."
minos_stop $cluster meta

echo ">>>> Sleep for 15 seconds..."
sleep 15

function undo()
{
  echo ">>>> Undo..."
  mv -f ${minos_config}.bak $minos_config
  minos_rolling_update $cluster meta
}

echo ">>>> Rolling update meta-server task 0..."
minos_rolling_update $cluster meta 0

echo ">>>> Sending recover command..."
echo "recover -f ${cluster}.recover.nodes" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.migrate_zookeeper.shell.recover
cat /tmp/$UID.$PID.pegasus.migrate_zookeeper.shell.recover
if [ `cat /tmp/$UID.$PID.pegasus.migrate_zookeeper.shell.recover | grep "Recover result: ERR_OK" | wc -l` -ne 1 ]; then
  echo "ERROR: recover failed, refer to /tmp/$UID.$PID.pegasus.migrate_zookeeper.shell.recover"
  undo
  exit 1
fi

echo ">>>> Checking recover result, refer to /tmp/$UID.$PID.pegasus.migrate_zookeeper.diff..."
awk '{print $1,$2,$3}' ${cluster}.nodes >/tmp/$UID.$PID.pegasus.migrate_zookeeper.diff.old
while true
do
  rm -f /tmp/$UID.$PID.pegasus.migrate_zookeeper.shell.nodes
  echo "nodes -d -o /tmp/$UID.$PID.pegasus.migrate_zookeeper.shell.nodes" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.migrate_zookeeper.shell.nodes.log
  if [ `cat /tmp/$UID.$PID.pegasus.migrate_zookeeper.shell.nodes | wc -l` -eq 0 ]; then
    echo "ERROR: get node list failed, refer to /tmp/$UID.$PID.pegasus.migrate_zookeeper.shell.nodes.log"
    exit 1
  fi
  awk '{print $1,$2,$3}' /tmp/$UID.$PID.pegasus.migrate_zookeeper.shell.nodes >/tmp/$UID.$PID.pegasus.migrate_zookeeper.diff.new
  diff /tmp/$UID.$PID.pegasus.migrate_zookeeper.diff.old /tmp/$UID.$PID.pegasus.migrate_zookeeper.diff.new &>/tmp/$UID.$PID.pegasus.migrate_zookeeper.diff
  if [ $? -eq 0 ]; then
    break
  fi
  sleep 1
done

echo ">>>> Modifying config..."
sed -i "s/ recover_from_replica_server = .*/ recover_from_replica_server = false/" $minos_config

echo ">>>> Rolling update meta-server task 1..."
minos_rolling_update $cluster meta 1

echo ">>>> Rolling update meta-server task 0..."
minos_rolling_update $cluster meta 0

echo ">>>> Querying cluster info..."
echo "cluster_info" | ./run.sh shell --cluster $meta_list

echo "Migrate zookeeper done."

rm -f /tmp/$UID.$PID.pegasus.* &>/dev/null
