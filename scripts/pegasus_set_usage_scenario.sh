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

PID=$$

if [ $# -ne 3 ]
then
  echo "This tool is for set usage scenario of specified table(app)."
  echo "USAGE: $0 <cluster-meta-list> <app-name> <normal|prefer_write|bulk_load>"
  exit 1
fi

pwd="$( cd "$( dirname "$0"  )" && pwd )"
shell_dir="$( cd $pwd/.. && pwd )"
cd $shell_dir

cluster=$1
app_name=$2
scenario=$3
scenario_key="rocksdb.usage_scenario"

if [ "$scenario" != "normal" -a "$scenario" != "prefer_write" -a "$scenario" != "bulk_load" ]; then
    echo "invalid usage scenario type: $scenario"
    exit 1
fi

echo "UID: $UID"
echo "PID: $PID"
echo "cluster: $cluster"
echo "app_name: $app_name"
echo "scenario: $scenario"
echo "Start time: `date`"
all_start_time=$((`date +%s`))
echo

echo -e "use $app_name\nset_app_envs $scenario_key $scenario" | ./run.sh shell --cluster $cluster &>/tmp/$UID.$PID.pegasus.set_app_envs
set_fail=`grep 'set app env failed' /tmp/$UID.$PID.pegasus.set_app_envs | wc -l`
if [ $set_fail -eq 1 ]; then
  grep ERR /tmp/$UID.$PID.pegasus.set_app_envs
  echo "ERROR: set app envs failed, refer to /tmp/$UID.$PID.pegasus.set_app_envs"
  exit 1
fi

echo ls | ./run.sh shell --cluster $cluster &>/tmp/$UID.$PID.pegasus.ls

while read app_line
do
  status=`echo $app_line | awk '{print $2}'`
  if [ "$status" = "AVAILABLE" ]
  then
    gid=`echo $app_line | awk '{print $1}'`
    app=`echo $app_line | awk '{print $3}'`
    partition_count=`echo $app_line | awk '{print $5}'`
    replica_count=`echo $app_line | awk '{print $6}'`
    if [ "$app_name" != "$app" ]
    then
      continue
    fi

    echo "Checking app envs take effect to all replicas..."
    sleeped=0
    while true
    do
      echo "remote_command -t replica-server replica.query-app-envs $gid" | ./run.sh shell --cluster $cluster &>/tmp/$UID.$PID.pegasus.query_app_envs.$app
      effect_count=`grep "$scenario_key=$scenario" /tmp/$UID.$PID.pegasus.query_app_envs.$app | wc -l`
      total_count=$((partition_count * replica_count))
      if [ $effect_count -ge $total_count ]; then
        echo "[${sleeped}s] $effect_count/$total_count finished."
        echo "All finished."
        break
      else
        echo "[${sleeped}s] $effect_count/$total_count finished."
        sleep 5
        sleeped=$((sleeped + 5))
      fi
    done
    echo
  fi
done </tmp/$UID.$PID.pegasus.ls

echo "Finish time: `date`"
all_finish_time=$((`date +%s`))
echo "Set usage scenario done, elasped time is $((all_finish_time - all_start_time)) seconds."

rm -f /tmp/$UID.$PID.pegasus.* &>/dev/null
