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

PID=$$

if [ $# -ne 2 ]
then
  echo "This tool is for create or update falcon screen for specified cluster."
  echo "USAGE: $0 <create|update> <cluster-name>"
  exit 1
fi

pwd="$( cd "$( dirname "$0"  )" && pwd )"
shell_dir="$( cd $pwd/.. && pwd )"
cd $shell_dir

operate=$1
cluster=$2

if [ "$operate" != "create" -a "$operate" != "update" ]; then
    echo "ERROR: invalid operation type: $operate"
    exit 1
fi

echo "UID: $UID"
echo "PID: $PID"
echo "cluster: $cluster"
echo "operate: $operate"
echo "Start time: `date`"
all_start_time=$((`date +%s`))
echo

cd $shell_dir
echo ls | ./run.sh shell -n $cluster &>/tmp/$UID.$PID.pegasus.ls
grep AVAILABLE /tmp/$UID.$PID.pegasus.ls | awk '{print $3}' >/tmp/$UID.$PID.pegasus.table.list
table_count=`cat /tmp/$UID.$PID.pegasus.table.list | wc -l`
if [ $table_count -eq 0 ]; then
    echo "ERROR: table list is empty, please check the cluster $cluster"
    exit 1
fi
cd $pwd

python falcon_screen.py $cluster falcon_screen.json /tmp/$UID.$PID.pegasus.table.list $operate
if [ $? -ne 0 ]; then
    echo "ERROR: falcon screen $operate failed"
    exit 1
fi

echo
echo "Finish time: `date`"
all_finish_time=$((`date +%s`))
echo "Falcon screen $operate done, elasped time is $((all_finish_time - all_start_time)) seconds."

rm -f /tmp/$UID.$PID.pegasus.* &>/dev/null
