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

if [ $# -ne 5 ]
then
    echo "USAGE: $0 <meta-count> <replica-count> <app-name> <kill-type> <sleep-time>"
    echo "  kill-type: meta | replica | all"
    exit 1
fi
META_COUNT=$1
REPLICA_COUNT=$2
APP_NAME=$3
KILL_TYPE=$4
SLEEP_TIME=$5
if [ "$KILL_TYPE" != "meta" -a "$KILL_TYPE" != "replica" -a "$KILL_TYPE" != "all" ]
then
    echo "ERROR: invalid kill-type, should be: meta | replica | all"
    exit 1
fi

function stop_kill_test() {
  echo "---------------------------"
  ./run.sh shell <$SHELL_INPUT | grep '^[0-9]\|^primary_meta_server'
  echo "---------------------------"
  ps -ef | grep pegasus_kill_test | grep -v grep | awk '{print $2}' | xargs kill
  ./run.sh stop_onebox
  exit 1
}

pwd="$( cd "$( dirname "$0"  )" && pwd )"
shell_dir="$( cd $pwd/.. && pwd )"
cd $shell_dir

LEASE_TIMEOUT=20
UNHEALTHY_TIMEOUT=1200
KILL_META_PROB=10
SHELL_INPUT=.kill_test.shell.input
SHELL_OUTPUT=.kill_test.shell.output
echo "cluster_info" >$SHELL_INPUT
echo "nodes" >>$SHELL_INPUT
echo "app $APP_NAME --detailed" >>$SHELL_INPUT
WAIT_SECONDS=0
while true
do
  if ! ps -ef | grep "pegasus_kill_test" |  grep -v grep &>/dev/null
  then
    echo "[`date`] pegasus_kill_test process is not exist, stop kill test"
    stop_kill_test
  fi

  if [ "`find onebox -name 'core.*' | wc -l `" -gt 0 -o "`find onebox -name 'core' | wc -l `" -gt 0 ]
  then
    echo "[`date`] coredump generated, stop kill test"
    stop_kill_test
  fi

  ./run.sh shell <$SHELL_INPUT &>$SHELL_OUTPUT
  UNALIVE_COUNT=`cat $SHELL_OUTPUT | grep UNALIVE | wc -l`
  PARTITION_COUNT=`cat $SHELL_OUTPUT | awk '{print $3}' | grep '/' | grep -o '^[0-9]*' | wc -l`
  UNHEALTHY_COUNT=`cat $SHELL_OUTPUT | awk '{print $3}' | grep '/' | grep -o '^[0-9]*' | grep -v '3' | wc -l`
  if [ $UNALIVE_COUNT -gt 0 -o $PARTITION_COUNT -eq 0 -o $UNHEALTHY_COUNT -gt 0 ]
  then
    if [ $WAIT_SECONDS -gt $UNHEALTHY_TIMEOUT ]
    then
      echo "[`date`] not healthy for $WAIT_SECONDS seconds, stop kill test"
      stop_kill_test
    fi
    WAIT_SECONDS=$((WAIT_SECONDS+1))
    sleep 1
    continue
  else
    echo "[`date`] healthy now, waited for $WAIT_SECONDS seconds"
    echo "---------------------------"
    cat $SHELL_OUTPUT | grep '^[0-9]\|^primary_meta_server'
    echo "---------------------------"
    echo
  fi

  echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>"
  if [ "$KILL_TYPE" = "meta" ]
  then
    TYPE=meta
  elif [ "$KILL_TYPE" = "replica" ]
  then
    TYPE=replica
  else
    R=$((RANDOM % KILL_META_PROB))
    if [ $R -eq 0 ]
    then
      TYPE=meta
    else
      TYPE=replica
    fi
  fi
  if [ "$TYPE" = "meta" ]
  then
    OPT="-m"
    TASK_ID=$((RANDOM % META_COUNT + 1))
  else
    OPT="-r"
    TASK_ID=$((RANDOM % REPLICA_COUNT + 1))
  fi
  ###
  SECONDS=$((RANDOM % SLEEP_TIME + 1))
  echo "[`date`] sleep for $SECONDS seconds before stopping $TYPE #$TASK_ID"
  sleep $SECONDS
  ###
  echo "[`date`] stop $TYPE #$TASK_ID"
  ./run.sh stop_onebox_instance $OPT $TASK_ID
  WAIT_STOP=0
  while ./run.sh list_onebox | grep "\<$TYPE@$TASK_ID\>" &>/dev/null
  do
    if [ $WAIT_STOP -gt 10 ]
    then
      echo "[`date`] wait stopping of $TYPE #$TASK_ID for $WAIT_STOP seconds"
    fi
    sleep 1
    WAIT_STOP=$((WAIT_STOP+1))
  done
  ###
  SECONDS=$((RANDOM % LEASE_TIMEOUT + 1))
  echo "[`date`] sleep for $SECONDS seconds before starting $TYPE #$TASK_ID"
  sleep $SECONDS
  ###
  echo "[`date`] start $TYPE #$TASK_ID"
  ./run.sh start_onebox_instance $OPT $TASK_ID
  WAIT_SECONDS=0
  while true
  do
    echo "[`date`] sleep for $LEASE_TIMEOUT seconds before checking cluster healthy"
    sleep $LEASE_TIMEOUT
    WAIT_SECONDS=$((WAIT_SECONDS+LEASE_TIMEOUT))
    if ./run.sh list_onebox | grep "\<$TYPE@$TASK_ID\>" &>/dev/null
    then
      break
    fi
    cd onebox/${TYPE}${TASK_ID}/data/log
    if ls --sort=time -r | tail -n 1 | xargs grep "Address already in use"
    then
      cd $shell_dir
      PORT=$((34800+TASK_ID))
      netstat -nap 2>/dev/null | grep '^tcp' | grep "\<$PORT\>"
      echo "[`date`] start $TYPE #$TASK_ID again"
      ./run.sh start_onebox_instance $OPT $TASK_ID
      WAIT_SECONDS=0
    else
      cd $shell_dir
      echo "[`date`] start $TYPE #$TASK_ID failed, stop kill test"
      stop_kill_test
    fi
  done
done

