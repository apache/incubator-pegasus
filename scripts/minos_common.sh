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

#
# You should set these environment variables:
#   * MINOS_CONFIG_FILE
#   * MINOS_CLIENT_DIR
#   * MINOS2_CONFIG_FILE
#   * MINOS2_CLIENT_DIR
#
# For example:
#   export MINOS_CONFIG_FILE=$HOME/infra/deployment-config/deploy.cfg
#   export MINOS_CLIENT_DIR=$HOME/infra/minos/client
#   export MINOS2_CONFIG_FILE=$HOME/infra/deployment/deploy.cfg
#   export MINOS2_CLIENT_DIR=$HOME/infra/minos2/client
#

# usage: find_cluster <cluster_name>
#
# return 0 if found
#
# if found, then these global variables will be set:
#   * minos_type
#   * minos_config
#   * minos_client_dir
function find_cluster()
{
  if [ -n "$MINOS_CONFIG_FILE" -a -n "$MINOS_CLIENT_DIR" ]; then
    minos_type=1
    minos_config=$(dirname $MINOS_CONFIG_FILE)/xiaomi-config/conf/pegasus/pegasus-${1}.cfg
    minos_client_dir=$MINOS_CLIENT_DIR
    if [ -f "$minos_config" -a -f "$minos_client_dir/deploy" ]; then
      return 0
    fi
  fi

  if [ -n "$MINOS2_CONFIG_FILE" -a -n "$MINOS2_CLIENT_DIR" ]; then
    minos_type=2
    minos_config=$(dirname $MINOS2_CONFIG_FILE)/xiaomi-config/conf/pegasus/pegasus-${1}.yaml
    minos_client_dir=$MINOS2_CLIENT_DIR
    if [ -f "$minos_config" -a -f "$minos_client_dir/deploy" ]; then
      return 0
    fi
  fi

  return 1
}

# usage: minos_show_replica <cluster_name> <result_file>
function minos_show_replica()
{
  local pwd=`pwd`
  local tmp_file="/tmp/$UID.$PID.pegasus.minos.show"
  cd $minos_client_dir
  ./deploy show pegasus $1 --job replica &>$tmp_file
  if [ $? -ne 0 ]; then
    echo "ERROR: minos show replica failed, refer to $tmp_file"
    exit 1
  fi
  if [ $minos_type -eq 1 ]; then
    grep -o 'Showing task [0-9][0-9]* of replica on [^(]*' $tmp_file | awk '{print $3,$7}' >$2
  else
    grep -o 'Task [0-9][0-9]* of replica on [^:]*' $tmp_file | awk '{print $2,$6}' >$2
  fi
  cd $pwd
}

# usage: minos_rolling_update <cluster_name> <job_name> [task_id]
function minos_rolling_update()
{
  local pwd=`pwd`
  local options="--job $2"
  if [ -n "$3" ]; then
    options="$options --task $3"
  fi
  options="$options --update_package --update_config --time_interval 20 --skip_confirm"
  if [ $minos_type -eq 2 ]; then
    options="$options --confirm_install"
  fi
  cd $minos_client_dir
  echo "./deploy rolling_update pegasus $1 $options"
  ./deploy rolling_update pegasus $1 $options
  if [ $? -ne 0 ]; then
    echo "ERROR: minos rolling update failed"
    exit 1
  fi
  cd $pwd
}

# usage: minos_stop <cluster_name> <job_name> [task_id]
function minos_stop()
{
  local pwd=`pwd`
  local options="--job $2"
  if [ -n "$3" ]; then
    options="$options --task $3"
  fi
  options="$options --skip_confirm"
  cd $minos_client_dir
  echo "./deploy stop pegasus $1 $options"
  ./deploy stop pegasus $1 $options
  if [ $? -ne 0 ]; then
    echo "ERROR: minos stop failed"
    exit 1
  fi
  cd $pwd
}

# usage: minos_restart <cluster_name> <job_name> [task_id]
function minos_restart()
{
  local pwd=`pwd`
  local options="--job $2"
  if [ -n "$3" ]; then
    options="$options --task $3"
  fi
  options="$options --skip_confirm"
  cd $minos_client_dir
  echo "./deploy restart pegasus $1 $options"
  ./deploy restart pegasus $1 $options
  if [ $? -ne 0 ]; then
    echo "ERROR: minos restart failed"
    exit 1
  fi
  cd $pwd
}

# usage: minos_bootstrap <cluster_name> <job_name> [task_id]
function minos_bootstrap()
{
  local pwd=`pwd`
  local options="--job $2"
  if [ -n "$3" ]; then
    options="$options --task $3"
  fi
  cd $minos_client_dir
  if [ $minos_type -eq 2 ]; then
    # minos2 have no "bootstrap" function,use "start" function
    echo "./deploy start pegasus $1 $options"
    ./deploy start pegasus $1 $options
  else
    echo "./deploy bootstrap pegasus $1 $options"
    ./deploy bootstrap pegasus $1 $options
  fi
  if [ $? -ne 0 ]; then
    echo "ERROR: minos bootstrap failed"
    exit 1
  fi
  cd $pwd
}
