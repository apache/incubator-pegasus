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

function usage()
{
    echo "This tool is for manual compact specified table(app)."
    echo "USAGE: $0 -c cluster -a app-name [-t periodic|once] [-w] [-g trigger_time] [...]"
    echo "Options:"
    echo "  -h|--help                   print help message"
    echo
    echo "  -c|--cluster <str>          cluster meta server list, default is \"127.0.0.1:34601,127.0.0.1:34602\""
    echo
    echo "  --config <str>              config file path"
    echo
    echo "  -a|--app_name <str>         target table(app) name"
    echo
    echo "  -t|--type <str>             manual compact type, should be periodic or once, default is once"
    echo
    echo "  -w|--wait_only              this option is only used when the type is once!"
    echo "                              not trigger but only wait the last once compact to finish"
    echo
    echo "  -g|--trigger_time <str>     this option is only used when the type is periodic!"
    echo "                              specify trigger time of periodic compact in 24-hour format,"
    echo "                              e.g. \"3:00,21:00\" means 3:00 and 21:00 everyday"
    echo
    echo "  --target_level <num>        number in range of [-1,num_levels], -1 means automatically, default is -1"
    echo
    echo "  --bottommost_level_compaction <skip|force>"
    echo "                              skip or force, default is skip"
    echo "                              more details: https://github.com/facebook/rocksdb/wiki/Manual-Compaction"
    echo
    echo "  --max_concurrent_running_count <num>"
    echo "                              max concurrent running count limit, should be positive integer."
    echo "                              if not set, means no limit."
    echo
    echo "for example:"
    echo
    echo "  1) Start once type manual compact with default options:"
    echo
    echo "      $0 -c 127.0.0.1:34601,127.0.0.1:34602 -a temp"
    echo
    echo "  2) Only wait last once type manual compact to finish:"
    echo
    echo "      $0 -c 127.0.0.1:34601,127.0.0.1:34602 -a temp -w"
    echo
    echo "  3) Config periodic type manual compact with specified options:"
    echo
    echo "      $0 -c 127.0.0.1:34601,127.0.0.1:34602 -a temp -t periodic -g 3:00,21:00 \\"
    echo "           --target_level 2 --bottommost_level_compaction force"
    echo
}

# determine whether the passed parameter is --cluster or --config.
function is_cluster()
{
    # true:str is cluster, false: str is config
    tmp_str=$1
    if [[ ${tmp_str} == *":"* ]]; then
        echo "true"
    else
        echo "false"
    fi
}

# get_env cluster app_name key
function get_env()
{
    tmp_str=$1
    app_name=$2
    key=$3

    log_file="/tmp/$UID.$PID.pegasus.get_app_envs.${app_name}"
    if [ `is_cluster ${tmp_str}` == "true" ]; then
        echo -e "use ${app_name}\n get_app_envs" | ./run.sh shell --cluster ${tmp_str} &>${log_file}
    else
        echo -e "use ${app_name}\n get_app_envs" | ./run.sh shell --config ${tmp_str} &>${log_file}
    fi
    get_fail=`grep 'get app env failed' ${log_file} | wc -l`
    if [ ${get_fail} -eq 1 ]; then
        echo "ERROR: get app envs failed, refer to ${log_file}"
        exit 1
    fi
    grep "^${key} =" ${log_file} | awk '{print $3}'
}

# set_env cluster app_name key value
function set_env()
{
    tmp_str=$1
    app_name=$2
    key=$3
    value=$4

    echo "set_app_envs ${key}=${value}"
    log_file="/tmp/$UID.$PID.pegasus.set_app_envs.${app_name}"
    if [ `is_cluster ${tmp_str}` == "true" ]; then
        echo -e "use ${app_name}\n set_app_envs ${key} ${value}" | ./run.sh shell --cluster ${tmp_str} &>${log_file}
    else
        echo -e "use ${app_name}\n set_app_envs ${key} ${value}" | ./run.sh shell --config ${tmp_str} &>${log_file}
    fi
    set_fail=`grep 'set app env failed' ${log_file} | wc -l`
    if [ ${set_fail} -eq 1 ]; then
        echo "ERROR: set app envs failed, refer to ${log_file}"
        exit 1
    fi
}

# wait_manual_compact app_id trigger_time total_replica_count
function wait_manual_compact()
{
    app_id=$1
    trigger_time=$2
    total_replica_count=$3

    query_cmd="remote_command -t replica-server replica.query-compact ${app_id}"
    earliest_finish_time_ms=$(date -d @${trigger_time} +"%Y-%m-%d %H:%M:%S.000")
    echo "Checking once compact progress since [$trigger_time] [$earliest_finish_time_ms]..."

    slept=0
    while true
    do
        query_log_file="/tmp/$UID.$PID.pegasus.query_compact.${app_id}"
        if [ "$cluster" != "" ]; then
            echo "${query_cmd}" | ./run.sh shell --cluster ${cluster} &>${query_log_file}
        else
            echo "${query_cmd}" | ./run.sh shell --config ${config} &>${query_log_file}
        fi

        queue_count=$(awk 'BEGIN {count=0} {match($0, /"recent_enqueue_at":"([^"]+)"/, enqueue); match($0, /"recent_start_at":"([^"]+)"/, start); if (1 in enqueue && enqueue[1] != "-" && start[1] == "-") {count++}} END {print count}' "$query_log_file")
        running_count=$(awk 'BEGIN {count=0} {match($0, /"recent_start_at":"([^"]+)"/, start); if (1 in start && start[1] != "-") {count++}} END {print count}' "$query_log_file")
        processing_count=$((queue_count+running_count))
        finish_count=$(awk -v date="$earliest_finish_time_ms" 'BEGIN {count=0} {match($0, /"recent_enqueue_at":"([^"]+)"/, enqueue); match($0, /"recent_start_at":"([^"]+)"/, start); match($0, /"last_finish":"([^"]+)"/, finish); if (enqueue[1] == "-" && start[1] == "-" && 1 in finish && finish[1] >= date) {count++}} END {print count}' "$query_log_file")
        not_finish_count=$((total_replica_count-finish_count))

        if [ ${processing_count} -eq 0 -a ${finish_count} -eq ${total_replica_count} ]; then
            echo "[${slept}s] $finish_count finished, $not_finish_count not finished ($queue_count in queue, $running_count in running), estimate remaining 0 seconds."
            echo "All finished, total $total_replica_count replicas."
            break
        else
            left_time="unknown"
            if [ ${finish_count} -gt 0 ]; then
              left_time=$((slept * not_finish_count / finish_count))
            fi
            echo "[${slept}s] $finish_count finished, $not_finish_count not finished ($queue_count in queue, $running_count in running), estimate remaining $left_time seconds."
            sleep 5
            slept=$((slept + 5))
        fi
    done
    echo
}

# create_checkpoint cluster app_id
function create_checkpoint()
{
    tmp_str=$1
    app_id=$2

    echo "Start to create checkpoint..."
    chkpt_log_file="/tmp/$UID.$PID.pegasus.trigger_checkpoint.${app_id}"
    if [ `is_cluster ${tmp_str}` == "true" ]; then
        echo "remote_command -t replica-server replica.trigger-checkpoint ${app_id}" | ./run.sh shell --cluster ${tmp_str} &>${chkpt_log_file}
    else
        echo "remote_command -t replica-server replica.trigger-checkpoint ${app_id}" | ./run.sh shell --config ${tmp_str} &>${chkpt_log_file}
    fi
    not_found_count=`grep '^    .*not found' ${chkpt_log_file} | wc -l`
    triggered_count=`grep '^    .*triggered' ${chkpt_log_file} | wc -l`
    ignored_count=`grep '^    .*ignored' ${chkpt_log_file} | wc -l`
    echo "Result: total $partition_count partitions, $triggered_count triggered, $ignored_count ignored, $not_found_count not found."
    echo
}

if [ $# -eq 0 ]; then
    usage
    exit 0
fi

# parse parameters
cluster=""
config=""
app_name=""
type="once"
trigger_time=""
wait_only="false"
target_level="-1"
bottommost_level_compaction="skip"
max_concurrent_running_count=""
while [[ $# > 0 ]]; do
    option_key="$1"
    case ${option_key} in
        -c|--cluster)
            cluster="$2"
            shift
            ;;
        --config)
            config="$2"
            shift
            ;;
        -t|--type)
            type="$2"
            shift
            ;;
        -g|--trigger_time)
            trigger_time="$2"
            shift
            ;;
        -a|--app_name)
            app_name="$2"
            shift
            ;;
        -w|--wait_only)
            wait_only="true"
            ;;
        --target_level)
            target_level="$2"
            shift
            ;;
        --bottommost_level_compaction)
            bottommost_level_compaction="$2"
            shift
            ;;
        --max_concurrent_running_count)
            max_concurrent_running_count="$2"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
    esac
    shift
done

# cd to shell dir
pwd="$(cd "$(dirname "$0")" && pwd)"
shell_dir="$(cd ${pwd}/.. && pwd )"
cd ${shell_dir}

# check cluster and config
if [ "${cluster}" == "" -a "${config}" == "" ]; then
    echo "ERROR: invalid cluster: ${cluster}, config: ${config}"
    exit 1
fi

if [ "${cluster}" != "" -a "${config}" != "" ]; then
    echo "ERROR: cluster and config cannot be set at the same time."
    exit 1
fi

# check app_name
if [ "${app_name}" == "" ]; then
    echo "ERROR: invalid app_name: ${app_name}"
    exit 1
fi

# check type
if [ "${type}" != "periodic" -a "${type}" != "once" ]; then
    echo "ERROR: invalid type: ${type}"
    exit 1
fi

# check wait_only
if [ "${wait_only}" == "true" -a "${type}" != "once" ]; then
    echo "ERROR: can not specify wait_only when type is ${type}"
    exit 1
fi

# check trigger_time
if [ "${type}" == "once" ]; then
    if [ "${trigger_time}" != "" ]; then
        echo "ERROR: can not specify trigger_time when type is ${type}"
        exit 1
    fi
    if [ "${wait_only}" == "true" ]; then
        if [ "${cluster}" != "" ]; then
            trigger_time=`get_env ${cluster} ${app_name} "manual_compact.once.trigger_time"`
        else
            trigger_time=`get_env ${config} ${app_name} "manual_compact.once.trigger_time"`
        fi
        if [ "${trigger_time}" == "" ]; then
            echo "No once compact triggered previously, nothing to wait"
            exit 1
        fi
    else
       trigger_time=`date +%s`
    fi
else # type == periodic
    if [ "${trigger_time}" == "" ]; then
        echo "ERROR: should specify trigger_time when type is ${type}"
        exit 1
    fi
fi

# check target_level
expr ${target_level} + 0 &>/dev/null
if [ $? -ne 0 ]; then
    echo "ERROR: invalid target_level: ${target_level}"
    exit 1
fi
if [ ${target_level} -lt -1 ]; then
    echo "ERROR: invalid target_level: ${target_level}"
    exit 1
fi

# check bottommost_level_compaction
if [ "${bottommost_level_compaction}" != "skip" -a "${bottommost_level_compaction}" != "force" ]; then
    echo "ERROR: invalid bottommost_level_compaction: ${bottommost_level_compaction}"
    exit 1
fi

# check max_concurrent_running_count
if [ "${max_concurrent_running_count}" != "" ]; then
    expr ${max_concurrent_running_count} + 0 &>/dev/null
    if [ $? -ne 0 ]; then
        echo "ERROR: invalid max_concurrent_running_count: ${max_concurrent_running_count}"
        exit 1
    fi
    if [ ${max_concurrent_running_count} -lt 0 ]; then
        echo "ERROR: invalid max_concurrent_running_count: ${max_concurrent_running_count}"
        exit 1
    fi
fi

# record start time
all_start_time=`date +%s`
echo "UID: $UID"
echo "PID: $PID"
echo "cluster: $cluster"
echo "app_name: $app_name"
echo "type: $type"
echo "Start time: `date -d @${all_start_time} +"%Y-%m-%d %H:%M:%S"`"
echo

if [ "${type}" == "periodic" ] || [ "${type}" == "once" -a "${wait_only}" == "false" ]; then
    # set steady
    if [ "${cluster}" != "" ]; then
        echo "set_meta_level steady" | ./run.sh shell --cluster ${cluster} &>/tmp/$UID.$PID.pegasus.set_meta_level
    else
        echo "set_meta_level steady" | ./run.sh shell --config ${config} &>/tmp/$UID.$PID.pegasus.set_meta_level
    fi

    # set manual compact envs
    if [ "${target_level}" != "" ]; then
        if [ "${cluster}" != "" ]; then
            set_env ${cluster} ${app_name} "manual_compact.${type}.target_level" ${target_level}
        else
            set_env ${config} ${app_name} "manual_compact.${type}.target_level" ${target_level}
        fi
    fi
    if [ "${bottommost_level_compaction}" != "" ]; then
        if [ "${cluster}" != "" ]; then
            set_env ${cluster} ${app_name} "manual_compact.${type}.bottommost_level_compaction" ${bottommost_level_compaction}
        else
            set_env ${config} ${app_name} "manual_compact.${type}.bottommost_level_compaction" ${bottommost_level_compaction}
        fi
    fi
    if [ "${max_concurrent_running_count}" != "" ]; then
        if [ "${cluster}" != "" ]; then
            set_env ${cluster} ${app_name} "manual_compact.max_concurrent_running_count" ${max_concurrent_running_count}
        else
            set_env ${config} ${app_name} "manual_compact.max_concurrent_running_count" ${max_concurrent_running_count}
        fi
    fi
    if [ "${cluster}" != "" ]; then
        set_env ${cluster} ${app_name} "manual_compact.${type}.trigger_time" ${trigger_time}
    else
        set_env ${config} ${app_name} "manual_compact.${type}.trigger_time" ${trigger_time}
    fi
    echo
fi

# only `once` manual compact will check progress
if [ "${type}" != "once" ]; then
    rm -f /tmp/$UID.$PID.pegasus.* &>/dev/null
    exit 0
fi

ls_log_file="/tmp/$UID.$PID.pegasus.ls"
if [ "${cluster}" != "" ]; then
    echo ls | ./run.sh shell --cluster ${cluster} &>${ls_log_file}
else
    echo ls | ./run.sh shell --config ${config} &>${ls_log_file}
fi

while read app_line
do
    app_id=`echo ${app_line} | awk '{print $1}'`
    status=`echo ${app_line} | awk '{print $2}'`
    app=`echo ${app_line} | awk '{print $3}'`
    partition_count=`echo ${app_line} | awk '{print $5}'`
    replica_count=`echo ${app_line} | awk '{print $6}'`

    if [ "${app_name}" != "$app" ]; then
        continue
    fi

    if [ "$status" != "AVAILABLE" ]; then
        echo "app ${app_name} is not available now, try to query result later"
        exit 1
    fi

    wait_manual_compact ${app_id} ${trigger_time} $(($partition_count*$replica_count))

    #create_checkpoint ${cluster} ${app_id}
done <${ls_log_file}

# record finish time
all_finish_time=`date +%s`
echo "Finish time: `date -d @${all_finish_time} +"%Y-%m-%d %H:%M:%S"`"
echo "Manual compact done, elapsed time is $((all_finish_time - all_start_time)) seconds."

rm -f /tmp/$UID.$PID.pegasus.* &>/dev/null
