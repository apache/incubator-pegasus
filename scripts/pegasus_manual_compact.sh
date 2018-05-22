#!/bin/bash

function usage()
{
  echo "This tool is for manual compact specified table(app)."
  echo "USAGE: $0 -c cluster -a app-name [-t periodic|once] -g trigger-time [-d true|false] [-o k1=v1,k2=v2]"
  echo "Options:"
  echo "  -h|--help"
  echo "  -c|--cluster          cluster meta server list, default is \"127.0.0.1:34601,127.0.0.1:34602\""
  echo "  -t|--type             manual compact type, should be periodic or once, default is once"
  echo "  -g|--trigger_time     manual compact trigger time"
  echo "                        24-hour format for periodic type, e.g. \"3:00,21:00\" for 3:00 and 21:00 everyday"
  echo "                        unix timestamp format for once type, e.g. \"1514736000\" for Jan. 1, 00:00:00 CST 2018"
  echo "  -a|--app_name         manual compact target table(app) name"
  echo "  -d|--disable_periodic whether to disable periodic manual compact, default is false which is not disable"
  echo "  --target_level        number in range of [1,num_levels], default is -1"
  echo "  --bottommost_level_compaction     skip or force, default is skip"
  echo "                        more details: https://github.com/facebook/rocksdb/wiki/Manual-Compaction"
  echo
  echo "for example:"
  echo "$0 127.0.0.1:34601,127.0.0.1:34602 -t periodic -g 3:00,21:00 -a temp -o target_level=2,bottommost_level_compaction=force"
}

# set_env cluster app_name type env_key env_value
function set_env()
{
    cluster=$1
    app_name=$2
    type=$3
    env_key=$4
    env_value=$5
    periodic_prefix="manual_compact.periodic."
    once_prefix="manual_compact.once."

    if [ "${type}" == "periodic" ]; then
        env_key=${periodic_prefix}${env_key}
    elif [ "${type}" == "once" ]; then
        env_key=${once_prefix}${env_key}
    else
        echo "invalid type: ${type}"
        usage
        exit -1
    fi

    echo "set_app_envs ${env_key}=${env_value}"
    set_envs_log_file="/tmp/$UID.pegasus.set_app_envs.${app_name}"
    echo -e "use ${app_name}\n set_app_envs ${env_key} ${env_value}" | ./run.sh shell --cluster ${cluster} &>${set_envs_log_file}
    set_ok=`grep 'set app envs succeed' ${set_envs_log_file} | wc -l`
    if [ ${set_ok} -ne 1 ]; then
      echo "ERROR: set app envs failed, refer to ${set_envs_log_file}"
      exit -1
    fi
}

# wait_manual_compact app_id trigger_time total_replica_count
function wait_manual_compact()
{
  app_id=$1
  trigger_time=$2
  total_replica_count=$3

  echo "Checking manual compact progress..."
  query_cmd="remote_command -t replica-server replica.query-compact ${app_id}"
  earliest_finish_time_ms=$(date -d @${trigger_time} +"%Y-%m-%d %H:%M:%S.000")
  slept=0
  while true
  do
    query_log_file="/tmp/$UID.pegasus.query_compact.${app_id}"
    echo "${query_cmd}" | ./run.sh shell --cluster ${cluster} &>${query_log_file}

    queue_count=`grep 'recent enqueue at' ${query_log_file} | grep -v 'recent start at' | wc -l`
    running_count=`grep 'recent start at' ${query_log_file} | wc -l`
    not_finish_count=$((queue_count+running_count))
    finish_count=`grep "last finish at" ${query_log_file} | grep -v "recent enqueue at" | grep -v "recent start at" | awk -F"[\[\]]" 'BEGIN{count=0}{if(length($2)==23 && $2>=$earliest_finish_time_ms){count++;}}END{print count}'`

    if [ ${finish_count} -eq ${total_replica_count} ]; then
      echo "All finished."
      break
    else
      left_time="unknown"
      if [ ${finish_count} -gt 0 ]; then
        left_time=$((slept / finish_count * not_finish_count))
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
  cluster=$1
  app_id=$2

  echo "start to create checkpoint..."
  chkpt_log_file="/tmp/$UID.pegasus.trigger_checkpoint.${app_id}"
  echo "remote_command -t replica-server replica.trigger-checkpoint ${app_id}" | ./run.sh shell --cluster ${cluster} &>${chkpt_log_file}
  not_found_count=`grep '^    .*not found' ${chkpt_log_file} | wc -l`
  triggered_count=`grep '^    .*triggered' ${chkpt_log_file} | wc -l`
  ignored_count=`grep '^    .*ignored' ${chkpt_log_file} | wc -l`
  echo "Result: total $partition_count partitions, $triggered_count triggered, $ignored_count ignored, $not_found_count not found."
  echo
}

# parse parameters
cluster="127.0.0.1:34601,127.0.0.1:34602"
type="once"
trigger_time=""
app_name=""
disable_periodic=""
target_level="-1"
bottommost_level_compaction="skip"
while [[ $# > 0 ]]; do
    option_key="$1"
    case ${option_key} in
        -c|--cluster)
            cluster="$2"
            shift
            ;;
        -t|--type)
            type="$2"
            shift
            ;;
        -g|--trigger_time)
            trigger_time="$2"
            ;;
        -a|--app_name)
            app_name="$2"
            ;;
        -d|--disable_periodic)
            disable_periodic="$2"
            ;;
        --target_level)
            target_level="$2"
            ;;
        --bottommost_level_compaction)
            bottommost_level_compaction="$2"
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

# check type
if [ "${type}" != "periodic" -a "${type}" != "once" ]; then
    echo "invalid type: ${type}"
    usage
    exit -1
fi

# check app_name
if [ "${app_name}" == "" ]; then
    echo "invalid app_name: ${app_name}"
    usage
    exit -1
fi

# check trigger_time
if [ "${trigger_time}" == "" ]; then
    echo "invalid trigger_time: ${trigger_time}"
    usage
    exit -1
fi

# check disable_periodic
if [ "${disable_periodic}" != "" ]; then
    if [ "${type}" != "periodic" ]; then
        echo "disable_periodic is meaningless when type is ${type}"
        usage
        exit -1
    fi
    if [ "${disable_periodic}" != "true" -a "${disable_periodic}" != "false" ]; then
        echo "invalid disable_periodic: ${disable_periodic}"
        usage
        exit -1
    fi
fi

# check target_level
if [ ${target_level} -lt -1 ]; then
    echo "invalid target_level: ${target_level}"
    usage
    exit -1
fi

# check bottommost_level_compaction
if [ "${bottommost_level_compaction}" != "skip" -a "${bottommost_level_compaction}" != "force" ]; then
    echo "invalid bottommost_level_compaction: ${bottommost_level_compaction}"
    usage
    exit -1
fi

# record start time
all_start_time=`date +%s`
echo "Start time: `date -d @${all_start_time} +"%Y-%m-%d %H:%M:%S"`"
echo

# set steady
echo "set_meta_level steady" | ./run.sh shell --cluster ${cluster} &>/tmp/$UID.pegasus.set_meta_level

# set manual compact envs
if [ "${target_level}" != "" ]; then
    set_env ${cluster} ${app_name} ${type} "target_level" ${target_level}
fi
if [ "${bottommost_level_compaction}" != "" ]; then
    set_env ${cluster} ${app_name} ${type} "bottommost_level_compaction" ${bottommost_level_compaction}
fi
if [ "${disable_periodic}" != "" ]; then
    set_env ${cluster} ${app_name} ${type} "disabled" ${disable_periodic}
fi
set_env ${cluster} ${app_name} ${type} "trigger_time" ${trigger_time}
echo

# only `once` manual compact will check progress
if [ "${type}" != "once" ]; then
    exit 0
fi

ls_log_file="/tmp/$UID.pegasus.ls"
echo ls | ./run.sh shell --cluster ${cluster} &>${ls_log_file}
# app_id    status              app_name            app_type            partition_count     replica_count       is_stateful         drop_expire_time    envs
# 1         AVAILABLE           temp                pegasus             8                   3                   true                -                   {...}
# ...

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
    exit -1
  fi

  wait_manual_compact ${app_id} ${trigger_time} $(($partition_count*$replica_count))

  create_checkpoint ${cluster} ${app_id}
done <${ls_log_file}

# record finish time
all_finish_time=`date +%s`
echo "Finish time: `date -d @${all_finish_time} +"%Y-%m-%d %H:%M:%S"`"
echo "Manual compact done, elapsed time is $((all_finish_time - all_start_time)) seconds."
