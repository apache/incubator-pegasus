#!/bin/bash

if [ $# -lt 2 ]
then
  echo "This tool is for manual compact specified table(app)."
  echo "USAGE: $0 <cluster-meta-list> <app-name> [opts]"
  echo "where opts including:"
  echo "  ============================================================================="
  echo "  | Name                        | ValueType                         | Default |"
  echo "  |---------------------------------------------------------------------------|"
  echo "  | target_level                | number in range of [1,num_levels] | -1      |"
  echo "  | bottommost_level_compaction | skip or force                     | skip    |"
  echo "  ============================================================================="
  echo "for example:"
  echo "  $0 127.0.0.1:34601,127.0.0.1:34602 temp target_level=2,bottommost_level_compaction=force"
  exit -1
fi

pwd="$( cd "$( dirname "$0"  )" && pwd )"
shell_dir="$( cd $pwd/.. && pwd )"
cd $shell_dir

cluster=$1
app_name=$2
if [ $# -ge 3 -a "$3" != "" ]; then
  opts="opts:$3"
fi

echo "Start time: `date`"
all_start_time=$((`date +%s`))
echo

echo "set_meta_level steady" | ./run.sh shell --cluster $cluster &>/tmp/$UID.pegasus.set_meta_level

echo ls | ./run.sh shell --cluster $cluster &>/tmp/$UID.pegasus.ls

while read app_line
do
  status=`echo $app_line | awk '{print $2}'`
  if [ "$status" = "AVAILABLE" ]
  then
    gid=`echo $app_line | awk '{print $1}'`
    app=`echo $app_line | awk '{print $3}'`
    partition_count=`echo $app_line | awk '{print $5}'`
    if [ "$app_name" != "$app" ]
    then
      continue
    fi

    echo "Send remote command manual-compact to replica servers, logging in /tmp/$UID.pegasus.manual_compact.$app"
    echo "remote_command -t replica-server replica.manual-compact $opts $gid" | ./run.sh shell --cluster $cluster &>/tmp/$UID.pegasus.manual_compact.$app
    not_found_count=`grep '^    .*not found' /tmp/$UID.pegasus.manual_compact.$app | wc -l`
    started_count=`grep '^    .*started' /tmp/$UID.pegasus.manual_compact.$app | wc -l`
    ignored_count=`grep '^    .*ignored' /tmp/$UID.pegasus.manual_compact.$app | wc -l`
    echo "Result: total $partition_count partitions, $started_count started, $ignored_count ignored, $not_found_count not found."
    echo

    echo "Checking manual compact progress..."
    sleeped=0
    while true
    do
      echo "remote_command -t replica-server replica.query-compact $gid" | ./run.sh shell --cluster $cluster &>/tmp/$UID.pegasus.query_compact.$app
      queue_count=`grep 'recent enqueue at' /tmp/$UID.pegasus.query_compact.$app | grep -v 'recent start at' | wc -l`
      running_count=`grep 'recent start at' /tmp/$UID.pegasus.query_compact.$app | wc -l`
      not_finish_count=$((queue_count+running_count))
      finish_count=$((started_count - not_finish_count))
      if [ $not_finish_count -eq 0 ]; then
        echo "All finished."
        break
      else
        left_time=unknown
        if [ $finish_count -gt 0 ]; then
          left_time=$((sleeped * started_count / finish_count - sleeped))
        fi
        echo "[${sleeped}s] $finish_count finished, $not_finish_count not finished ($queue_count in queue, $running_count in running), estimate remaining $left_time seconds."
        sleep 5
        sleeped=$((sleeped + 5))
      fi
    done
    echo
  fi
done </tmp/$UID.pegasus.ls

echo "Finish time: `date`"
all_finish_time=$((`date +%s`))
echo "Manual compact done, elasped time is $((all_finish_time - all_start_time)) seconds."

