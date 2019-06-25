#!/bin/bash

PID=$$

if [ $# -lt 3 ]; then
  echo "USGAE: $0 <cluster> <stat_app> <filter1> [<filter2> ...]"
  echo "   eg: $0 onebox temp 2017-07 2017-08 2017-09"
  echo
  echo "Result: <app_id>,<app_name>,<storage_size_in_mb>,<read_cu>,<write_cu>"
  exit 1
fi

cluster=$1
if [ "$cluster" != "onebox" ]; then
  cluster_param="-n $cluster"
fi
stat_app=$2
shift
shift

pwd="$( cd "$( dirname "$0"  )" && pwd )"
shell_dir="$( cd $pwd/.. && pwd )"
cd $shell_dir

ls_result="/tmp/$UID.$PID.pegasus.stat_usage.ls_result"
echo "ls -a" | ./run.sh shell $cluster_param &>$ls_result
ls_succeed=`grep "^total_app_count " $ls_result | wc -l`
if [ $ls_succeed -ne 1 ]; then
  echo "ERROR: ls failed, refer error to $ls_result"
  exit 1
fi

app_map_file="/tmp/$UID.$PID.pegasus.stat_usage.app_map"
grep "pegasus " $ls_result | awk '{print $1","$3}' >$app_map_file
declare -A app_map=()
while read line; do
  app_id=`echo $line | awk -F, '{print $1}'`
  app_name=`echo $line | awk -F, '{print $2}'`
  app_map["$app_id"]="$app_name"
done <$app_map_file

files=""
filter_id=1
for filter in $*; do
  echo "INFO: scan usage stat by filter '$filter' ..."
  result_file="/tmp/$UID.$PID.pegasus.stat_usage.scan_result.filter${filter_id}"
  tmp_file="/tmp/$UID.$PID.pegasus.stat_usage.scan"
  echo -e "use $stat_app \nfull_scan -t 10000 -h prefix -x \"$filter\" -o $result_file" | ./run.sh shell $cluster_param &>$tmp_file
  scan_ok=`grep 'key-value pairs got' $tmp_file | wc -l`
  if [ $scan_ok -ne 1 ]; then
    echo "ERROR: scan usage stat table failed, refer error to $tmp_file"
    exit 1
  fi
  line_count=`cat $result_file | wc -l`
  echo "INFO: scan usage stat by filter '$filter' done, total $line_count lines"
  files="$files $result_file"
  filter_id=$((filter_id+1))
done

python_result="/tmp/$UID.$PID.pegasus.stat_usage.python_result"
python $shell_dir/scripts/pegasus_stat_usage.py $files &>$python_result
if [ $? -ne 0 ]; then
  echo "ERROR: run pegasus_stat_usage.py failed, refer error to $python_result"
  exit 1
fi

stat_result="/tmp/$UID.$PID.pegasus.stat_usage.stat_result"
rm -f $stat_result &>/dev/null
while read line; do
  app_id=`echo $line | awk -F, '{print $1}'`
  app_name=${app_map["$app_id"]}
  if [ "$app_name" == "" ]; then
    echo "ERROR: invalid app id $app_id"
    exit 1
  fi
  date=`echo $line | awk -F, '{print $2}'`
  storage_size=`echo $line | awk -F, '{print $3}'`
  read_cu=`echo $line | awk -F, '{print $4}'`
  write_cu=`echo $line | awk -F, '{print $5}'`
  echo "$app_id,$app_name,$date,$storage_size,$read_cu,$write_cu" >>$stat_result
done <$python_result

result="$shell_dir/stat_usage_result.$cluster.$PID"
cp $stat_result $result
echo "INFO: stat succeed, detailed result by day is in file: $result"

awk -F, '{sum1+=$4;sum2+=$5;sum3+=$6}END{print "SUM: "sum1,sum2,sum3}' $result

rm -f /tmp/$UID.$PID.pegasus.stat_usage.* &>/dev/null
