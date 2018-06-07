#!/bin/bash

PID=$$

if [ $# -ne 2 ]; then
  echo "USGAE: $0 <cluster> <filter>"
  echo "   eg: $0 onebox 2017-07"
  echo
  echo "Result: <cluster> <filter> <day_count> <available>"
  exit 1
fi

cluster=$1
filter=$2

pwd="$( cd "$( dirname "$0"  )" && pwd )"
shell_dir="$( cd $pwd/.. && pwd )"
minos_config_dir=$(dirname $MINOS_CONFIG_FILE)/xiaomi-config/conf/pegasus
minos_config=$minos_config_dir/pegasus-${cluster}.cfg
cd $shell_dir

if [ ! -f $minos_config ]; then
  echo "ERROR: minos config \"$minos_config\" not found"
  exit 1
fi

detect_table=`grep '^ *available_detect_app = ' $minos_config | awk '{print $3}'`
if [ "$detect_table" == "" ]; then
  echo "ERROR: get detect table from $minos_config failed"
  exit 1
fi

result_file="pegasus.stat_available.scan_result"
tmp_file="/tmp/$UID.$PID.pegasus.stat_available.scan"
echo -e "use $detect_table\nhash_scan detect_available_day '' '' -s prefix -y \"$filter\" -o $result_file" | ./run.sh shell -n $cluster &>$tmp_file
scan_ok=`grep 'key-value pairs got' $tmp_file | wc -l`
if [ $scan_ok -ne 1 ]; then
  echo "ERROR: scan detect table failed, refer error to $tmp_file"
  rm -f $result_file
  exit 1
fi

days=`cat $result_file | wc -l`
if [ $days -eq 0 ]; then
  echo "ERROR: no detect data found for filter \"$filter\", refer to $tmp_file"
  rm -f $result_file
  exit 1
fi

available=`cat $result_file | grep -o '[0-9]*,[0-9]*,[0-9]*' | awk -F, '{a+=$1;b+=$2}END{printf("%f\n",(double)b/a);}'`
rm -f $result_file
echo "$cluster $filter $days $available"

rm -f /tmp/$UID.$PID.pegasus.* &>/dev/null
