#!/bin/bash

PID=$$

if [ $# -lt 2 ]; then
  echo "USGAE: $0 <cluster> <filter1> [<filter2> ...]"
  echo "   eg: $0 onebox 2017-07 2017-08 2017-09"
  echo
  echo "Result: <cluster> <serve_minutes> <available> <app_count> <data_size>"
  exit 1
fi

cluster=$1
shift

pwd="$( cd "$( dirname "$0"  )" && pwd )"
shell_dir="$( cd $pwd/.. && pwd )"
cd $shell_dir

detect_table="test"
app_stat_result="/tmp/$UID.$PID.pegasus.stat_available.app_stat_result"
tmp_file="/tmp/$UID.$PID.pegasus.stat_available.app_stat"
echo "app_stat -o $app_stat_result" | ./run.sh shell -n $cluster &>$tmp_file
app_stat_ok=`grep "succeed" $tmp_file | wc -l`
if [ $app_stat_ok -ne 1 ]; then
  echo "ERROR: app stat failed, refer error to $tmp_file"
  exit 1
fi

app_count=`cat $app_stat_result | wc -l`
app_count=$((app_count-2))
data_size_column=`cat $app_stat_result | awk '/file_mb/{ for(i = 1; i <= NF; i++) { if ($i == "file_mb") print i; } }'`
data_size=`cat $app_stat_result | tail -n 1 | awk '{print $'$data_size_column'}' | sed 's/\.00$//'`
data_size=$(((data_size+1023)/1024))

all_result="/tmp/$UID.$PID.pegasus.stat_available.all_result"
rm -f $all_result
for filter in $*; do
  result_file="/tmp/$UID.$PID.pegasus.stat_available.scan_result.$filter"
  tmp_file="/tmp/$UID.$PID.pegasus.stat_available.scan"
  echo -e "use $detect_table\nhash_scan detect_available_minute '' '' -s prefix -y \"$filter\" -o $result_file" | ./run.sh shell -n $cluster &>$tmp_file
  scan_ok=`grep 'key-value pairs got' $tmp_file | wc -l`
  if [ $scan_ok -ne 1 ]; then
    echo "ERROR: scan detect table failed, refer error to $tmp_file"
    rm -f $result_file
    exit 1
  fi
  cat $result_file >>$all_result
done

minutes=`cat $all_result | wc -l`
if [ $minutes -eq 0 ]; then
  available="0.000000"
else
  available=`cat $all_result | grep -o '[0-9]*,[0-9]*,[0-9]*' | awk -F, '{a+=$1;b+=$2}END{printf("%f\n",(double)b/a);}'`
fi
echo "$cluster $minutes $available $app_count $data_size"

cat $all_result >>/tmp/pegasus.stat_available.all_result

rm -f /tmp/$UID.$PID.pegasus.* &>/dev/null
