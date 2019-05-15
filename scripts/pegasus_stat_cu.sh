#!/bin/bash

PID=$$

if [ $# -lt 2 ]; then
  echo "USGAE: $0 <cluster> <filter1> [<filter2> ...]"
  echo "   eg: $0 onebox 2017-07 2017-08 2017-09"
  echo
  echo "Result: <app_name> <data_size> <read_cu> <write_cu>"
  exit 1
fi

cluster=$1
shift

pwd="$( cd "$( dirname "$0"  )" && pwd )"
shell_dir="$( cd $pwd/.. && pwd )"
cd $shell_dir

cu_stat_table="stat"
app_stat_result="/tmp/$UID.$PID.pegasus.stat_cu.app_stat_result"
tmp_file="/tmp/$UID.$PID.pegasus.stat_cu.app_stat"
echo "app_stat -o $app_stat_result" | ./run.sh shell &>$tmp_file
app_stat_ok=`grep "succeed" $tmp_file | wc -l`
if [ $app_stat_ok -ne 1 ]; then
  echo "ERROR: app stat failed, refer error to $tmp_file"
  exit 1
fi

app_info="/tmp/$UID.$PID.pegasus.stat_cu.app_info"
app_name_column=`cat $app_stat_result | awk '/app_name/{ for(i = 1; i <= NF; i++) { if ($i == "app_name") print i; } }'`
data_size_column=`cat $app_stat_result | awk '/file_mb/{ for(i = 1; i <= NF; i++) { if ($i == "file_mb") print i; } }'`
cat $app_stat_result | tail -n +2| head -n -1 | awk '{print $'$app_name_column',$'$data_size_column'}' > $app_info

app_count=`cat $app_info | wc -l`
total_data_size=0
total_read_cu=0
total_write_cu=0
while read line
do
  app_name=`echo $line | awk '{print $1}'`
  data_size=`echo $line | awk '{print $2}' |sed 's/\.00$//'`
  data_size=$(((data_size+1023)/1024))
  cu_result="/tmp/$UID.$PID.pegasus.stat_cu.cu_result.$app_name"
  for filter in $*; do
    result_file="/tmp/$UID.$PID.pegasus.stat_cu.scan_result.$app_name.$filter"
    tmp_file="/tmp/$UID.$PID.pegasus.stat_cu.scan"
    echo -e "use $cu_stat_table\nfull_scan -h prefix -x \"$filter\" -o $result_file" | ./run.sh shell &>$tmp_file
    scan_ok=`grep 'key-value pairs got' $tmp_file | wc -l`
    if [ $scan_ok -ne 1 ]; then
      echo "ERROR: scan cu stat table failed, refer error to $tmp_file"
      rm -f $result_file
      exit 1
    fi
    cat $result_file | grep -o ''$app_name'\\":\\"\[[0-9]*,[0-9]*\]' >>$cu_result
  done
  read_cu=`cat $cu_result | grep -o '[0-9]*,[0-9]*' | awk -F, '{a+=$1}END{print a}'`
  write_cu=`cat $cu_result | grep -o '[0-9]*,[0-9]*' | awk -F, '{a+=$2}END{print a}'`
  echo "$app_name $data_size $read_cu $write_cu"
  total_data_size=$((total_data_size + data_size))
  total_read_cu=$((total_read_cu + read_cu))
  total_write_cu=$((total_write_cu + write_cu))
done <$app_info
echo "(total:$app_count) $total_data_size $total_read_cu $total_write_cu"


rm -f /tmp/$UID.$PID.pegasus.* &>/dev/null
