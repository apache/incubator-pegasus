#!/bin/bash

if [ $# -lt 2 ]; then
  echo "USAGE: $0 <cluster-list-file> <result-format>"
  echo
  echo "The result format must be 'table' or 'csv'."
  echo
  echo "For example:"
  echo "  $0 \"clusters.txt\" \"table\""
  echo
  exit 1
fi

PID=$$
clusters_file=$1
format=$2
if [ "$format" != "table" -a "$format" != "csv" ]; then
  echo "ERROR: invalid result format, should be 'table' or 'csv'."
  exit 1
fi

pwd="$( cd "$( dirname "$0"  )" && pwd )"
shell_dir="$( cd $pwd/.. && pwd )"
cd $shell_dir

echo "show_time = `date`"
echo
echo "Columns:"
echo "  - cluster: name of the cluster"
echo "  - rs_count: current count of replica servers"
echo "  - version: current version of replica servers"
echo "  - lb_op_count: current count of load balance operations to make cluster balanced"
echo "  - app_count: current count of tables in the cluster"
echo "  - storage_gb: current total data size in GB of tables in the cluster"
echo
if [ "$format" == "table" ]; then
  printf '%-30s%-12s%-12s%-12s%-12s%-12s\n' cluster rs_count version lb_op_count app_count storage_gb
elif [ "$format" == "csv" ]; then
  echo "cluster,rs_count,version,lb_op_count,app_count,storage_gb"
else
  echo "ERROR: invalid format: $format"
  exit -1
fi
cluster_count=0
rs_count_sum=0
app_count_sum=0
data_size_sum=0
lb_op_count_sum=0
while read cluster
do
  tmp_file="/tmp/$UID.$PID.pegasus.clusters_status.cluster_info"
  echo "cluster_info" | ./run.sh shell -n $cluster &>$tmp_file
  cluster_info_fail=`grep "\<failed\>" $tmp_file | wc -l`
  if [ $cluster_info_fail -eq 1 ]; then
    echo "ERROR: get cluster info failed, refer error to $tmp_file"
    exit 1
  fi
  lb_op_count=`cat $tmp_file | grep 'balance_operation_count' | grep -o 'total=[0-9]*' | cut -d= -f2`
  if [ -z $lb_op_count ]; then
    lb_op_count="-"
  else
    lb_op_count_sum=$((lb_op_count_sum + lb_op_count))
  fi

  tmp_file="/tmp/$UID.$PID.pegasus.clusters_status.server_info"
  echo "server_info" | ./run.sh shell -n $cluster &>$tmp_file
  rs_count=`cat $tmp_file | grep 'replica-server' | wc -l`
  rs_version=`cat $tmp_file | grep 'replica-server' | grep -o 'Pegasus Server [^ ]*' | head -n 1 | sed 's/SNAPSHOT/SN/' | awk '{print $3}'`

  app_stat_result="/tmp/$UID.$PID.pegasus.clusters_status.app_stat_result"
  tmp_file="/tmp/$UID.$PID.pegasus.clusters_status.app_stat"
  echo "app_stat -o $app_stat_result" | ./run.sh shell -n $cluster &>$tmp_file
  app_stat_fail=`grep "\<failed\>" $tmp_file | wc -l`
  if [ $app_stat_fail -eq 1 ]; then
    sleep 1
    echo "app_stat -o $app_stat_result" | ./run.sh shell -n $cluster &>$tmp_file
    app_stat_fail=`grep "\<failed\>" $tmp_file | wc -l`
    if [ $app_stat_fail -eq 1 ]; then
      echo "ERROR: app stat failed, refer error to $tmp_file"
      exit 1
    fi
  fi
  app_count=`cat $app_stat_result | wc -l`
  app_count=$((app_count-2))
  data_size_column=`cat $app_stat_result | awk '/file_mb/{ for(i = 1; i <= NF; i++) { if ($i == "file_mb") print i; } }'`
  data_size=`cat $app_stat_result | tail -n 1 | awk '{print $'$data_size_column'}' | sed 's/\.00$//'`
  data_size=$(((data_size+1023)/1024))

  if [ "$format" == "table" ]; then
    printf '%-30s%-12s%-12s%-12s%-12s%-12s\n' $cluster $rs_count $rs_version $lb_op_count $app_count $data_size
  elif [ "$format" == "csv" ]; then
    echo -e "$cluster,$rs_count,$rs_version,$lb_op_count,$app_count,$data_size"
  else
    echo "ERROR: invalid format: $format"
    exit -1
  fi

  cluster_count=$((cluster_count + 1))
  rs_count_sum=$((rs_count_sum + rs_count))
  app_count_sum=$((app_count_sum + app_count))
  data_size_sum=$((data_size_sum + data_size))
done <clusters

if [ "$format" == "table" ]; then
  printf '%-30s%-12s%-12s%-12s%-12s%-12s\n' "(total:$cluster_count)" $rs_count_sum "-" $lb_op_count_sum $app_count_sum $data_size_sum
elif [ "$format" == "csv" ]; then
  echo -e "(total:$cluster_count),$rs_count_sum,,$lb_op_count_sum,$app_count_sum,$data_size_sum"
else
  echo "ERROR: invalid format: $format"
  exit -1
fi

rm -rf /tmp/$UID.$PID.pegasus.* &>/dev/null

