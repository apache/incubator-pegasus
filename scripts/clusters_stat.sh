#!/bin/bash

if [ $# -lt 3 ]; then
  echo "USAGE: $0 <cluster-list-file> <month-list> <result-format>"
  echo
  echo "The result format must be 'table' or 'csv'."
  echo
  echo "For example:"
  echo "  $0 \"clusters.txt\" \"2019-01\" \"table\""
  echo "  $0 \"clusters.txt\" \"2019-01 2019-02\" \"csv\""
  echo
  exit 1
fi

clusters_file=$1
months=$2
format=$3
if [ "$format" != "table" -a "$format" != "csv" ]; then
  echo "ERROR: invalid result format, should be 'table' or 'csv'."
  exit 1
fi

pwd="$( cd "$( dirname "$0"  )" && pwd )"
shell_dir="$( cd $pwd/.. && pwd )"
cd $shell_dir

all_result="/tmp/pegasus.stat_available.all_result"
rm $all_result &>/dev/null
echo "stat_time = `date`"
echo "month_list = $months"
echo
echo "Stat method:"
echo "  - for each cluster, there is a collector which sends get/set requests to detect table every 3 seconds."
echo "  - every minute, the collector will write a record of Send and Succeed count into detect table."
echo "  - to stat cluster availability, we scan all the records for the months from detect table, calculate the"
echo "    total Send count and total Succeed count, and calculate the availability by:"
echo "        Available = TotalSucceedCount / TotalSendCount"
echo
echo "Columns:"
echo "  - cluster: name of the cluster"
echo "  - rs_count: current count of replica servers"
echo "  - version: current version of replica servers"
echo "  - minutes: record count in detect table for the months"
echo "  - available: cluster availability"
echo "  - app_count: current count of tables in the cluster"
echo "  - storage_gb: current total data size in GB of tables in the cluster"
echo
if [ "$format" == "table" ]; then
  printf '%-30s%-12s%-12s%-12s%-12s%-12s%-12s\n' cluster rs_count version minutes available app_count storage_gb
elif [ "$format" == "csv" ]; then
  echo "cluster,rs_count,version,minutes,available,table_count,storage_gb"
else
  echo "ERROR: invalid format: $format"
  exit -1
fi
cluster_count=0
rs_count_sum=0
app_count_sum=0
data_size_sum=0
while read cluster
do
  rs_count=`echo server_info | ./run.sh shell -n $cluster 2>&1 | grep 'replica-server' | wc -l`
  rs_version=`echo server_info | ./run.sh shell -n $cluster 2>&1 | grep 'replica-server' | \
      grep -o 'Pegasus Server [^ ]*' | head -n 1 | sed 's/SNAPSHOT/SN/' | awk '{print $3}'`
  result=`./scripts/pegasus_stat_available.sh $cluster $months`
  minutes=`echo $result | awk '{print $2}'`
  available=`echo $result | awk '{print $3}' | sed 's/data/-/'`
  app_count=`echo $result | awk '{print $4}'`
  data_size=`echo $result | awk '{print $5}'`
  if [ "$available" == "1.000000" ]; then
    available_str="99.9999%"
  elif [ "$available" == "0" ]; then
    available_str="00.0000%"
  else
    available_str="${available:2:2}.${available:4:4}%"
  fi
  if [ "$format" == "table" ]; then
    printf '%-30s%-12s%-12s%-12s%-12s%-12s%-12s\n' $cluster $rs_count $rs_version $minutes $available $app_count $data_size
  elif [ "$format" == "csv" ]; then
    echo -e "$cluster,$rs_count,$rs_version,$minutes,=\"$available_str\",$app_count,$data_size"
  else
    echo "ERROR: invalid format: $format"
    exit -1
  fi
  cluster_count=$((cluster_count + 1))
  rs_count_sum=$((rs_count_sum + rs_count))
  app_count_sum=$((app_count_sum + app_count))
  data_size_sum=$((data_size_sum + data_size))
done <$clusters_file

minutes=`cat $all_result | wc -l`
if [ $minutes -eq 0 ]; then
  available="0.000000"
else
  available=`cat $all_result | grep -o '[0-9]*,[0-9]*,[0-9]*' | awk -F, '{a+=$1;b+=$2}END{printf("%f\n",(double)b/a);}'`
fi

if [ "$available" == "1.000000" ]; then
  available_str="99.9999%"
elif [ "$available" == "0" ]; then
  available_str="00.0000%"
else
  available_str="${available:2:2}.${available:4:4}%"
fi

if [ "$format" == "table" ]; then
  printf '%-30s%-12s%-12s%-12s%-12s%-12s%-12s\n' "(total:$cluster_count)" $rs_count_sum "-" $minutes $available $app_count_sum $data_size_sum
  echo
elif [ "$format" == "csv" ]; then
  echo -e "(total:$cluster_count),$rs_count_sum,,$minutes,=\"$available_str\",$app_count_sum,$data_size_sum"
else
  echo "ERROR: invalid format: $format"
  exit -1
fi

rm $all_result &>/dev/null

