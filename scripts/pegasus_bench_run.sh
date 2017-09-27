#!/bin/bash
#
# test pegasus_bench with thread 1~50

pwd="$( cd "$( dirname "$0"  )" && pwd )"
shell_dir="$( cd $pwd/.. && pwd )"
cd $shell_dir

TYPE=fillseq_pegasus # fillseq_pegasus or readrandom_pegasus
CLUSTER=127.0.0.1:34601,127.0.0.1:34602,127.0.0.1:34603
TABLE=temp

outdir=pegasus_bench_result
rm -rf $outdir
mkdir -p $outdir
echo "Thread	Count	Runtime	QPS	AvgLat	P99Lat"
for T in {1..50}
do
  N=10000
  outfile=$outdir/fill_t${T}.out
  ./run.sh bench -t $TYPE -n $N --cluster $CLUSTER --app_name $TABLE --thread_num $T --value_size 1000 &>$outfile
  Count=$((N*T))
  Runtime=`cat $outfile | grep 'ops/second in ([0-9,.]*) seconds' | tail -n 1 | grep -o ',[0-9.]*) seconds' | grep -o '[0-9.]\{6\}'`
  Throughput=`cat $outfile | grep $TYPE | grep -o '[0-9]* ops' | awk '{print $1}'`
  AvgLatency=`cat $outfile | grep 'Average:' | awk '{print $4}' | cut -d. -f1`
  P99Latency=`cat $outfile | grep 'Percentiles:' | grep -o 'P99: [0-9]*' | awk '{print $2}'`
  echo "$T	$Count	$Runtime	$Throughput	$AvgLatency	$P99Latency"
done
