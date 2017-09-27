#!/bin/bash
#
# Rolling update pegasus cluster using minos.
#

if [ $# -le 3 ]; then
  echo "USAGE: $0 <cluster-name> <cluster-meta-list> <type> <start_task_id>"
  echo
  echo "The type may be 'one' or 'all':"
  echo "  - one: rolling update only one task of replica server."
  echo "  - all: rolling update all replica servers, meta servers and collectors."
  echo
  echo "For example:"
  echo "  $0 onebox 127.0.0.1:34601,127.0.0.1:34602 one 0"
  echo
  exit -1
fi

update_options="--update_package --update_config"

cluster=$1
meta_list=$2
type=$3
start_task_id=$4
if [ "$type" != "one" -a "$type" != "all" ]; then
  echo "ERROR: invalid type, should be one or all"
  exit -1
fi

pwd="$( cd "$( dirname "$0"  )" && pwd )"
shell_dir="$( cd $pwd/.. && pwd )"
minos_config_dir=$(dirname $MINOS_CONFIG_FILE)/xiaomi-config/conf/pegasus
minos_client_dir=/home/work/pegasus/infra/minos/client
cd $shell_dir

minos_config=$minos_config_dir/pegasus-${cluster}.cfg
if [ ! -f $minos_config ]; then
  echo "ERROR: minos config \"$minos_config\" not found"
  exit -1
fi

minos_client=$minos_client_dir/deploy
if [ ! -f $minos_client ]; then
  echo "ERROR: minos client \"$minos_client\" not found"
  exit -1
fi

echo "Start time: `date`"
all_start_time=$((`date +%s`))
echo

echo "Generating /tmp/pegasus.rolling_update.minos.show..."
cd $minos_client_dir
./deploy show pegasus $cluster &>/tmp/pegasus.rolling_update.minos.show

echo "Generating /tmp/pegasus.rolling_update.rs.list..."
grep 'Showing task [0-9][0-9]* of replica' /tmp/pegasus.rolling_update.minos.show | awk '{print $5,$9}' | sed 's/(.*)$//' >/tmp/pegasus.rolling_update.rs.list
replica_server_count=`cat /tmp/pegasus.rolling_update.rs.list | wc -l`
if [ $replica_server_count -eq 0 ]; then
  echo "ERROR: replica server count is 0 by minos show"
  exit -1
fi
cd $shell_dir

echo "Generating /tmp/pegasus.rolling_update.cluster_info..."
echo cluster_info | ./run.sh shell --cluster $meta_list &>/tmp/pegasus.rolling_update.cluster_info
cname=`grep zookeeper_root /tmp/pegasus.rolling_update.cluster_info | grep -o '/[^/]*$' | grep -o '[^/]*$'`
if [ "$cname" != "$cluster" ]; then
  echo "ERROR: cluster name and meta list not matched"
  exit -1
fi

echo "Generating /tmp/pegasus.rolling_update.nodes..."
echo nodes | ./run.sh shell --cluster $meta_list &>/tmp/pegasus.rolling_update.nodes
rs_port=`grep '^[0-9.]*:' /tmp/pegasus.rolling_update.nodes | head -n 1 | grep -o ':[0-9]*' | grep -o '[0-9]*'`
if [ "$rs_port" == "" ]; then
  echo "ERROR: extract replica server port by shell failed"
  exit -1
fi

echo "Set meta level to steady..."
echo "set_meta_level steady" | ./run.sh shell --cluster $meta_list &>/tmp/pegasus.rolling_update.set_meta_level
set_ok=`grep 'control meta level ok' /tmp/pegasus.rolling_update.set_meta_level | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: set meta level to steady failed"
  exit -1
fi

echo
while read line
do
  task_id=`echo $line | awk '{print $1}'`
  if [ $task_id -lt $start_task_id ]; then
    continue
  fi
  start_time=$((`date +%s`))
  node_str=`echo $line | awk '{print $2}'`
  node_ip=`getent hosts $node_str | awk '{print $1}'`
  node_name=`getent hosts $node_str | awk '{print $2}'`
  node=${node_ip}:${rs_port}
  echo "=================================================================="
  echo "=================================================================="
  echo "Rolling update replica server task $task_id of [$node_name] [$node]..."
  echo

  echo "Getting serving replica count..."
  serving_replica_count=`echo 'nodes -d' | ./run.sh shell --cluster $meta_list | grep $node | awk '{print $3}'`
  echo "servicing_replica_count=$serving_replica_count"
  echo

  echo "Migrating primary replicas out of node..."
  ./run.sh migrate_node -c $meta_list -n $node -t run &>/tmp/pegasus.rolling_update.migrate_node
  echo "Wait [$node] to migrate done..."
  while true
  do
    pri_count=`echo 'nodes -d' | ./run.sh shell --cluster $meta_list | grep $node | awk '{print $4}'`
    if [ $pri_count -eq 0 ]; then
      echo "Migrate done."
      break
    else
      sleep 1
    fi
  done 
  echo

  echo "Rolling update by minos..."
  cd $minos_client_dir
  ./deploy rolling_update pegasus $cluster --skip_confirm --time_interval 10 $update_options --job replica --task $task_id
  cd $shell_dir
  echo "Rolling update by minos done."
  echo

  echo "Sleep 20 seconds for server restarting..."
  sleep 20
  echo "Sleep done."
  echo

  echo "Wait [$node] to become alive..."
  while true
  do
    node_status=`echo 'nodes -d' | ./run.sh shell --cluster $meta_list | grep $node | awk '{print $2}'`
    if [ $node_status = "ALIVE" ]; then
      echo "Node becomes alive."
      break
    else
      sleep 1
    fi
  done 
  echo

  echo "Wait cluster to become healthy..."
  while true
  do
    unhealthy_count=`echo "ls -d" | ./run.sh shell --cluster $meta_list | awk 'f{ if($NF<7){f=0} else if($3!=$4){print} } /fully_healthy_num/{f=1}' | wc -l`
    if [ $unhealthy_count -eq 0 ]; then
      echo "Cluster becomes healthy, sleep 10 seconds before stepping next..."
      sleep 10
      break
    else
      sleep 1
    fi
  done 
  echo "Sleep done."
  echo

  finish_time=$((`date +%s`))
  echo "Rolling update replica server task $task_id of [$node_name] [$node] done."
  echo "Elapsed time is $((finish_time - start_time)) seconds."
  echo

  if [ "$type" = "one" ]; then
    echo "Finish time: `date`"
    all_finish_time=$((`date +%s`))
    echo "Rolling update one done, elasped time is $((all_finish_time - all_start_time)) seconds."
    exit 0
  fi
done </tmp/pegasus.rolling_update.rs.list

echo "=================================================================="
echo "=================================================================="
echo "Rolling update meta servers and collectors..."
cd $minos_client_dir
./deploy rolling_update pegasus $cluster --skip_confirm --time_interval 10 $update_options --job meta collector
cd $shell_dir
echo

echo "Finish time: `date`"
all_finish_time=$((`date +%s`))
echo "Rolling update all done, elasped time is $((all_finish_time - all_start_time)) seconds."
