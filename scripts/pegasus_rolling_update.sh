#!/bin/bash
#
# Rolling update pegasus cluster using minos.
#

PID=$$

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
  exit 1
fi

if [ -z ${TMUX} ]; then
  echo "ERROR: This script must be run in a tmux session"
  exit 1
fi

cluster=$1
meta_list=$2
type=$3
start_task_id=$4
if [ "$type" != "one" -a "$type" != "all" ]; then
  echo "ERROR: invalid type, should be one or all"
  exit 1
fi

pwd="$( cd "$( dirname "$0"  )" && pwd )"
shell_dir="$( cd $pwd/.. && pwd )"
cd $shell_dir

source ./scripts/minos_common.sh
find_cluster $cluster
if [ $? -ne 0 ]; then
  echo "ERROR: cluster \"$cluster\" not found"
  exit 1
fi

echo "UID=$UID"
echo "PID=$PID"
echo "Start time: `date`"
all_start_time=$((`date +%s`))
echo

rs_list_file="/tmp/$UID.$PID.pegasus.rolling_update.rs.list"
echo "Generating $rs_list_file..."
minos_show_replica $cluster $rs_list_file
replica_server_count=`cat $rs_list_file | wc -l`
if [ $replica_server_count -eq 0 ]; then
  echo "ERROR: replica server count is 0 by minos show"
  exit 1
fi

echo "Generating /tmp/$UID.$PID.pegasus.rolling_update.cluster_info..."
echo cluster_info | ./run.sh shell --cluster $meta_list 2>&1 | sed 's/ *$//' >/tmp/$UID.$PID.pegasus.rolling_update.cluster_info
cname=`grep zookeeper_root /tmp/$UID.$PID.pegasus.rolling_update.cluster_info | grep -o '/[^/]*$' | grep -o '[^/]*$'`
if [ "$cname" != "$cluster" ]; then
  echo "ERROR: cluster name and meta list not matched"
  exit 1
fi
pmeta=`grep primary_meta_server /tmp/$UID.$PID.pegasus.rolling_update.cluster_info | grep -o '[0-9.:]*$'`
if [ "$pmeta" == "" ]; then
  echo "ERROR: extract primary_meta_server by shell failed"
  exit 1
fi

echo "Generating /tmp/$UID.$PID.pegasus.rolling_update.nodes..."
echo nodes | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rolling_update.nodes
rs_port=`grep '^[0-9.]*:' /tmp/$UID.$PID.pegasus.rolling_update.nodes | head -n 1 | grep -o ':[0-9]*' | grep -o '[0-9]*'`
if [ "$rs_port" == "" ]; then
  echo "ERROR: extract replica server port by shell failed"
  exit 1
fi

echo "Set meta level to steady..."
echo "set_meta_level steady" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rolling_update.set_meta_level
set_ok=`grep 'control meta level ok' /tmp/$UID.$PID.pegasus.rolling_update.set_meta_level | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: set meta level to steady failed"
  exit 1
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

  echo "Set lb.add_secondary_max_count_for_one_node to 0..."
  echo "remote_command -l $pmeta meta.lb.add_secondary_max_count_for_one_node 0" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rolling_update.add_secondary_max_count_for_one_node
  set_ok=`grep OK /tmp/$UID.$PID.pegasus.rolling_update.add_secondary_max_count_for_one_node | wc -l`
  if [ $set_ok -ne 1 ]; then
    echo "ERROR: set lb.add_secondary_max_count_for_one_node to 0 failed"
    exit 1
  fi
  echo

  echo "Migrating primary replicas out of node..."
  sleeped=0
  while true
  do
    if [ $((sleeped%10)) -eq 0 ]; then
      ./run.sh migrate_node -c $meta_list -n $node -t run &>/tmp/$UID.$PID.pegasus.rolling_update.migrate_node
      echo "Send migrate propose, refer to /tmp/$UID.$PID.pegasus.rolling_update.migrate_node for details"
    fi
    pri_count=`echo 'nodes -d' | ./run.sh shell --cluster $meta_list | grep $node | awk '{print $4}'`
    if [ $pri_count -eq 0 ]; then
      echo "Migrate done."
      break
    elif [ $sleeped -gt 28 ]; then
      echo "Downgrade timeout."
      break
    else
      echo "Still $pri_count primary replicas left on $node"
      sleep 1
      sleeped=$((sleeped+1))
    fi
  done
  echo
  sleep 1

  echo "Downgrading replicas on node..."
  sleeped=0
  while true
  do
    if [ $((sleeped%10)) -eq 0 ]; then
      ./run.sh downgrade_node -c $meta_list -n $node -t run &>/tmp/$UID.$PID.pegasus.rolling_update.downgrade_node
      echo "Send downgrade propose, refer to /tmp/$UID.$PID.pegasus.rolling_update.downgrade_node for details"
    fi
    rep_count=`echo 'nodes -d' | ./run.sh shell --cluster $meta_list | grep $node | awk '{print $3}'`
    if [ $rep_count -eq 0 ]; then
      echo "Downgrade done."
      break
    elif [ $sleeped -gt 28 ]; then
      echo "Downgrade timeout."
      break
    else
      echo "Still $rep_count replicas left on $node"
      sleep 1
      sleeped=$((sleeped+1))
    fi
  done
  echo
  sleep 1

  echo "Checking replicas closed on node..."
  sleeped=0
  while true
  do
    if [ $((sleeped%10)) -eq 0 ]; then
      echo "Send kill_partition commands to node..."
      grep '^propose ' /tmp/$UID.$PID.pegasus.rolling_update.downgrade_node >/tmp/$UID.$PID.pegasus.rolling_update.downgrade_node.propose
      while read line2
      do
        gpid=`echo $line2 | awk '{print $3}' | sed 's/\./ /'`
        echo "remote_command -l $node replica.kill_partition $gpid" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rolling_update.kill_partition
      done </tmp/$UID.$PID.pegasus.rolling_update.downgrade_node.propose
      echo "Sent to `cat /tmp/$UID.$PID.pegasus.rolling_update.downgrade_node.propose | wc -l` partitions."
    fi
    echo "remote_command -l $node perf-counters '.*replica(Count)'" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rolling_update.replica_count_perf_counters
    serving_count=`grep -o 'replica_stub.replica(Count)","type":"NUMBER","value":[0-9]*' /tmp/$UID.$PID.pegasus.rolling_update.replica_count_perf_counters | grep -o '[0-9]*$'`
    opening_count=`grep -o 'replica_stub.opening.replica(Count)","type":"NUMBER","value":[0-9]*' /tmp/$UID.$PID.pegasus.rolling_update.replica_count_perf_counters | grep -o '[0-9]*$'`
    closing_count=`grep -o 'replica_stub.closing.replica(Count)","type":"NUMBER","value":[0-9]*' /tmp/$UID.$PID.pegasus.rolling_update.replica_count_perf_counters | grep -o '[0-9]*$'`
    if [ "$serving_count" = "" -o "$opening_count" = "" -o "$closing_count" = "" ]; then
      echo "ERROR: extract replica count from perf counters failed"
      exit 1
    fi
    rep_count=$((serving_count + opening_count + closing_count))
    if [ $rep_count -eq 0 ]; then
      echo "Close done."
      break
    elif [ $sleeped -gt 28 ]; then
      echo "Close timeout."
      break
    else
      echo "Still $rep_count replicas not closed on $node"
      sleep 1
      sleeped=$((sleeped+1))
    fi
  done
  echo
  sleep 1

  echo "remote_command -l $node flush-log" | ./run.sh shell --cluster $meta_list &>/dev/null

  echo "Set lb.add_secondary_max_count_for_one_node to 100..."
  echo "remote_command -l $pmeta meta.lb.add_secondary_max_count_for_one_node 100" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rolling_update.add_secondary_max_count_for_one_node
  set_ok=`grep OK /tmp/$UID.$PID.pegasus.rolling_update.add_secondary_max_count_for_one_node | wc -l`
  if [ $set_ok -ne 1 ]; then
    echo "ERROR: set lb.add_secondary_max_count_for_one_node to 100 failed"
    exit 1
  fi

  echo "Rolling update by minos..."
  minos_rolling_update $cluster replica $task_id
  echo "Rolling update by minos done."
  echo
  sleep 1

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
  sleep 1

  echo "Wait cluster to become healthy..."
  while true
  do
    unhealthy_count=`echo "ls -d" | ./run.sh shell --cluster $meta_list | awk 'f{ if(NF<7){f=0} else if($3!=$4){print} } / fully_healthy /{f=1}' | wc -l`
    if [ $unhealthy_count -eq 0 ]; then
      echo "Cluster becomes healthy."
      break
    else
      sleep 1
    fi
  done
  echo
  sleep 1

  finish_time=$((`date +%s`))
  echo "Rolling update replica server task $task_id of [$node_name] [$node] done."
  echo "Elapsed time is $((finish_time - start_time)) seconds."
  echo

  if [ "$type" = "one" ]; then
    break
  fi
done <$rs_list_file

echo "Set lb.add_secondary_max_count_for_one_node to DEFAULT..."
echo "remote_command -l $pmeta meta.lb.add_secondary_max_count_for_one_node DEFAULT" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rolling_update.add_secondary_max_count_for_one_node
set_ok=`grep OK /tmp/$UID.$PID.pegasus.rolling_update.add_secondary_max_count_for_one_node | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: set lb.add_secondary_max_count_for_one_node to DEFAULT failed"
  exit 1
fi

if [ "$type" = "all" ]; then
  echo "=================================================================="
  echo "=================================================================="
  echo "Rolling update meta servers..."
  minos_rolling_update $cluster meta
  echo "Rolling update meta servers done."
  echo

  echo "Rolling update collectors..."
  minos_rolling_update $cluster collector
  echo "Rolling update collectors done."
  echo

  echo "Set meta level to lively..."
  echo "set_meta_level lively" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rolling_update.set_meta_level
  set_ok=`grep 'control meta level ok' /tmp/$UID.$PID.pegasus.rolling_update.set_meta_level | wc -l`
  if [ $set_ok -ne 1 ]; then
    echo "ERROR: set meta level to lively failed"
    exit 1
  fi
  echo

  echo "Wait cluster to become balanced..."
  echo "Wait for 3 minutes to do load balance..."
  sleep 180
  while true
  do
    op_count=`echo "cluster_info" | ./run.sh shell --cluster $meta_list | grep balance_operation_count | grep -o 'total=[0-9][0-9]*' | cut -d= -f2`
    if [ -z "op_count" ]; then
      break
    fi
    if [ $op_count -eq 0 ]; then
      echo "Cluster becomes balanced."
      break
    else
      echo "Still $op_count balance operations to do..."
      sleep 10
    fi
  done
  echo

  echo "Set meta level to steady..."
  echo "set_meta_level steady" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rolling_update.set_meta_level
  set_ok=`grep 'control meta level ok' /tmp/$UID.$PID.pegasus.rolling_update.set_meta_level | wc -l`
  if [ $set_ok -ne 1 ]; then
    echo "ERROR: set meta level to steady failed"
    exit 1
  fi
  echo
fi

echo "Finish time: `date`"
all_finish_time=$((`date +%s`))
echo "Rolling update $type done, elasped time is $((all_finish_time - all_start_time)) seconds."

rm -f /tmp/$UID.$PID.pegasus.* &>/dev/null
