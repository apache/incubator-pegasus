#!/bin/bash
#
# Pegasus cluster rebalance 
#

PID=$$

if [ $# -le 2 ]; then
  echo "USAGE: $0 <cluster-name> <cluster-meta-list> <only_move_primary>"
  echo
  echo "for example:"
  echo "  $0 onebox 127.0.0.1:34601,127.0.0.1:34602 true"
  echo
  exit 1
fi

cluster=$1
meta_list=$2
only_move_primary=$3

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
rebalance_start_time=$((`date +%s`))
echo

echo "Generating /tmp/$UID.$PID.pegasus.rebalance.cluster_info..."
echo cluster_info | ./run.sh shell --cluster $meta_list 2>&1 | sed 's/ *$//' >/tmp/$UID.$PID.pegasus.rebalance.cluster_info
cname=`grep zookeeper_root /tmp/$UID.$PID.pegasus.rebalance.cluster_info | grep -o '/[^/]*$' | grep -o '[^/]*$'`
if [ "$cname" != "$cluster" ]; then
  echo "ERROR: cluster name and meta list not matched"
  exit 1
fi
pmeta=`grep primary_meta_server /tmp/$UID.$PID.pegasus.rebalance.cluster_info | grep -o '[0-9.:]*$'`
if [ "$pmeta" == "" ]; then
  echo "ERROR: extract primary_meta_server by shell failed"
  exit 1
fi

if [ "$only_move_primary" == "true" ]; then
  echo "Set meta.lb.only_move_primary true"
  echo "This remote-command tells the meta-server to ignore copying primaries during rebalancing."
  echo "So the following steps only include move_primary and copy_secondary."
  echo "remote_command -l $pmeta meta.lb.only_move_primary true" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rebalance.only_move_primary
  set_ok=`grep OK /tmp/$UID.$PID.pegasus.rebalance.only_move_primary | wc -l`
  if [ $set_ok -ne 1 ]; then
    echo "ERROR: meta.lb.only_move_primary true"
    exit 1
  fi
fi
echo

echo "Set meta level to lively..."
echo "set_meta_level lively" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rebalance.set_meta_level
set_ok=`grep 'control meta level ok' /tmp/$UID.$PID.pegasus.rebalance.set_meta_level | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: set meta level to lively failed"
  exit 1
fi

echo "Wait cluster to become balanced..."
echo "Wait for 3 minutes to do load balance..."
sleep 180
op_count_check_time=1
while true; do
    op_count=$(echo "cluster_info" | ./run.sh shell --cluster $meta_list | grep balance_operation_count | grep -o 'total=[0-9][0-9]*' | cut -d= -f2)
    if [ -z "op_count" ]; then
        break
    fi

    if [ $op_count -eq 0 && op_count_check_time -eq 0 ]; then
        break
    fi

    if [ $op_count -eq 0 && op_count_check_time -gt 0 ]; then
        echo "Cluster may be balanced, try wait 30 seconds..."
        $op_count_check_time--
        sleep 30
    else
        echo "Still $op_count balance operations to do..."
        sleep 10
    fi
done
echo

echo "Set meta level to steady..."
echo "set_meta_level steady" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rebalance.set_meta_level
set_ok=`grep 'control meta level ok' /tmp/$UID.$PID.pegasus.rebalance.set_meta_level | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: set meta level to steady failed"
  exit 1
fi

if [ "$only_move_primary" == "true"]; then
  echo "Set meta.lb.only_move_primary false"
  echo "This remote-command tells the meta-server to rebalance with copying primaries."
  echo "remote_command -l $pmeta meta.lb.only_move_primary false" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.rebalance.only_move_primary
  set_ok=`grep OK /tmp/$UID.$PID.pegasus.rebalance.only_move_primary | wc -l`
  if [ $set_ok -ne 1 ]; then
    echo "ERROR: meta.lb.only_move_primary false"
    exit 1
  fi
  echo
fi

echo "Finish time: `date`"
rebalance_finish_time=$((`date +%s`))
echo "rebalance done, elasped time is $((rebalance_finish_time - rebalance_start_time)) seconds."

rm -f /tmp/$UID.$PID.pegasus.* &>/dev/null
