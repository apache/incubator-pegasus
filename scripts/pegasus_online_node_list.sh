#!/bin/bash
#
# Online replica servers using minos.
#

PID=$$

if [ $# -le 2 ]; then
  echo "USAGE: $0 <cluster-name> <cluster-meta-list> <replica-task-id-list>"
  echo
  echo "For example:"
  echo "  $0 onebox 127.0.0.1:34601,127.0.0.1:34602 1,2,3"
  echo
  exit 1
fi

cluster=$1
meta_list=$2
replica_task_id_list=$3

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

online_list_file="/tmp/$UID.$PID.pegasus.online_node.list"
echo "Generating $online_list_file..."
minos_show_replica $cluster $online_list_file
replica_server_count=`cat $online_list_file= | wc -l`
if [ $replica_server_count -eq 0 ]; then
  echo "ERROR: replica server count is 0 by minos show"
  exit 1
fi


echo "Generating /tmp/$UID.$PID.pegasus.online_node_list.cluster_info..."
echo cluster_info | ./run.sh shell --cluster $meta_list 2>&1 | sed 's/ *$//' >/tmp/$UID.$PID.pegasus.online_node_list.cluster_info
cname=`grep zookeeper_root /tmp/$UID.$PID.pegasus.online_node_list.cluster_info | grep -o '/[^/]*$' | grep -o '[^/]*$'`
if [ "$cname" != "$cluster" ]; then
  echo "ERROR: cluster name and meta list not matched"
  exit 1
fi
pmeta=`grep primary_meta_server /tmp/$UID.$PID.pegasus.online_node_list.cluster_info | grep -o '[0-9.:]*$'`
if [ "$pmeta" == "" ]; then
  echo "ERROR: extract primary_meta_server by shell failed"
  exit 1
fi

echo "Generating /tmp/$UID.$PID.pegasus.online_node_list.nodes..."
echo nodes | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.online_node_list.nodes
rs_port=`grep '^[0-9.]*:' /tmp/$UID.$PID.pegasus.online_node_list.nodes | head -n 1 | grep -o ':[0-9]*' | grep -o '[0-9]*'`
if [ "$rs_port" == "" ]; then
  echo "ERROR: extract replica server port by shell failed"
  exit 1
fi

echo "Checking replica task id list..."
address_list=""
id_list=""
for id in `echo $replica_task_id_list | sed 's/,/ /g'` ; do
  if [ "$id_list" != "" ]; then
    if echo "$id_list" | grep -q "\<$id\>" ; then
      echo "ERROR: duplicate replica task id $id"
      exit 1;
    fi
  fi
  pair=`grep "^$id " $rs_list_file`
  if [ "$pair" == "" ]; then
    echo "ERROR: replica task id $id not found, refer to $rs_list_file"
    exit 1;
  fi
  node_str=`echo $pair | awk '{print $2}'`
  node_ip=`getent hosts $node_str | awk '{print $1}'`
  node=${node_ip}:${rs_port}
  if [ "$id_list" != "" ]; then
    id_list="$id_list $id"
    address_list="$address_list,$node"
  else
    id_list="$id"
    address_list="$node"
  fi
done

echo "Set meta level to steady..."
echo "set_meta_level steady" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.online_node.set_meta_level
set_ok=`grep 'control meta level ok' /tmp/$UID.$PID.pegasus.online_node.set_meta_level | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: set meta level to steady failed"
  exit 1
fi


echo
for id in $id_list
do
  echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
  minos_bootstrap $cluster replica $id
  if [ $? -ne 0 ]; then
    echo "ERROR: online replica task $id failed"
    exit 1
  fi
  echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
  echo "sleep for 10 seconds"
  sleep 10
done

echo "set meta.lb.only_move_primary true"
echo "remote_command -l $pmeta meta.lb.only_move_primary true" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.online_node.only_move_primary
set_ok=`grep OK /tmp/$UID.$PID.pegasus.online_node.only_move_primary | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: meta.lb.only_move_primary true"
  exit 1
fi
echo

echo "Set meta level to lively..."
echo "set_meta_level lively" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.online_node.set_meta_level
set_ok=`grep 'control meta level ok' /tmp/$UID.$PID.pegasus.online_node.set_meta_level | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: set meta level to steady failed"
  exit 1
fi

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
      echo "Cluster may be balanced, try wait 10 seconds..."
      sleep 10
      op_count=`echo "cluster_info" | ./run.sh shell --cluster $meta_list | grep balance_operation_count | grep -o 'total=[0-9][0-9]*' | cut -d= -f2`
      if [ $op_count -eq 0 ]; then
        echo "Cluster becomes balanced."
        break
      fi
    else
      echo "Still $op_count balance operations to do..."
      sleep 1
    fi
  done
echo



echo "Set meta level to steady..."
echo "set_meta_level steady" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.online_node.set_meta_level
set_ok=`grep 'control meta level ok' /tmp/$UID.$PID.pegasus.online_node.set_meta_level | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: set meta level to steady failed"
  exit 1
fi

echo "set meta.lb.only_move_primary false"
echo "remote_command -l $pmeta meta.lb.only_move_primary false" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.online_node.only_move_primary
set_ok=`grep OK /tmp/$UID.$PID.pegasus.online_node.only_move_primary | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: meta.lb.only_move_primary false"
  exit 1
fi
echo

echo "Finish time: `date`"
all_finish_time=$((`date +%s`))
echo "online node $type done, elasped time is $((all_finish_time - all_start_time)) seconds."

rm -f /tmp/$UID.$PID.pegasus.* &>/dev/null
