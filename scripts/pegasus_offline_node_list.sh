#!/bin/bash
#
# Offline replica servers using minos.
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

rs_list_file="/tmp/$UID.$PID.pegasus.rolling_update.rs.list"
echo "Generating $rs_list_file..."
minos_show_replica $cluster $rs_list_file
replica_server_count=`cat $rs_list_file | wc -l`
if [ $replica_server_count -eq 0 ]; then
  echo "ERROR: replica server count is 0 by minos show"
  exit 1
fi


echo "Generating /tmp/$UID.$PID.pegasus.offline_node_list.cluster_info..."
echo cluster_info | ./run.sh shell --cluster $meta_list 2>&1 | sed 's/ *$//' >/tmp/$UID.$PID.pegasus.offline_node_list.cluster_info
cname=`grep zookeeper_root /tmp/$UID.$PID.pegasus.offline_node_list.cluster_info | grep -o '/[^/]*$' | grep -o '[^/]*$'`
if [ "$cname" != "$cluster" ]; then
  echo "ERROR: cluster name and meta list not matched"
  exit 1
fi
pmeta=`grep primary_meta_server /tmp/$UID.$PID.pegasus.offline_node_list.cluster_info | grep -o '[0-9.:]*$'`
if [ "$pmeta" == "" ]; then
  echo "ERROR: extract primary_meta_server by shell failed"
  exit 1
fi

echo "Generating /tmp/$UID.$PID.pegasus.offline_node_list.nodes..."
echo nodes | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.offline_node_list.nodes
rs_port=`grep '^[0-9.]*:' /tmp/$UID.$PID.pegasus.offline_node_list.nodes | head -n 1 | grep -o ':[0-9]*' | grep -o '[0-9]*'`
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

echo "Set lb.assign_secondary_black_list..."
echo "remote_command -l $pmeta meta.lb.assign_secondary_black_list $address_list" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.offline_node_list.assign_secondary_black_list
set_ok=`grep "set ok" /tmp/$UID.$PID.pegasus.offline_node_list.assign_secondary_black_list | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: set lb.assign_secondary_black_list failed, refer to /tmp/$UID.$PID.pegasus.offline_node_list.assign_secondary_black_list"
  exit 1
fi

echo "Set live_percentage to 0...(No need to set it back to default. When this offline task is done, meta-server should be restarted, and live_percentage will be loaded from the config file)"
echo "remote_command -l $pmeta meta.live_percentage 0" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.offline_node.live_percentage
set_ok=`grep OK /tmp/$UID.$PID.pegasus.offline_node.live_percentage | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: set live_percentage to 0 failed"
  exit 1
fi

echo
for id in $id_list
do
  echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
  ./scripts/pegasus_offline_node.sh $cluster $meta_list $id
  if [ $? -ne 0 ]; then
    echo "ERROR: offline replica task $id failed"
    exit 1
  fi
  echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
  echo "sleep for 10 seconds"
  sleep 10
done

echo "Clear lb.assign_secondary_black_list..."
echo "remote_command -l $pmeta meta.lb.assign_secondary_black_list clear" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.offline_node_list.assign_secondary_black_list
set_ok=`grep "clear ok" /tmp/$UID.$PID.pegasus.offline_node_list.assign_secondary_black_list | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: clear lb.assign_secondary_black_list failed, refer to /tmp/$UID.$PID.pegasus.offline_node_list.assign_secondary_black_list"
  exit 1
fi

all_finish_time=$((`date +%s`))
echo "Offline replica server task list done."
echo "Elapsed time is $((all_finish_time - all_start_time)) seconds."

rm -f /tmp/$UID.$PID.pegasus.* &>/dev/null
