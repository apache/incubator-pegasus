#!/bin/bash
#
# Add replica servers using minos.
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

echo "Check the argument..."
source ./scripts/pegasus_check_arguments.sh add_node_list $cluster $meta_list $replica_task_id_list

if [ $? -ne 0 ]; then
    echo "ERROR: the argument check failed"
    exit 1
fi

echo "Set meta level to steady..."
echo "set_meta_level steady" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.add_node_list.set_meta_level
set_ok=`grep 'control meta level ok' /tmp/$UID.$PID.pegasus.add_node_list.set_meta_level | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: set meta level to steady failed"
  exit 1
fi

for id in $task_id_list
do
  echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
  minos_bootstrap $cluster replica $id
  if [ $? -ne 0 ]; then
    echo "ERROR: online replica task $id failed"
    exit 1
  fi
  echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
done

echo "Set meta.lb.only_move_primary true"
echo "This remote-command tells the meta-server to ignore copying primaries during rebalancing."
echo "So the following steps only include move_primary and copy_secondary."
echo "remote_command -l $pmeta meta.lb.only_move_primary true" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.add_node_list.only_move_primary
set_ok=`grep OK /tmp/$UID.$PID.pegasus.add_node_list.only_move_primary | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: meta.lb.only_move_primary true"
  exit 1
fi
echo

echo "Set meta level to lively..."
echo "set_meta_level lively" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.add_node_list.set_meta_level
set_ok=`grep 'control meta level ok' /tmp/$UID.$PID.pegasus.add_node_list.set_meta_level | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: set meta level to lively failed"
  exit 1
fi

echo "Wait cluster to become balanced..."
echo "Wait for 3 minutes to do load balance..."
sleep 180
while true; do
    op_count=$(echo "cluster_info" | ./run.sh shell --cluster $meta_list | grep balance_operation_count | grep -o 'total=[0-9][0-9]*' | cut -d= -f2)
    if [ -z "op_count" ]; then
        break
    fi
    if [ $op_count -eq 0 ]; then
        echo "Cluster may be balanced, try wait 30 seconds..."
        sleep 30
        op_count=$(echo "cluster_info" | ./run.sh shell --cluster $meta_list | grep balance_operation_count | grep -o 'total=[0-9][0-9]*' | cut -d= -f2)
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
echo "set_meta_level steady" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.add_node_list.set_meta_level
set_ok=`grep 'control meta level ok' /tmp/$UID.$PID.pegasus.add_node_list.set_meta_level | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: set meta level to steady failed"
  exit 1
fi

echo "Set meta.lb.only_move_primary false"
echo "This remote-command tells the meta-server to rebalance with copying primaries."
echo "remote_command -l $pmeta meta.lb.only_move_primary false" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.add_node_list.only_move_primary
set_ok=`grep OK /tmp/$UID.$PID.pegasus.add_node_list.only_move_primary | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: meta.lb.only_move_primary false"
  exit 1
fi
echo

echo "Finish time: `date`"
all_finish_time=$((`date +%s`))
echo "add node list done, elasped time is $((all_finish_time - all_start_time)) seconds."

rm -f /tmp/$UID.$PID.pegasus.* &>/dev/null
