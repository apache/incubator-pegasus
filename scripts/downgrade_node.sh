#!/bin/bash

PID=$$

if [ $# -ne 4 ]
then
  echo "This tool is for downgrading replicas of specified node."
  echo "USAGE: $0 <cluster-meta-list> <node> <app-name> <run|test>"
  echo "  app-name = * means migrate all apps"
  exit 1
fi

pwd="$( cd "$( dirname "$0"  )" && pwd )"
shell_dir="$( cd $pwd/.. && pwd )"
cd $shell_dir

cluster=$1
node=$2
app_name=$3
type=$4

if [ "$type" != "run" -a "$type" != "test" ]
then
  echo "ERROR: invalid type: $type"
  echo "USAGE: $0 <cluster-meta-list> <node> <app-name> <run|test>"
  exit 1
fi

echo "UID=$UID"
echo "PID=$PID"
echo

echo "set_meta_level steady" | ./run.sh shell --cluster $cluster &>/tmp/$UID.$PID.pegasus.set_meta_level

echo ls | ./run.sh shell --cluster $cluster &>/tmp/$UID.$PID.pegasus.ls

while read app_line
do
  status=`echo $app_line | awk '{print $2}'`
  if [ "$status" = "AVAILABLE" ]
  then
    gid=`echo $app_line | awk '{print $1}'`
    app=`echo $app_line | awk '{print $3}'`
    if [ "$app_name" != "*" -a "$app_name" != "$app" ]
    then
      continue
    fi

    echo "app $app -d" | ./run.sh shell --cluster $cluster &>/tmp/$UID.$PID.pegasus.app.$app

    while read line
    do
      sec=`echo $line | awk '{print $5}' | grep -o '\[.*\]' | grep -o '[0-9.:,]*'`
      if echo $sec | grep -q "$node"
      then
        pid=`echo $line | awk '{print $1}'`
        pri=`echo $line | awk '{print $4}'`
        if [ "$pri" = "" ]
        then
          echo "ERROR: can't downgrade ${gid}.${pid} because it is unhealthy"
          exit 1
        fi
        if [ "$pri" = "$node" ]
        then
          echo "ERROR: can't downgrade ${gid}.${pid} because $node is primary"
          exit 1
        fi
        if echo $sec | grep -v -q ','
        then
          echo "ERROR: can't downgrade ${gid}.${pid} because it is unhealthy"
          exit 1
        fi
        echo "propose --gpid ${gid}.${pid} --type DOWNGRADE_TO_INACTIVE -t $pri -n $node"
      fi
    done </tmp/$UID.$PID.pegasus.app.$app >/tmp/$UID.$PID.pegasus.cmd.$app

    if [ "$type" = "run" ]
    then
      cat /tmp/$UID.$PID.pegasus.cmd.$app
      cat /tmp/$UID.$PID.pegasus.cmd.$app | ./run.sh shell --cluster $cluster 2>/dev/null
      echo
      echo
    else
      cat /tmp/$UID.$PID.pegasus.cmd.$app
    fi
  fi
done </tmp/$UID.$PID.pegasus.ls

rm -f /tmp/$UID.$PID.pegasus.* &>/dev/null
