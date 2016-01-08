#!/bin/bash

b_dir=$1
t_dir=$2
if [ "$#" -eq "3" ];then 
repub="x"
fi
applist="meta replica client client.perf.test"
echo $applist > $t_dir/applist

sed -e "s/{{ placeholder\['deploy_name'\] }}/simple_kv/g" $scripts_dir/deploy/start.sh > $t_dir/start.sh

chmod a+x $t_dir/start.sh

function publish_app(){
    cp $DSN_ROOT/lib/libdsn.core.so $t_dir/$1
    cp $b_dir/bin/dsn.replication.simple_kv/dsn.replication.simple_kv $t_dir/$1/simple_kv
    if [ -z $repub ] || [ ! -f $t_dir/$1/config.ini ];then
        cp $b_dir/bin/dsn.replication.simple_kv/config.ini $t_dir/$1
    fi
    cp $t_dir/start.sh $t_dir/$1
    cp $scripts_dir/deploy/stop.sh $t_dir/$1
}

if [ -z $repub ] || [ ! -f $t_dir/metalist ] || [ ! -f $t_dir/replicalist ] || [ ! -f $t_dir/clientlist ] || [ ! -f $t_dir/client.perf.testlist ];then
    rm -f $t_dir/{meta,replica,client,client.perf.test}list
    touch $t_dir/metalist $t_dir/replicalist $t_dir/clientlist $t_dir/client.perf.testlist
fi

for app in $applist; do
    echo "publish $app in $t_dir"
    mkdir -p $t_dir/$app
    publish_app $app
done
