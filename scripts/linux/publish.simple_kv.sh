#!/bin/bash

b_dir=$1
t_dir=$2

applist="meta replica client client.perf.test"
echo $applist > $t_dir/applist
function publish_app(){
    cp $DSN_ROOT/lib/libdsn.core.so $t_dir/$1
    cp $b_dir/bin/dsn.replication.simple_kv/dsn.replication.simple_kv $t_dir/$1/simple_kv
    cp $b_dir/bin/dsn.replication.simple_kv/config.ini $t_dir/$1
    cp $scripts_dir/deploy/start.sh $t_dir/$1
}

for app in $applist; do
    mkdir -p $t_dir/$app
    publish_app $app
done
