#!/bin/bash

CMD=$1
s_dir=$2
t_dir=$3

applist="meta replica client client.perf.test"

metaip=${metaip:-"127.0.0.1"}

#m_files="metalist replicalist clientlist client.perf.testlist"

#$1 machine $2 app
function deploy_files(){

    echo "deploy $s_dir/$2 to $t_dir at $1"
    ssh $1 "mkdir -p ${t_dir}/$2"
    scp $s_dir/$2/* "${1}:${t_dir}/$2"
}


#$1 machine $2 app
function clean_files(){
    echo "cleaning at $1"
    ssh $1 'rm -rf '$t_dir'/'
}

#$1 machine $2 app
function start_server(){
    echo "starting $2 at $1"

    ssh $1 'cd '${t_dir}'/'$2';nohup sh -c "(( APP='$2' META_IP='${metaip}' ./start.sh >foo.out 2>foo.err </dev/null)&)"'
}

function stop_server(){
    echo "stopping $2 at $1"
    ssh $1 'pkill simple_kv'
}


function run_(){
    for app in $applist;do
        machines=$(cat ${s_dir}/${app}list)
        for mac in $machines;do
            $1 $mac $app
        done
    done
}


if [ -f ${s_dir}/metalist ] && [ -f ${s_dir}/replicalist ] && [ -f ${s_dir}/clientlist ] && [ -f ${s_dir}/perflist ];then
    echo "machine lists not exist"
    echo "please create file named metalist replicalist clientlist client.perf.testlist"
    exit -1
fi

metaip=$(cat ${s_dir}/metalist)
metaip=${metaip#*@}

case $CMD in
    start)
        run_ "start_server"
        ;;
    stop)
        run_ "stop_server"
        ;;
    deploy)
        run_ "deploy_files"
        ;;
    clean)
        run_ "clean_files"
        ;;
    *)
        echo "Bug shouldn't come here"
        echo
        ;;
esac











