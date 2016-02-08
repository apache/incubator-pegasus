#!/bin/bash

exe=dsn.replication.simple_kv
cfg=config-zk.ini
start_dir=run
meta_count=1
replica_count=3

function run_app()
{
    cd $start_dir
    local app_dir=$1$2
    if [ ! -d $app_dir ]; then
        mkdir $app_dir
    fi

    cd $app_dir
    if [ ! -f $exe ]; then
        cp ../$exe ../$cfg ./
    fi
    ./$exe $cfg -app_list $1@$2 > output.log 2>&1 &
    cd ../..
}

function kill_app()
{
    ps aux | grep $exe | grep $cfg | grep $1@$2 | awk '{print $2}' | xargs kill -9
}

function start_all_apps()
{
    for ((i=1; i<=$meta_count; ++i)); do
        run_app meta $i
    done
    for ((i=1; i<=$replica_count; ++i)); do
        run_app replica $i
    done
}

function stop_all_apps()
{
    ps aux | grep $exe | grep $cfg | awk '{print $2}' | xargs kill -9
}

function count_core_file()
{
    local target_dir=$start_dir/$1$2
    if [ -d $target_dir ]; then
        echo `ls $target_dir | grep core | wc -l`
    else
        echo "0"
    fi
}

function core_file_detect()
{
    for ((i=1; i<=$meta_count; ++i)); do
        if [ `count_core_file meta $i` -ne "0" ]; then
            echo meta$i
            return
        fi
    done
    for ((i=1; i<=$replica_count; ++i)); do
        if [ `count_core_file replica $i` -ne "0" ]; then
            echo replica$i
            return
        fi
    done
    echo 0
}

function round_kill_meta()
{
    local i=0
    local meta_id
    while [ `core_file_detect` == 0 ]; do
        meta_id=$[ $i+1 ]
        echo "kill meta $meta_id"
        kill_app meta $meta_id
        sleep 30
        echo "start meta $meta_id"
        run_app meta $meta_id
        i=$[ ($i+1)%$meta_count ]
        sleep 60
    done
    echo "core file detected, exit"
}

function random_kill_replica()
{
    local random_replica_id
    local core_result=0

    while [ $core_result == 0 ]; do
        sleep 30
        random_replica_id=$[ $RANDOM%$replica_count+1 ]
        echo "kill replica $reandom_replica_id"
        kill_app replica $random_replica_id
        sleep 10
        echo "restart replica $random_replica_id"
        run_app replica $random_replica_id
        core_result=`core_file_detect`
    done

    echo "core file detect in dir $core_result"
    sleep 2
    stop_all_apps
}

function poll_core()
{
    local core_result=0
    while [ $core_result == 0 ]; do
        sleep 1
        core_result=`core_file_detect`
    done
    echo "core file detect in dir: $core_result"
    sleep 2
    stop_all_apps
}

if [ ! -d $start_dir ]; then
    mkdir $start_dir
fi

if [ ! -f $start_dir/$exe ]; then
    cp $exe $cfg $start_dir
fi

case $1 in
    start)
        start_all_apps ;;
    stop)
        stop_all_apps ;;
    clear)
        rm -rf run ;;
    meta_test)
        round_kill_meta ;;
    replica_test)
        random_kill_replica ;;
    start_svc)
        apps_count=$meta_count
        if [ $2 == "replica" ]; then
            apps_count=$replica_count
        fi

        if [ $# -eq 2 ]; then
            for ((index=1; index<=$apps_count; ++index)); do
                run_app $2 $index
            done
        elif [ $# -eq 3 ]; then
            run_app $2 $3
        fi
        ;;
    check_svc)
        poll_core ;;
    kill_svc)
        if [ $# -eq 2 ]; then
            ps aux | grep $exe | grep $cfg | grep $2 | awk '{print $2}' | xargs kill -9
        elif [ $# -eq 3 ]; then
            kill_app $2 $3
        fi
        ;;
esac
