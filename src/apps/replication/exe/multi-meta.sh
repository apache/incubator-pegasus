#!/bin/bash

exe=dsn.replication.simple_kv
cfg=config-zk.ini
start_dir=run

function run_app()
{
    cd $start_dir
    local app_dir=$1$2
    if [ ! -d $app_dir ]; then
        mkdir $app_dir
    fi
    cp $exe $cfg $app_dir
    cd $app_dir
    ./$exe $cfg -app_list $1@$2 > output.log 2>&1 &
    cd ../..
}

function kill_app()
{
    ps aux | grep $exe | grep $cfg | grep $1@$2 | awk '{print $2}' | xargs kill -9
}

function start_all_apps()
{
    if [ ! -d $start_dir ]; then
        mkdir $start_dir
    fi
    cp $exe $cfg $start_dir

    for i in 1 2 3; do
        run_app meta $i
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
    for i in 1 2 3; do
        if [ `count_core_file meta $i` -ne "0" ]; then
            echo 1
            return
        fi
        if [ `count_core_file replica $i` -ne "0" ]; then
            echo 1
            return
        fi
    done
    echo 0
}

function round_kill_meta()
{
    local i=0
    local meta_id
    while [ `core_file_detect` -eq 0 ]; do
        meta_id=$[ $i+1 ]
        echo "kill meta $meta_id"
        kill_app meta $meta_id
        echo "start meta $meta_id"
        run_app meta $meta_id
        i=$[ ($i+1)%3 ]
        sleep 60
    done
    echo "core file detected, stop meta"
}

case $1 in
    start)
        start_all_apps ;;
    stop)
        stop_all_apps ;;
    clear)
        rm -rf run ;;
    meta_test)
        round_kill_meta ;;
esac
