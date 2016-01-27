#!/bin/bash
exe=dsn.replication.simple_kv
cfg=vconfig.ini
test_dir=test
test_count=5

function count_core_file()
{
    local target_dir=$test_dir/test-$1
    if [ -d $target_dir ]; then
        echo `ls $target_dir | grep core | wc -l`
    else
        echo "0"
    fi
}

function core_file_detect()
{
    for ((i=1; i<=$test_count; ++i)); do
        if [ `count_core_file $i` -ne "0" ]; then
            echo $i
            return
        fi
    done
    echo 0
}

function run_test()
{
    mkdir $test_dir    
    replica_port=34801
    meta_port=34601
    
    cd $test_dir
    for ((i=1; i<=$test_count; ++i)); do
        echo start test instance $i
        mkdir test-$i
        cp ../$exe ../$cfg test-$i
        cd test-$i
        replica_port=$[ $replica_port+3 ]
            meta_port=$[ $meta_port+3 ]
        command="./dsn.replication.simple_kv vconfig.ini -cargs meta_port=$meta_port,replica_port=$replica_port"
        echo $command
        ulimit -c unlimited
        $command > output.log 2>&1 &
        cd ..
    done
    cd ..
}

function stop_all()
{
    ps aux | grep $exe | grep $cfg | awk '{print $2}' | xargs kill -9
}

function check_core()
{
    local check_result
    while [ 0 -lt 1 ]; do
        check_result=`core_file_detect`
        if [ $check_result -ne "0" ]; then
            echo "core file detect in $check_result"
        fi
        sleep 5
    done
}

case $1 in 
    start)
        run_test ;;
    stop)
        stop_all ;;
    check)
        check_core ;;
esac
