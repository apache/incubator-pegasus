exe=dsn.replication.simple_kv
cfg=config-zk.ini

function run_app()
{
    app_dir=$1$2
    if [ ! -d $app_dir ]; then
        mkdir $app_dir
    fi
    cp $exe $cfg $app_dir
    cd $app_dir
    ./$exe $cfg -app_list $1@$2 > output.log 2>&1 &
    cd ..
}

function kill_app()
{
    ps aux | grep $exe | grep $cfg | grep $1@$2 | awk '{print $2}' | xargs kill -9
}

function start_all_apps()
{
    if [ ! -d run ]; then
        mkdir run
    fi
    cp $exe $cfg run
    cd run

    for i in 1 2 3; do
        run_app meta $i
        run_app replica $i
    done

    cd ..
}

function stop_all_apps()
{
    ps aux | grep $exe | grep $cfg | awk '{print $2}' | xargs kill -9
}

case $1 in
    start)
        start_all_apps ;;
    stop)
        stop_all_apps ;;
    clear)
        rm -rf run ;;
esac
