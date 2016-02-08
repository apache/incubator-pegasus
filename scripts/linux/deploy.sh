#!/bin/bash
os=linux
export scripts_dir=$PWD/scripts/$os

function usage() {
    echo "Option for subcommand 'deploy|start|stop|clean"
    echo " -s|--source-dir <dir>      local source directory for deployment"
    echo " -t|--target <dir>    remote target directory for deployment"
    echo " -d|--deploy-name <name> deployment name to be deployed, like simple_kv"
}

CMD=$1
shift

while [ $# -gt 0 ];do
#TODO: this may cause infinit loop when parameters are invalid
    key=$1
    case $key in
        -h|--help)
            usage
            exit 0
            ;;
        -s|--source-dir)
            s_dir=$2
            shift 2
            ;;
        -t|--target-dir)
            t_dir=$2
            shift 2
            ;;
        -d|--deploy-name)
            d_unit=$2
            shift 2
            ;;
        *)
            echo "ERROR: unknown option $key"
            echo
            usage
            exit -1
            ;;
    esac

done

if [ -z $s_dir ] || [ -z $t_dir ]|| [ -z $d_unit ];then
    usage
    exit -1
fi

if [ ! -d $s_dir ];then
    echo "$s_dir no such directory"
    exit -1
fi

applist=$(cat ${s_dir}/applist)


#$1 machine $2 app
function deploy_files(){

    echo "deploy $s_dir/$2 to $t_dir at $1"
    ssh $1 "mkdir -p ${t_dir}/$2"
    scp ${s_dir}/$2/* ${s_dir}/*list "${1}:${t_dir}/$2"
}


#$1 machine $2 app
function clean_files(){
    echo "cleaning at $1"
    ssh $1 'rm -rf '$t_dir'/'
}

#$1 machine $2 app
function start_server(){
    echo "starting $2 at $1"

    ssh $1 'cd '${t_dir}'/'$2';nohup sh -c "(( ./start.sh >foo.out 2>foo.err </dev/null)&)"'
}

function stop_server(){
    echo "stopping $2 at $1"
#    ssh $1 'pkill '${d_unit}''
    ssh $1 'cd '${t_dir}'/'$2';nohup sh -c "(( ./stop.sh >foo.out 2>foo.err </dev/null)&)"'
}


function run_(){
    for app in $applist;do
        machines=$(cat ${s_dir}/${app}list)
        for mac in $machines;do
            $1 $mac $app
        done
    done
}

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

