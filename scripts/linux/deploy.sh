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


$scripts_dir/deploy.${d_unit}.sh $CMD $s_dir $t_dir 
