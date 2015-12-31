#!/bin/bash
os=linux
export scripts_dir=$PWD/scripts/$os

function usage(){
    echo "Options for subcommand 'publish|publish_docker|republish|republish_docker'"
    echo " -b, --build-dir <dir> "
    echo "                      rdsn build directory, by default is 'builder'"
    echo " -d, --deploy-name <name>"
    echo "                      deployment name to be deployed, like simple_kv"
    echo " -t, --target-dir <dir>"
    echo "                      target directory as a deployment unit"
}

if [ "x${DSN_ROOT}" == "x" ];then
    echo "DSN_ROOT is not set"
    echo "pleasing run :   run.sh install and source ~/.bashrc"
    exit -1
fi
CMD=$1
shift
while [ $# -gt 0 ]; do
    key=$1
    case $key in
        -h|--help)
            usage
            exit 0
            ;;
        -b|--build-dir)
            b_dir=$2
            shift 2
            ;;
        -d|--deploy-name)
            d_unit=$2
            shift 2
            ;;
        -t|--target-dir)
            t_dir=$2
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

#process default variable

b_dir=${b_dir:-"builder"}


if [  -z $b_dir ] || [ -z $d_unit ] || [ -z $t_dir ];then
    usage
    exit -1
fi

if [ ! -d $b_dir ];then
    echo "$b_dir no such directory"
    exit -1
fi


mkdir -p $t_dir
case $CMD in
    publish)
        ${scripts_dir}/publish.${d_unit}.sh $b_dir $t_dir
        ;;
    republish)
        ${scripts_dir}/publish.${d_unit}.sh $b_dir $t_dir republish
        ;;
    publish_docker)
        ${scripts_dir}/publish_docker.${d_unit}.sh $b_dir $t_dir
        ;;
    republish_docker)
        ${scripts_dir}/publish_docker.${d_unit}.sh $b_dir $t_dir republish
        ;;
    *)
        echo "Error: unknown subcommand"
        exit -1
        ;;
esac
