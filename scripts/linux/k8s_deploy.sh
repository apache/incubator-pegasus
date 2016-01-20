#!/bin/bash
os=linux
export scripts_dir=$PWD/scripts/$os

function usage() {
    echo "Option for subcommand 'k8s_deploy|k8s_undeploy"
    echo "-s|--source-dir <dir>      local source directory for deployment"
    echo 
}
CMD=$1
shift

kubectl 2>&1 > /dev/null

if [ "$?" -ne "0" ];then
    echo "kubectl is not in PATH"
    exit -1
fi

kubectl cluster-info

if [ "$?" -ne "0" ];then
    echo "k8s cluster is not on"
    exit -1
fi



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
        *)
            echo "ERROR: unknown option $key"
            echo
            usage
            exit -1
            ;;
    esac
done

if [ -z $s_dir ];then
    usage
    exit -1
fi

function deployment(){
    echo "deployment svc onto k8s"
    cd $s_dir
    svc_files=`./gensvcyaml.sh`
    for svc_file in $svc_files;do
        kubectl create -f $svc_file
    done
    echo "deployment rc onto k8s"
    rc_files=`./genrcyaml.sh`
    for rc_file in $rc_files;do
        kubectl create -f $rc_file
    done
}

function undeployment(){
    echo "undeployment rc and svc"
    cd $s_dir
    d_unit=$(cat d_unit)
    kubectl delete rc  -l d_unit=${d_unit}
    kubectl delete svc -l d_unit=${d_unit}
}



case $CMD in
    k8s_deploy)
        deployment
        ;;
    k8s_undeploy)
        undeployment
        ;;
    *)
        echo "Bug: shouldn't come here"
        echo
        ;;
esac
