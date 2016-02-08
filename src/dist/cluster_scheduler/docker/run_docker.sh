#!/bin/bash

set -e

cd $DSN_ROOT/..

case $1 in
    deploy_and_start)
        shift
        ./run.sh deploy $*
        ./run.sh start $*
        ;;
    stop_and_clean)
        shift
        ./run.sh stop $*
        ./run.sh clean $*
        ;;
    *)
        echo "Error: unknown subcommand"
        exit -1
        ;;
esac
