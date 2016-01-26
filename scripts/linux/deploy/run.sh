#!/bin/bash

PROGRAM=/home/rdsn/{{ placeholder['deploy_name'] }}
CONFIG=/home/rdsn/config.ini
DATA=/home/rdsn/data

if [ "x$NAME" = "x" ];then
    echo "NAME is empty"
    exit 137
fi

if [ "x$NUM" = "x" ];then
    echo "NUM is empty"
    exit 137
fi

if [ "x$INSTANCE" -ne "x" ];then
    INSTANCE=${INSTANCE^^}
fi


#echo $NAME
META_HOST=${META_HOST:-`printenv ${INSTANCE}_META_1_SERVICE_HOST`}

case $NAME in
    meta)       
        HOST=${HOST:-`printenv ${INSTANCE}_${NAME^^}_${NUM}_SERVICE_HOST`}
        ;;
    replica)
        HOST=${HOST:-`printenv ${INSTANCE}_${NAME^^}_${NUM}_SERVICE_HOST`}
        ;;
    client)
        HOST=${HOST:-"localhost"}
        ;;
    client.perf.test)
        HOST=${HOST:-"localhost"}
        ;;
    *)
        echo "Not supporting type of service"
        exit 137
        ;;
esac
ARGS="-cargs meta-ip=${META_HOST};data-dir=${DATA};explicit-host-address=${HOST} -app_list ${NAME};monitor"

echo ${PROGRAM} ${CONFIG} ${ARGS} > ${DATA}/command.log

${PROGRAM} ${CONFIG} ${ARGS} > ${DATA}/foo.out 2> ${DATA}/foo.err </dev/null


#for easy to debug
if [ $? != 0 ];then
    while true;do
        echo "Welcome to Debug ......"
        sleep 500
    done
fi
