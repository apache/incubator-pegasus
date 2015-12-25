#!/bin/bash



PREFIX=$(readlink -m $(dirname ${BASH_SOURCE}))
export LD_LIBRARY_PATH=${PREFIX}
CFG_TOOL="${PREFIX}/configtool"
META_HOST=${META_HOST:-"$(cat ${PREFIX}/metalist)"}
META_HOST=${META_HOST#*@}
NAME=${NAME:-"${PREFIX##*/}"}
HOST=${HOST:-`ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{print $1}'`}
PORT=`${CFG_TOOL} config.ini apps.${NAME} ports`
NUM=${NUM:-1}

docker run -d -p ${PORT}:${PORT}/udp -p ${PORT}:${PORT} -p 8088:8080 -e NAME=${NAME} -e META_HOST=${META_HOST} -e HOST=${HOST} -e NUM=${NUM} {{ placeholder['image_name'] }}

#PROGRAM=${PREFIX}/{{ placeholder['deploy_name'] }}
#CONFIG=${PREFIX}/config.ini
#ARGS="-cargs meta-ip=${META_IP};data-dir=${PREFIX} -app ${APP}"

#${PROGRAM} ${CONFIG} ${ARGS} &>${PREFIX}/${APP}.out


