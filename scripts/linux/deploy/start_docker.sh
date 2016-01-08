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

m_port=8080

while true;do
    nc -z 127.0.0.1 ${m_port} || break
    ((m_port=m_port+1))
done

if [ -z $PORT ];then
    PORT_ARGS="-p ${m_port}:8080"
else
    PORT_ARGS="-p ${PORT}:${PORT}/udp -p ${PORT}:${PORT} -p ${m_port}:8080"
fi

docker pull {{ placeholder['image_name'] }}
docker run -d ${PORT_ARGS} -e NAME=${NAME} -e META_HOST=${META_HOST} -e HOST=${HOST} -e NUM=${NUM} {{ placeholder['image_name'] }} > ${PREFIX}/containerid
