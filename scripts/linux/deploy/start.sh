#!/bin/bash


APP=${APP:-"meta"}
META_IP=${META_IP:-"localhost"}


PREFIX=$(dirname ${BASH_SOURCE})
export LD_LIBRARY_PATH=${PREFIX}


PROGRAM=${PREFIX}/simple_kv
CONFIG=${PREFIX}/config.ini
ARGS="-cargs 'meta-ip=${META_IP};data-dir=${PREFIX}' -app ${APP}"

${PROGRAM} ${CONFIG} ${ARGS} &>${PREFIX}/${APP}.out
