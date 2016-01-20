#!/bin/bash

REPO=${REPO:-docker.io}
SOURCE_DIR=$(dirname ${BASH_SOURCE})

function prepare_file()
{
#    wget https://github.com/mcfatealan/rDSN.Python/tree/master/release/linux/MonitorPack.7z
    tar -cvzf ${SOURCE_DIR}/MonitorPack.tar.gz -C /home/guoxi/rdsn-python/src apps/rDSN.monitor dev setup.py
    tar -cvzf ${SOURCE_DIR}/rdsn-release.tar.gz -C $DSN_ROOT include lib bin
}


prepare_file
docker build -t ${DOCKER_REPO}/rdsn ${SOURCE_DIR}
docker push ${DOCKER_REPO}/rdsn
