#!/bin/bash

REPO=${REPO:-docker.io}
SOURCE_DIR=$(dirname ${BASH_SOURCE})

function prepare_file()
{
    wget https://github.com/mcfatealan/rDSN.Python/tree/master/release/linux/MonitorPack.7z
    tar -cvzf ${SOURCE_DIR}/rdsn-release.tar.gz -C $DSN_ROOT include lib bin
}


prepare_file
docker build -t ${REPO}/rdsn ${SOURCE_DIR}
docker push ${REPO}/rdsn
