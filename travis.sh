#!/usr/bin/env bash

set -e

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
PROJECT_DIR=$(dirname "${SCRIPT_DIR}")
cd "${PROJECT_DIR}" || exit 1

PEGASUS_PKG="pegasus-tools-1.11.6-9f4e5ae-glibc2.12-release"
PEGASUS_PKG_URL="https://github.com/XiaoMi/pegasus/releases/download/v1.11.6/pegasus-tools-1.11.6-9f4e5ae-glibc2.12-release.tar.gz"

# start pegasus onebox environment
if [ ! -f $PEGASUS_PKG.tar.gz ]; then
    wget $PEGASUS_PKG_URL
    tar xvf $PEGASUS_PKG.tar.gz
fi
cd $PEGASUS_PKG

sed -i "s#https://github.com/xiaomi/pegasus-common/raw/master/zookeeper-3.4.6.tar.gz#https://github.com/XiaoMi/pegasus-common/releases/download/deps/zookeeper-3.4.6.tar.gz#" scripts/start_zk.sh
./run.sh start_onebox -w
cd ../

GO111MODULE=on make build
./bin/echo & # run echoserver in the background

if ! GO111MODULE=on time make ci
then
    cd $PEGASUS_PKG
    ./run.sh list_onebox
    exit 1
fi
