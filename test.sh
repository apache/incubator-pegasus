#!/usr/bin/env bash

set -e

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
PROJECT_DIR=$(dirname "${SCRIPT_DIR}")
cd "${PROJECT_DIR}" || exit 1

PEGASUS_PKG="pegasus-tools-2.0.0-5d969e8-glibc2.12-release"
PEGASUS_PKG_URL="https://github.com/XiaoMi/pegasus/releases/download/v2.0.0/pegasus-tools-2.0.0-5d969e8-glibc2.12-release.tar.gz"

# start pegasus onebox environment
if [ ! -f $PEGASUS_PKG.tar.gz ]; then
    wget --quiet $PEGASUS_PKG_URL
    tar xf $PEGASUS_PKG.tar.gz
fi
cd $PEGASUS_PKG

./run.sh start_onebox -m 2 -r 3 -w
cd ../

GO111MODULE=on make build
./bin/echo > /dev/null 2>&1 & # run echoserver in the background, drop its stderr/stdout

if ! GO111MODULE=on time make ci
then
    cd $PEGASUS_PKG
    ./run.sh list_onebox
    exit 1
fi
