#!/usr/bin/env bash

set -e

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
PROJECT_DIR=$(dirname "${SCRIPT_DIR}")
cd "${PROJECT_DIR}" || exit -1

# lint all scripts, abort if there's any warning.
function shellcheck_must_pass()
{
    if [[ $(shellcheck "$1") ]]; then
        echo "shellcheck $1 failed"
        shellcheck "$1"
        exit 1
    fi
}
shellcheck_must_pass ./scripts/format-all.sh
shellcheck_must_pass ./scripts/travis.sh

# ensure source files are well formatted
./scripts/format-all.sh
if [[ $(git status -s) ]]; then
    git status -s
    echo "please format the above files before commit"
    exit 1
fi

# start pegasus onebox environment
wget https://github.com/XiaoMi/pegasus/releases/download/v1.11.2/pegasus-tools-1.11.2-a186d38-ubuntu-18.04-release.tar.gz
tar xvf pegasus-tools-1.11.2-a186d38-ubuntu-18.04-release.tar.gz
cd pegasus-tools-1.11.2-a186d38-ubuntu-release

# download zookeeper
# TODO(wutao1): remove this when upgrading the server to latest version
mkdir -p .zk_install && cd .zk_install
wget "https://github.com/xiaomi/pegasus-common/raw/master/zookeeper-3.4.6.tar.gz"
cd ..

./run.sh start_onebox
sleep 4
./run.sh list_onebox
cd ../

if ! mvn clean test
then
    cd pegasus-tools-1.11.2-a186d38-ubuntu-release
    ./run.sh list_onebox
    exit 1
fi
