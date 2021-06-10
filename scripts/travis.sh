#!/usr/bin/env bash

set -e

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
PROJECT_DIR=$(dirname "${SCRIPT_DIR}")
cd "${PROJECT_DIR}" || exit 1

# lint all scripts, abort if there's any warning.
function shellcheck_must_pass()
{
    if [[ $(shellcheck "$1") ]]; then
        echo "shellcheck $1 failed"
        shellcheck "$1"
        exit 1
    fi
}
shellcheck_must_pass ./scripts/travis.sh

# check format
sbt scalafmtSbtCheck scalafmtCheck test:scalafmtCheck

# install java-client dependency
git clone https://github.com/XiaoMi/pegasus-java-client.git
cd pegasus-java-client
git checkout v2.2.0
mvn clean package -DskipTests
mvn clean install -DskipTests
cd ..

# start pegasus onebox environment
PEGASUS_PKG="pegasus-tools-2.0.0-5d969e8-glibc2.12-release"
PEGASUS_PKG_URL="https://github.com/apache/incubator-pegasus/releases/download/v2.0.0/pegasus-tools-2.0.0-5d969e8-glibc2.12-release.tar.gz"

# start pegasus onebox environment
if [ ! -f $PEGASUS_PKG.tar.gz ]; then
    wget $PEGASUS_PKG_URL
    tar xvf $PEGASUS_PKG.tar.gz
fi
cd $PEGASUS_PKG

./run.sh start_onebox -w
cd ../

if ! sbt test
then
     cd $PEGASUS_PKG
    ./run.sh list_onebox
    exit 1
fi
