#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

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
shellcheck_must_pass ./scripts/ci-test.sh

# The new version of pegasus client is not compatible with old version server which contains old rpc protocol,
# So we use snapshot version of pegasus-tools, because we don`t have a new release version, which contains the new version of rpc protocol,
# TODO: This is a temp url. We will change it later.
PEGASUS_PKG="pegasus-tools-2.1.SNAPSHOT-dc4c710-glibc2.17-release"
PEGASUS_PKG_URL="https://github.com/levy5307/pegasus-tools/raw/master/pegasus-tools-2.1.SNAPSHOT-dc4c710-glibc2.17-release.tar.gz"

# check format
if ! mvn spotless:check
then
    exit 1
fi

# start pegasus onebox environment
if [ ! -f $PEGASUS_PKG.tar.gz ]; then
    wget $PEGASUS_PKG_URL
    tar xvf $PEGASUS_PKG.tar.gz
fi
cd $PEGASUS_PKG

./run.sh start_onebox -w
cd ../


pushd scripts
echo "bash recompile_thrift.sh"
bash recompile_thrift.sh
popd
mvn spotless:apply

if ! mvn clean test
then
    cd $PEGASUS_PKG
    ./run.sh list_onebox
    exit 1
fi
