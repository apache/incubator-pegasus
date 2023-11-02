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

# check format
sbt scalafmtSbtCheck scalafmtCheck test:scalafmtCheck

# install java-client dependency
git clone https://github.com/apache/incubator-pegasus.git
cd incubator-pegasus/java-client
git checkout master
cd scripts
echo "run recompile_thrift.sh"
./recompile_thrift.sh
cd ..
mvn clean package -DskipTests -Dcheckstyle.skip=true
mvn clean install -DskipTests -Dcheckstyle.skip=true
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
