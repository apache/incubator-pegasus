#!/usr/bin/env bash
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

set -e

NODEJS_CLIENT_DIR=`pwd`
PEGASUS_PKG="pegasus-tools-2.0.0-5d969e8-glibc2.12-release"
PEGASUS_PKG_URL="https://github.com/apache/incubator-pegasus/releases/download/v2.0.0/pegasus-tools-2.0.0-5d969e8-glibc2.12-release.tar.gz"

# start pegasus onebox environment
if [ ! -f ${PEGASUS_PKG}.tar.gz ]; then
    wget --quiet ${PEGASUS_PKG_URL}
    tar xf ${PEGASUS_PKG}.tar.gz
fi
cd ${PEGASUS_PKG}
./run.sh clear_onebox
./run.sh start_onebox -m 3 -r 3 -w

cd "${NODEJS_CLIENT_DIR}"
npm test
