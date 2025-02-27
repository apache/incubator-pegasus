#!/bin/bash

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

function GenThriftTool() {
    set -e
    wget --progress=dot:giga https://archive.apache.org/dist/thrift/0.11.0/thrift-0.11.0.tar.gz -O thrift-0.11.0.tar.gz
    tar xzf thrift-0.11.0.tar.gz
    pushd thrift-0.11.0
    ./bootstrap.sh
    ./configure --enable-libs=no
    make -j$(($(nproc)/2+1))
    make install
    popd
    rm -rf thrift-0.11.0 thrift-0.11.0.tar.gz
    set +e
}

root=`dirname $0`
root=`cd $root; pwd`

thrift=thrift
which thrift
if [ $? -ne 0 ]; then
    echo "ERROR: not found thrift, try to get it"
    GenThriftTool
fi

if ! $thrift -version | grep "0.11.0" ; then
    echo "ERROR: thrift version should be 0.11.0, please manual fix it"
    exit 1
fi

echo "done"
