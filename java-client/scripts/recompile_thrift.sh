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
    VERSION=0.14.0
    DIR_NAME=thrift-${VERSION}
    TAR_NAME=${DIR_NAME}.tar.gz
    DOWNLOAD_URL=https://archive.apache.org/dist/thrift/0.14.0/${TAR_NAME}
    wget --progress=dot:giga ${DOWNLOAD_URL}
    tar xzf ${TAR_NAME}
    pushd ${DIR_NAME}
    ./bootstrap.sh
    ./configure --enable-libs=no
    make -j$(($(nproc)/2+1))
    make install
    popd
    rm -rf ${DIR_NAME} ${TAR_NAME}
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

TMP_DIR=./gen-java
rm -rf $TMP_DIR

mkdir -p $TMP_DIR
$thrift --gen java ../../idl/backup.thrift
$thrift --gen java ../../idl/bulk_load.thrift
$thrift --gen java ../../idl/dsn.layer2.thrift
$thrift --gen java ../../idl/duplication.thrift
$thrift --gen java ../../idl/metadata.thrift
$thrift --gen java ../../idl/meta_admin.thrift
$thrift --gen java ../../idl/partition_split.thrift
$thrift --gen java ../../idl/rrdb.thrift
$thrift --gen java ../../idl/security.thrift

cp -v -r $TMP_DIR/* ../src/main/java/
rm -rf $TMP_DIR

echo "done"
