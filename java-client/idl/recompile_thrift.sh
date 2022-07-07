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
    wget --progress=dot:giga https://github.com/apache/thrift/archive/refs/tags/0.11.0.tar.gz -O thrift-0.11.0.tar.gz
    tar xzf thrift-0.11.0.tar.gz
    pushd thrift-0.11.0
    ./bootstrap.sh
    ./configure --with-csharp=no --with-java=no --with-python=no --with-erlang=no --with-perl=no \
        --with-php=no --with-ruby=no --with-haskell=no --with-php_extension=no --with-rs=no --with-go=no
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

TMP_DIR=./gen-java
rm -rf $TMP_DIR

mkdir -p $TMP_DIR
$thrift --gen java rrdb.thrift
$thrift --gen java replication.thrift
$thrift --gen java security.thrift
$thrift --gen java meta_admin.thrift

for gen_file in `find $TMP_DIR -name "*.java"`; do
    cat apache-licence-template $gen_file > $gen_file.tmp
    mv $gen_file.tmp $gen_file
done

cp -v -r $TMP_DIR/* ../src/main/java/
rm -rf $TMP_DIR

echo "done"
