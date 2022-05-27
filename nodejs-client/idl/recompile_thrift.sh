#!/bin/bash
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

# complier require: thrift 0.10.0
 
thrift=thrift

if ! $thrift -version | grep '0.10.0' 
then
    echo "Should use thrift 0.10.0"
    exit -1
fi

TMP_DIR=./gen-nodejs
rm -rf $TMP_DIR

mkdir -p $TMP_DIR
$thrift --gen js:node rrdb.thrift
$thrift --gen js:node replication.thrift 
$thrift --gen js:node base.thrift

cp -v -r $TMP_DIR/* ../dsn
rm -rf $TMP_DIR

echo "done"
