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

set -e

CWD=$(cd "$(dirname "$0")" && pwd)

if [ $# -lt 1 ]; then
    echo "Invalid arguments !"
    echo "USAGE: $0 <HADOOP_BIN_PATH>"
    exit 1
fi

HADOOP_BIN_PATH=$1

HADOOP_VERSION="hadoop-3.3.6"
arch_output=$(arch)
if [ "$arch_output"x == "aarch64"x ]; then
    HADOOP_PACKAGE_MD5="369f899194a920e0d1c3c3bc1718b3b5"
    HADOOP_BASE_NAME=${HADOOP_VERSION}-"$(arch)"
else
    if [ "$arch_output"x != "x86_64"x ]; then
        echo "WARNING: unrecognized CPU architecture '$arch_output', use 'x86_64' as default"
    fi
    HADOOP_PACKAGE_MD5="1cbe1214299cd3bd282d33d3934b5cbd"
    HADOOP_BASE_NAME=${HADOOP_VERSION}
fi

DOWNLOAD_BASE_URL="https://mirrors.aliyun.com/apache/hadoop/common/${HADOOP_VERSION}/"
"${CWD}"/download_package.sh "${HADOOP_BASE_NAME}" ${HADOOP_PACKAGE_MD5} "${HADOOP_BIN_PATH}" ${DOWNLOAD_BASE_URL} "${HADOOP_VERSION}"
