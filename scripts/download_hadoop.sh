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

if [ $# -lt 1 ]; then
    echo "Target HADOOP_BIN_PATH is not provided, thus do not try to download hadoop"
    exit 0
fi

HADOOP_BIN_PATH=$1
if [ -d ${HADOOP_BIN_PATH} ]; then
    echo "Target HADOOP_BIN_PATH ${HADOOP_BIN_PATH} has existed, thus do not try to download hadoop"
    exit 0
fi

HADOOP_VERSION=2.8.4
HADOOP_DIR_NAME=hadoop-${HADOOP_VERSION}
HADOOP_PACKAGE_NAME=${HADOOP_DIR_NAME}.tar.gz
if [ ! -f ${HADOOP_PACKAGE_NAME} ]; then
    echo "Downloading hadoop..."

    DOWNLOAD_URL="https://pegasus-thirdparty-package.oss-cn-beijing.aliyuncs.com/${HADOOP_PACKAGE_NAME}"
    if ! wget -T 10 -t 5 ${DOWNLOAD_URL}; then
        echo "ERROR: download hadoop failed"
        exit 1
    fi

    HADOOP_PACKAGE_MD5="b30b409bb69185003b3babd1504ba224"
    if [ `md5sum ${HADOOP_PACKAGE_NAME} | awk '{print$1}'` != ${HADOOP_PACKAGE_MD5} ]; then
        echo "Check file ${HADOOP_PACKAGE_NAME} md5sum failed!"
        exit 1
    fi
fi

rm -rf ${HADOOP_DIR_NAME}

echo "Decompressing hadoop..."
if ! tar xf ${HADOOP_PACKAGE_NAME}; then
    echo "ERROR: decompress hadoop failed"
    rm -f ${HADOOP_PACKAGE_NAME}
    exit 1
fi

rm -f ${HADOOP_PACKAGE_NAME}

if [ ! -d ${HADOOP_DIR_NAME} ]; then
    echo "ERROR: ${HADOOP_DIR_NAME} does not exist"
    exit 1
fi

mv ${HADOOP_DIR_NAME} ${HADOOP_BIN_PATH}
