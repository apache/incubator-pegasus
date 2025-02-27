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

if [ $# -lt 3 ]; then
    echo "Invalid arguments !"
    echo "USAGE: $0 <PACKAGE_BASE_NAME> <PACKAGE_MD5> <TARGET_PATH>"
    exit 1
fi

PACKAGE_BASE_NAME=$1
PACKAGE_MD5=$2

TARGET_PATH=$3
if [ -d "${TARGET_PATH}" ]; then
    echo "TARGET_PATH ${TARGET_PATH} has existed, thus do not try to download ${PACKAGE_BASE_NAME}"
    exit 0
fi

DEFAULT_DOWNLOAD_BASE_URL="https://pegasus-thirdparty-package.oss-cn-beijing.aliyuncs.com/"
if [ $# -ge 4 ]; then
    DEFAULT_DOWNLOAD_BASE_URL=$4
fi

DIR_NAME=${PACKAGE_BASE_NAME}
if [ $# -ge 5 ]; then
    DIR_NAME=$5
fi

PACKAGE_NAME=${PACKAGE_BASE_NAME}.tar.gz
if [ ! -f "${PACKAGE_NAME}" ]; then
    echo "Downloading ${PACKAGE_NAME} ..."

    DOWNLOAD_URL=${DEFAULT_DOWNLOAD_BASE_URL}${PACKAGE_NAME}
    if ! wget -q -T 10 -t 5 "${DOWNLOAD_URL}"; then
        echo "ERROR: download ${PACKAGE_NAME} failed"
        exit 1
    fi

    if [ "$(md5sum "${PACKAGE_NAME}" | awk '{print$1}')" != "${PACKAGE_MD5}" ]; then
        echo "Check file ${PACKAGE_NAME} md5sum failed!"
        exit 1
    fi
fi

rm -rf "${DIR_NAME}"

echo "Decompressing ${PACKAGE_NAME} ..."
if ! tar xf "${PACKAGE_NAME}"; then
    echo "ERROR: decompress ${PACKAGE_NAME} failed"
    rm -f "${PACKAGE_NAME}"
    exit 1
fi

rm -f "${PACKAGE_NAME}"

if [ ! -d "${DIR_NAME}" ]; then
    echo "ERROR: ${DIR_NAME} does not exist"
    exit 1
fi

if [ -d "${TARGET_PATH}" ]; then
    echo "TARGET_PATH ${TARGET_PATH} has been generated, which means it and ${DIR_NAME} are the same dir thus do not do mv any more"
    exit 0
fi

mv "${DIR_NAME}" "${TARGET_PATH}"
