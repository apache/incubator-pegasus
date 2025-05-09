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

ARG GITHUB_BRANCH=master
ARG OS_VERSION=rockylinux9
FROM apache/pegasus:thirdparties-src-${OS_VERSION}-${GITHUB_BRANCH} as builder
FROM apache/pegasus:build-env-${OS_VERSION}-${GITHUB_BRANCH}

WORKDIR /root

COPY --from=builder /root/thirdparties-src.zip /root/thirdparties-src.zip

ARG GITHUB_BRANCH
ARG GITHUB_REPOSITORY_URL=https://github.com/apache/incubator-pegasus.git
ARG ROCKSDB_PORTABLE=native
ARG USE_JEMALLOC=OFF
ARG HADOOP_BIN_PATH=hadoop-bin
ARG ZOOKEEPER_BIN_PATH=zookeeper-bin
RUN git clone --depth=1 --branch=${GITHUB_BRANCH} ${GITHUB_REPOSITORY_URL} \
    && cd incubator-pegasus/thirdparty \
    && unzip /root/thirdparties-src.zip -d . \
    && cmake -DCMAKE_BUILD_TYPE=Release -DROCKSDB_PORTABLE=${ROCKSDB_PORTABLE} -DUSE_JEMALLOC=${USE_JEMALLOC} -B build/ . \
    && cmake --build build/ -j $(($(nproc)/2+1)) \
    && ../admin_tools/download_hadoop.sh ${HADOOP_BIN_PATH} \
    && ../admin_tools/download_zk.sh ${ZOOKEEPER_BIN_PATH} \
    && zip -r ~/thirdparties-bin.zip output/ build/Source/rocksdb/cmake build/Source/http-parser build/Source/hadoop build/Download/zookeeper ${HADOOP_BIN_PATH} ${ZOOKEEPER_BIN_PATH} \
    && cd ~ \
    && rm -rf incubator-pegasus;
