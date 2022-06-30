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

ARG OS_VERSION=centos7
FROM apache/pegasus:thirdparties-src as builder

ARG OS_VERSION=centos7
FROM apache/pegasus:build-env-${OS_VERSION}

WORKDIR /root

COPY --from=builder /root/thirdparties-src.zip /root/thirdparties-src.zip

ARG GITHUB_BRANCH=master
ARG GITHUB_REPOSITORY_URL=https://github.com/apache/incubator-pegasus.git
RUN git clone --depth=1 --branch=${GITHUB_BRANCH} ${GITHUB_REPOSITORY_URL} \
    && cd incubator-pegasus/rdsn/thirdparty \
    && unzip /root/thirdparties-src.zip -d . \
    && cmake -DCMAKE_BUILD_TYPE=Release -DROCKSDB_PORTABLE=OFF -B build/ . \
    && cmake --build build/ -j $(($(nproc)/2+1)) \
    && zip -r ~/thirdparties-bin.zip output/ build/Source/rocksdb/cmake build/Source/http-parser build/Source/hadoop build/Download/zookeeper \
    && cd ~ \
    && rm -rf incubator-pegasus;
