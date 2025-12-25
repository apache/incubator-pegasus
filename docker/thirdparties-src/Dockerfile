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
FROM apache/pegasus:build-env-${OS_VERSION}-${GITHUB_BRANCH} as base


# --- Ubuntu 特有层 ---
FROM base AS stage-ubuntu2004
ENV OPENSSL_ROOT_DIR=/usr/local/ssl \
    LIBRARY_PATH=/usr/local/ssl/lib64:/usr/local/ssl/lib:$LIBRARY_PATH \
    LD_LIBRARY_PATH=/usr/local/ssl/lib64:/usr/local/ssl/lib:$LD_LIBRARY_PATH \
    CMAKE_PREFIX_PATH=/usr/local/ssl:$CMAKE_PREFIX_PATH 

# --- 其他系统默认层 ---
FROM base AS stage-rockylinux9
# 不做任何操作

# --- 最终编译切换 ---
# 必须再次声明 ARG 才能在 FROM 中使用
ARG OS_VERSION
FROM stage-${OS_VERSION} AS builder

WORKDIR /root

ARG GITHUB_BRANCH
ARG GITHUB_REPOSITORY_URL=https://github.com/apache/incubator-pegasus.git
RUN git clone --depth=1 --branch=${GITHUB_BRANCH} ${GITHUB_REPOSITORY_URL}

ARG ROCKSDB_PORTABLE=native
RUN cd incubator-pegasus/thirdparty \
    && mkdir -p build \
    && cmake -DCMAKE_BUILD_TYPE=Release -DROCKSDB_PORTABLE=${ROCKSDB_PORTABLE} -B build/ . \
    && cmake --build build/ -j $(($(nproc)/2+1))

RUN cd incubator-pegasus/thirdparty \
    && zip -r ~/thirdparties-src.zip build/Download \
    && cd ~ \
    && rm -rf incubator-pegasus;

# Unzip thirdparties-src.zip to thirdparty, the download will automatically be skipped.

FROM alpine

COPY --from=builder /root/thirdparties-src.zip /root/thirdparties-src.zip
