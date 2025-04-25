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

FROM ubuntu:22.04

ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update -y; \
    apt-get install -y --no-install-recommends \
                       automake \
                       build-essential \
                       bison \
                       flex \
		       g++ \
                       libboost-dev \
                       libevent-dev \
                       libssl-dev \
                       libtool \
		       make \
                       pkg-config \
		       ca-certificates \
                       wget \
                       unzip; \
    rm -rf /var/lib/apt/lists/*

ARG THRIFT_VERSION=0.13.0
RUN wget --progress=dot:giga https://archive.apache.org/dist/thrift/${THRIFT_VERSION}/thrift-${THRIFT_VERSION}.tar.gz -P /opt/thrift && \
    cd /opt/thrift && tar xzf thrift-${THRIFT_VERSION}.tar.gz && cd thrift-${THRIFT_VERSION} && ./bootstrap.sh && \
    ./configure --enable-libs=no && \
    make -j$(($(nproc)/2+1)) && make install && cd - && \
    rm -rf thrift-${THRIFT_VERSION} thrift-${THRIFT_VERSION}.tar.gz
