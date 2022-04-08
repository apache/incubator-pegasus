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

FROM ubuntu:16.04

LABEL maintainer=wutao

ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update -y; \
    apt-get -y install build-essential \
                       software-properties-common \
                       openjdk-8-jdk \
                       python3-pip \
                       libaio-dev \
                       libsnappy-dev \
                       libbz2-dev \
                       libzstd-dev \
                       liblz4-dev \
                       zlib1g \
                       zlib1g.dev \
                       patch \
                       netcat \
                       wget \
                       ccache \
                       git \
                       curl \
                       zip \
                       gdb \
                       vim \
                       automake \
                       libtool \
                       libssl-dev \
                       bison \
                       libkrb5-dev \
                       libsasl2-dev \
                       maven \
                       flex; \
    rm -rf /var/lib/apt/lists/*

RUN add-apt-repository ppa:git-core/ppa -y; \
    apt-get update -y; \
    apt-get install git -y; \
    apt-get install pkg-config -y; \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir cmake

WORKDIR /root/apache
