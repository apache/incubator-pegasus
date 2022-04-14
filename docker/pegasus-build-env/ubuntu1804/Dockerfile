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

FROM ubuntu:18.04

LABEL maintainer=wutao

ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update -y; \
    apt-get install -y --no-install-recommends \
                       build-essential \
                       software-properties-common \
                       clang-9 \
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
                       unzip \
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
    apt-get install git -y --no-install-recommends; \
    apt-get install pkg-config -y --no-install-recommends; \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir cmake

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV CLASSPATH=$JAVA_HOME/lib/
ENV PATH=$JAVA_HOME/bin:$PATH
ENV LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/amd64/server:$LD_LIBRARY_PATH

WORKDIR /root/apache
