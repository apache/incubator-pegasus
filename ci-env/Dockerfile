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

FROM apachepegasus/build-env:centos7

WORKDIR /root

# install jdk for building pegasus(hdfs_service) and running zookeeper during testing.
RUN yum -y install ccache zip java-1.8.0-openjdk-devel.x86_64 nmap-ncat; \
    yum clean all; \
    rm -rf /var/cache/yum;

RUN git clone --depth=1 https://github.com/XiaoMi/rdsn.git; \
    cd rdsn/thirdparty; \
    mkdir -p build; \
    cmake -DCMAKE_BUILD_TYPE=Release -B build/ .; \
    cmake --build build/ -j $(($(nproc)/2+1)); \
    zip -r ~/pegasus-thirdparty-output.zip output/ build/Source/rocksdb/cmake build/Source/http-parser build/Download/zookeeper; \
    cd ~; \
    rm -rf rdsn;
