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

FROM rockylinux/rockylinux:9.5.20241118

RUN dnf -y install epel-release && \
    dnf -y install autoconf \
                   automake \
                   bison \
                   ccache \
                   cmake \
                   cyrus-sasl-devel \
                   file \
                   flex \
                   gcc-c++ \
                   gdb \
                   git \
                   java-1.8.0-openjdk-devel \
                   krb5-devel \
                   libaio-devel \
                   libtool \
                   make \
                   maven \
                   nmap-ncat \
                   openssl-devel \
                   patch \
                   python3 \
                   unzip \
                   vim \
                   wget \
                   which \
                   zip \
                   zlib \
                   zlib-devel && \
    dnf clean all && \
    rm -rf /var/cache/dnf

RUN wget --progress=dot:giga https://archive.apache.org/dist/thrift/0.11.0/thrift-0.11.0.tar.gz -P /opt/thrift && \
    cd /opt/thrift && tar xzf thrift-0.11.0.tar.gz && cd thrift-0.11.0 && ./bootstrap.sh && \
    ./configure --enable-libs=no && \
    make -j$(($(nproc)/2+1)) && make install && cd - && \
    rm -rf thrift-0.11.0 thrift-0.11.0.tar.gz

ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk

WORKDIR /root/apache
