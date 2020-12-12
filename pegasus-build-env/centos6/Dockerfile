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

FROM centos:6

LABEL maintainer=wutao

# As centos6 became EOL, we could only use a official vault mirror to fetch softwares.
RUN curl https://www.getpagespeed.com/files/centos6-eol.repo --output /etc/yum.repos.d/CentOS-Base.repo;

# test if the remote yum repository is valid
RUN yum list

RUN yum -y install centos-release-scl \
scl-utils \
epel-release; \
yum -y install devtoolset-7-gcc \
devtoolset-7-gcc-c++ \
rh-python36-python-pip \
java-1.8.0-openjdk-devel.x86_64 \
automake \
autoconf \
make \
libtool \
git \
file \
wget \
unzip \
which \
openssl-devel \
libaio-devel \
snappy-devel \
bzip2-devel \
zlib \
zlib-devel \
libzstd-devel \
lz4-devel \
bison \
flex \
patch; \
export LD_LIBRARY_PATH=/opt/rh/rh-python36/root/usr/lib64; \
export PATH=/opt/rh/rh-python36/root/usr/bin:${PATH}; \
pip install --no-cache-dir cmake; \
yum clean all; \
rm -rf /var/cache/yum;

ENV PATH="/opt/rh/devtoolset-7/root/usr/bin/:${PATH}"
ENV PATH="/opt/rh/rh-python36/root/usr/bin:${PATH}"

WORKDIR /root/pegasus
