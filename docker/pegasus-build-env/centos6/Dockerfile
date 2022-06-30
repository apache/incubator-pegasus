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

# use the centos with valid yum repo url
FROM apachepegasus/centos-vault:6.10

LABEL maintainer=jiashuo

RUN yum -y install \
devtoolset-7-gcc \
devtoolset-7-gcc-c++ \
java-1.8.0-openjdk-devel \
git \
gettext \
curl-devel \
rh-maven33 \
python27 \
python34 \
automake \
autoconf \
make \
libtool \
file \
wget \
ccache \
nc \
zip \
gdb \
vim \
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
yum clean all; \
rm -rf /var/cache/yum;

ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk.x86_64/
ENV GCC_HOME=/opt/rh/devtoolset-7/root/usr/
ENV PYTHON2_HOME=/opt/rh/python27/root/usr/
ENV MAVEN_HOME=/opt/rh/rh-maven33/root/usr/

ENV PATH=$MAVEN_HOME/bin:$JAVA_HOME/bin:$GCC_HOME/bin:$PYTHON2_HOME/bin/:$PATH
ENV LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/amd64/server:$PYTHON2_HOME/lib64/:$LD_LIBRARY_PATH

RUN pip install --no-cache-dir cmake


WORKDIR /root/apache
