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

FROM centos:6.10

LABEL maintainer=jiashuo

# defaut repo url hasn't been supported, use thirdparty repo url
RUN wget --progress=dot:giga https://www.getpagespeed.com/files/centos6-eol.repo -O /etc/yum.repos.d/CentOS-Base.repo
RUN wget --progress=dot:giga https://www.getpagespeed.com/files/centos6-epel-eol.repo -O /etc/yum.repos.d/epel.repo
RUN wget --progress=dot:giga https://www.getpagespeed.com/files/centos6-scl-eol.repo -O /etc/yum.repos.d/CentOS-SCLo-scl.repo
RUN wget --progress=dot:giga https://www.getpagespeed.com/files/centos6-scl-rh-eol.repo -O /etc/yum.repos.d/CentOS-SCLo-scl-rh.repo

# diable gpgcheck, otherwise the repo check will be failed.
RUN sed -i 's/gpgcheck=1/gpgcheck=0/g'  /etc/yum.repos.d/CentOS-Base.repo
RUN sed -i 's/gpgcheck=1/gpgcheck=0/g' /etc/yum.repos.d/epel.repo
RUN sed -i 's/gpgcheck=1/gpgcheck=0/g' /etc/yum.repos.d/CentOS-SCLo-scl-rh.repo
RUN sed -i 's/gpgcheck=1/gpgcheck=0/g' /etc/yum.repos.d/CentOS-SCLo-scl.repo

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

ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0/
ENV GCC_HOME=/opt/rh/devtoolset-7/root/usr/
ENV PYTHON2_HOME=/opt/rh/python27/root/usr/
ENV MAVEN_HOME=/opt/rh/rh-maven33/root/usr/

ENV PATH=$MAVEN_HOME/bin:$JAVA_HOME/bin:$GCC_HOME/bin:$PYTHON2_HOME/bin/:$PATH
ENV LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/amd64/server:$PYTHON2_HOME/lib64/:$LD_LIBRARY_PATH

RUN pip install --no-cache-dir cmake

RUN wget --progress=dot:giga https://github.com/apache/thrift/archive/refs/tags/0.11.0.tar.gz -P /opt/thrift && \
    cd /opt/thrift && tar xzf 0.11.0.tar.gz && cd thrift-0.11.0 && ./bootstrap.sh && \
    ./configure --enable-libs=no && \
    make -j$(($(nproc)/2+1)) && make install && cd - && \
    rm -rf thrift-0.11.0 0.11.0.tar.gz

WORKDIR /root/apache
