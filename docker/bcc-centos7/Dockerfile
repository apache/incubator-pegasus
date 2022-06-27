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

FROM centos:7.5.1804 as builder

LABEL maintainer=wutao

RUN yum install -y epel-release \
  && yum update -y \
  && yum groupinstall -y "Development tools" \
  && yum install -y elfutils-libelf-devel git bison flex ncurses-devel python3 \
  && yum install -y luajit luajit-devel \
  && yum -y install centos-release-scl \
  && yum-config-manager --enable rhel-server-rhscl-7-rpms \
  && yum install -y devtoolset-7 llvm-toolset-7 llvm-toolset-7-llvm-devel llvm-toolset-7-llvm-static llvm-toolset-7-clang-devel \
  && yum clean all \
  && rm -rf /var/cache/yum;

RUN pip3 install --no-cache-dir cmake

# install results to /root/bcc
RUN git clone https://github.com/iovisor/bcc.git \
  && cd bcc \
  && source scl_source enable devtoolset-7 llvm-toolset-7 \
  && cmake -B build/ . \
  && cmake --build build/ -j $(($(nproc)/2+1)) \
  && cmake --install build/ --prefix $HOME/bcc \
  && rm -rf /bcc

FROM centos:7.5.1804

COPY --from=builder /root/bcc /bcc-pkg/bcc
COPY --from=builder /opt/rh/llvm-toolset-7 /bcc-pkg/llvm-toolset-7
COPY --from=builder /usr/lib/python2.7/site-packages/ /bcc-pkg/site-packages/

ENV LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:/bcc-pkg/bcc/lib64/"
ENV LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:/bcc-pkg/llvm-toolset-7/root/lib64"
ENV PYTHONPATH="${PYTHONPATH}:/bcc-pkg/site-packages"
