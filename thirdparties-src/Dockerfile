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

FROM apachepegasus/build-env:ubuntu1804

WORKDIR /root

RUN git clone --depth=1 https://github.com/XiaoMi/rdsn.git \
    && cd rdsn/thirdparty \
    && mkdir -p build \
    && cmake -DCMAKE_BUILD_TYPE=Release -B build/ . \
    && cmake --build build/ -j $(($(nproc)/2+1)) \
    && zip -r ~/thirdparties-src.zip build/Download \
    && cd ~ \
    && rm -rf rdsn;

# Unzip thirdparties-src.zip to rdsn/thirdparty/build, the download will automatically be skipped.
