#!/bin/bash
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

if [ ! -d "$PEGASUS_THIRDPARTY_ROOT" ]; then
  echo "ERROR: PEGASUS_THIRDPARTY_ROOT not set"
  exit 1
fi

if [ ! -d "$JAVA_HOME" ]; then
  echo "ERROR: JAVA_HOME not set"
  exit 1
fi

ARCH_TYPE=''
arch_output=$(arch)
if [ "$arch_output"x == "x86_64"x ]; then
    ARCH_TYPE="amd64"
elif [ "$arch_output"x == "aarch64"x ]; then
    ARCH_TYPE="aarch64"
else
    echo "WARNING: unrecognized CPU architecture '$arch_output', use 'x86_64' as default"
fi
export LD_LIBRARY_PATH=${JAVA_HOME}/jre/lib/${ARCH_TYPE}:${JAVA_HOME}/jre/lib/${ARCH_TYPE}/server:${PEGASUS_THIRDPARTY_ROOT}/output/lib:$(pwd)/../../lib:${LD_LIBRARY_PATH}

./sample onebox temp
