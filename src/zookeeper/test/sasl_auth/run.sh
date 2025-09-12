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

if [ -z "${REPORT_DIR}" ]; then
    REPORT_DIR="."
fi

#./clear.sh

output_xml="${REPORT_DIR}/zookeeper_sasl_auth_test.xml"
GTEST_OUTPUT="xml:${output_xml}" ./zookeeper_sasl_auth_test

if [ $? -ne 0 ]; then
    echo "run zookeeper_sasl_auth_test failed"
    echo "---- ls ----"
    ls -l
    if [ `find . -name pegasus.log.* | wc -l` -ne 0 ]; then
        echo "---- tail -n 100 pegasus.log.* ----"
        tail -n 100 `find . -name pegasus.log.*`
    fi
    if [ -f core ]; then
        echo "---- gdb ./zookeeper_sasl_auth_test core ----"
        gdb ./zookeeper_sasl_auth_test core -ex "thread apply all bt" -ex "set pagination 0" -batch
    fi
    exit 1
fi
