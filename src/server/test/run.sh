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

exit_if_fail() {
    if [ $1 != 0 ]; then
        echo $2
        exit 1
    fi
}

if [ -n ${TEST_OPTS} ]; then
    if [ ! -f ./config.ini ]; then
        echo "./config.ini does not exists !"
        exit 1
    fi

    OPTS=`echo ${TEST_OPTS} | xargs`
    config_kvs=(${OPTS//,/ })
    for config_kv in ${config_kvs[@]}; do
        config_kv=`echo $config_kv | xargs`
        kv=(${config_kv//=/ })
        if [ ! ${#kv[*]} -eq 2 ]; then
            echo "Invalid config kv !"
            exit 1
        fi
        sed -i '/^\s*'"${kv[0]}"'/c '"${kv[0]}"' = '"${kv[1]}" ./config.ini
    done
fi

./pegasus_unit_test

exit_if_fail $? "run unit test failed"

