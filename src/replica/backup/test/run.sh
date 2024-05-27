#!/bin/bash
# The MIT License (MIT)
#
# Copyright (c) 2015 Microsoft Corporation
#
# -=- Robust Distributed System Nucleus (rDSN) -=-
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

if [ -n ${TEST_OPTS} ]; then
    if [ ! -f "./config-test.ini" ]; then
        echo "./config-test.ini does not exists"
        exit 1
    fi

    OPTS=`echo ${TEST_OPTS} | xargs`
    config_kvs=(${OPTS//;/ })
    for config_kv in ${config_kvs[@]}; do
        config_kv=`echo $config_kv | xargs`
        kv=(${config_kv//=/ })
        if [ ! ${#kv[*]} -eq 2 ]; then
            echo "Invalid config kv !"
            exit 1
        fi
        sed -i '/^\s*'"${kv[0]}"'/c '"${kv[0]}"' = '"${kv[1]}" ./config-test.ini
    done
fi

./dsn_replica_backup_test

if [ $? -ne 0 ]; then
    tail -n 100 `find . -name pegasus.log.*`
    if [ -f core ]; then
        gdb ./dsn_replica_backup_test core -ex "bt"
    fi
    exit 1
fi
