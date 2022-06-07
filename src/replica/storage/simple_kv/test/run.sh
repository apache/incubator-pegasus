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


bin=./dsn.rep_tests.simple_kv

function run_single()
{
    prefix=$1
    echo "${bin} ${prefix}.ini ${prefix}.act"
    ${bin} ${prefix}.ini ${prefix}.act
    ret=$?
    if find . -name log.1.txt &>/dev/null; then
        log=`find . -name log.1.txt`
        cat ${log} | grep -v FAILURE_DETECT | grep -v BEACON | grep -v beacon | grep -v THREAD_POOL_FD >${prefix}.log
        rm ${log}
    fi

    if [ ${ret} -ne 0 ]; then
        echo "run ${prefix} failed, return value = ${ret}"
        if [ -f core ]; then
            echo "---- gdb ./dsn.rep_tests.simple_kv core ----"
            gdb ./dsn.rep_tests.simple_kv core -ex "thread apply all bt" -ex "set pagination 0" -batch
        fi
        exit 1
    fi
}

function run_case()
{
    id=$1

    if [ -d case-${id} ]; then
        cd case-${id}
        ./run.sh
        if [ $? -ne 0 ]; then
            exit 1
        fi
        cd ..
        return
    fi

    if [ -f case-${id}.act ]; then
        ./clear.sh
        run_single case-${id}
        return
    fi

    subcases=`ls case-${id}-[0-9].act 2>/dev/null | sed -n 's/^case-[0-9][0-9][0-9]-\([0-9]\).act$/\1/p' | sort -u`
    if [ ! -z "${subcases}" ]; then
        ./clear.sh
        for subid in ${subcases}; do
            run_single case-${id}-${subid}
        done
        return
    fi

    echo "case-${id} not found"
    exit 1
}

if [ $# -eq 0 ]; then
    if [ ! -z "${DSN_TEST_FILTER}" ]; then
        cases=`echo ${DSN_TEST_FILTER} | sed 's/[,:]/ /g'`
    else
        cases=`ls case-* 2>/dev/null | sed -n 's/^case-\([0-9][0-9][0-9]\).*$/\1/p' | sort -u`
    fi
else
    cases=$*
fi

if [ ! -z "${cases}" ]; then
    for id in ${cases}; do
        run_case ${id}
        echo
    done
fi

