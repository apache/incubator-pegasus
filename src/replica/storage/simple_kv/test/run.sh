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


bin=./dsn.rep_tests.simple_kv

function run_single()
{
    prefix=$1

    if [ -n ${TEST_OPTS} ]; then
        if [ ! -f "./${prefix}.ini" ]; then
            echo "./${prefix}.ini does not exists"
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
            sed -i '/^\s*'"${kv[0]}"'/c '"${kv[0]}"' = '"${kv[1]}" ./${prefix}.ini
        done
    fi

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
    OLD_TEST_OPTS=${TEST_OPTS}
    TEST_OPTS=${OLD_TEST_OPTS},encrypt_data_at_rest=false
    for id in ${cases}; do
        run_case ${id}
        echo
    done
    TEST_OPTS=${OLD_TEST_OPTS},encrypt_data_at_rest=true
    for id in ${cases}; do
        run_case ${id}
        echo
    done
fi

