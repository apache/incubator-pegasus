#!/bin/bash

bin=./dsn.rep_tests.simple_kv

function run_single()
{
    prefix=$1
    echo "${bin} ${prefix}.ini ${prefix}.act"
    ${bin} ${prefix}.ini ${prefix}.act
    ret=$?
    if [ -f log.1.txt ]; then
        #mv log.1.txt ${prefix}.log
        cat log.1.txt | grep -v FAILURE_DETECT | grep -v BEACON | grep -v beacon | grep -v THREAD_POOL_FD >${prefix}.log
        rm log.1.txt
    fi
    if [ ${ret} -ne 0 ]; then
        echo "run ${prefix} failed"
        exit -1
    fi
}

function run_case()
{
    id=$1

    if [ -d case-${id} ]; then
        cd case-${id}
        ./run.sh
        if [ $? -ne 0 ]; then
            exit -1
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
    exit -1
}

if [ $# -eq 0 ]; then
    cases=`ls case-* 2>/dev/null | sed -n 's/^case-\([0-9][0-9][0-9]\).*$/\1/p' | sort -u`
else
    cases=$*
fi

if [ ! -z "${cases}" ]; then
    for id in ${cases}; do
        run_case ${id}
        echo
    done
fi

