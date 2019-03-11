#!/bin/bash

function prepare_environment()
{
    if [ $1 == "distributed_lock_service_zookeeper.simple_lock_unlock" ]; then
        echo "test $1, try to restart to zookeeper service periodically"
        ./restart_zookeeper.sh >output.log 2>&1 &
    fi
}

function destroy_environment()
{
    if [ $1 == "distributed_lock_service_zookeeper.simple_lock_unlock" ]; then
        echo "stop restart-zookeeper process"
        ps aux | grep restart_zookeeper | grep -v "grep" | awk '{print $2}' | xargs kill -9
        echo "make sure the zookeeper is started"
        pushd ../../../ && ./run.sh start_zk && popd
    fi
}

if [ -z "${REPORT_DIR}" ]; then
    REPORT_DIR="."
fi

while read -r -a line; do
    test_case=${line[0]}
    gtest_filter=${line[1]}
    output_xml="${REPORT_DIR}/dsn.tests_${test_case/.ini/.xml}"
    echo "============ run dsn.tests ${test_case} with gtest_filter ${gtest_filter} ============"
    ./clear.sh
    GTEST_OUTPUT="xml:${output_xml}" GTEST_FILTER=${gtest_filter} ./dsn.tests ${test_case}

    if [ $? -ne 0 ]; then
        echo "run dsn.tests $test_case failed"
        echo "---- ls ----"
        ls -l
        if find . -name log.1.txt; then
            echo "---- tail -n 100 log.1.txt ----"
            tail -n 100 `find . -name log.1.txt`
        fi
        if [ -f core ]; then
            echo "---- gdb ./dsn.tests core ----"
            gdb ./dsn.tests core -ex "thread apply all bt" -ex "set pagination 0" -batch
        fi
        exit 1
    fi
    echo "============ done dsn.tests ${test_case} with gtest_filter ${gtest_filter} ============"
done <gtest.filter
