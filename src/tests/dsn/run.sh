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

./clear.sh
if [ -z "${REPORT_DIR}" ]; then
    REPORT_DIR="."
fi

filters="distributed_lock_service_zookeeper.simple_lock_unlock -distributed_lock_service_zookeeper.simple_lock_unlock"

for filter in $filters; do
    echo "============ run dsn.tests with gtest_filter ${filter} ============"
    output_xml="${REPORT_DIR}/dsn.tests.xml"
    #prepare_environment $filter

    GTEST_OUTPUT="xml:${output_xml}" GTEST_FILTER=$filter ./dsn.tests config-test.ini
    if [ $? -ne 0 ]; then
        echo "run dsn.tests failed"
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
        #destroy_environment $filter
        exit 1
    fi

    #destroy_environment $filter
    echo "============ done dsn.tests with gtest_filter ${filter} ============"
done
