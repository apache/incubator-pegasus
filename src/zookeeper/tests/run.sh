#!/bin/bash

if [ -z "${REPORT_DIR}" ]; then
    REPORT_DIR="."
fi

while read -r -a line; do
    test_case=${line[0]}
    gtest_filter=${line[1]}
    output_xml="${REPORT_DIR}/dsn.zookeeper.tests_${test_case/.ini/.xml}"
    echo "============ run dsn.zookeeper.tests ${test_case} with gtest_filter ${gtest_filter} ============"
    ./clear.sh
    GTEST_OUTPUT="xml:${output_xml}" GTEST_FILTER=${gtest_filter} ./dsn.zookeeper.tests ${test_case}

    if [ $? -ne 0 ]; then
        echo "run dsn.zookeeper.tests $test_case failed"
        echo "---- ls ----"
        ls -l
        if find . -name log.1.txt; then
            echo "---- tail -n 100 log.1.txt ----"
            tail -n 100 `find . -name log.1.txt`
        fi
        if [ -f core ]; then
            echo "---- gdb ./dsn.zookeeper.tests core ----"
            gdb ./dsn.zookeeper.tests core -ex "thread apply all bt" -ex "set pagination 0" -batch
        fi
        exit 1
    fi
    echo "============ done dsn.zookeeper.tests ${test_case} with gtest_filter ${gtest_filter} ============"
done <gtest.filter
