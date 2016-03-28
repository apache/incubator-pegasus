#!/bin/bash

if [ -z "${REPORT_DIR}" ]; then
    REPORT_DIR="."
fi
cat gtest.filter | while read -r -a line; do
    echo GTEST_FILTER=\"${line[1]}\" ./dsn.core.tests ${line[0]}
    echo "============ run dsn.core.tests $test_case ============"
    ./clear.sh
    output_xml="${REPORT_DIR}/dsn.core.tests_${test_case/.ini/.xml}"
    export GTEST_OUTPUT="xml:${output_xml}"
    GTEST_FILTER=${line[1]} ./dsn.core.tests ${line[0]} < command.txt

    if [ $? -ne 0 ]; then
        echo "run dsn.core.tests $test_case failed"
        echo "---- ls ----"
        ls -l
        if find . -name log.1.txt; then
            echo "---- tail -n 100 log.1.txt ----"
            tail -n 100 `find . -name log.1.txt`
        fi
        if [ -f core ]; then
            echo "---- gdb ./dsn.core.tests core ----"
            gdb ./dsn.core.tests core -ex "thread apply all bt" -ex "set pagination 0" -batch
        fi
        exit -1
    fi
done

echo "============ done ============"

