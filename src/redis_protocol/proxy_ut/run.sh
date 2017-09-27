#!/bin/sh

if [ -z $REPORT_DIR ]; then
    REPORT_DIR="./"
fi

test_case=pegasus_rproxy_test
GTEST_OUTPUT="xml:$REPORT_DIR/$test_case.xml" ./$test_case
