#!/bin/sh

if [ -z $REPORT_DIR ]; then
    REPORT_DIR="./"
fi

test_case=pegasus_function_test
config_file=config.ini
table_name=temp
GTEST_OUTPUT="xml:$REPORT_DIR/$test_case.xml" GTEST_FILTER="-recovery_test.recovery" ./$test_case $config_file $table_name
GTEST_OUTPUT="xml:$REPORT_DIR/recovery.xml" GTEST_FILTER="recovery_test.recovery" ./$test_case $config_file $table_name
