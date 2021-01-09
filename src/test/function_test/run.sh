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

exit_if_fail() {
    if [ $1 != 0 ]; then
        echo $2
        exit 1
    fi
}

if [ -z $REPORT_DIR ]; then
    REPORT_DIR="./"
fi

# If run function tests on traivs, we exclude some time-consuming tests
# incluing restore test, recovery test
on_travis="NO"
while [ $# -gt 0 ]; do
    key="$1"
    case $key in
        --on_travis)
            on_travis="YES"
            ;;
        *)
            echo "Error: unknow option \"$key\""
            exit 1
            ;;
    esac
    shift
done

test_case=pegasus_function_test
config_file=config.ini
table_name=temp

GTEST_OUTPUT="xml:$REPORT_DIR/basic.xml" GTEST_FILTER="basic.*" ./$test_case $config_file $table_name
exit_if_fail $? "run test basic failed: $test_case $config_file $table_name"
GTEST_OUTPUT="xml:$REPORT_DIR/incr.xml" GTEST_FILTER="incr.*" ./$test_case $config_file $table_name
exit_if_fail $? "run test incr failed: $test_case $config_file $table_name"
GTEST_OUTPUT="xml:$REPORT_DIR/check_and_set.xml" GTEST_FILTER="check_and_set.*" ./$test_case $config_file $table_name
exit_if_fail $? "run test check_and_set failed: $test_case $config_file $table_name"
GTEST_OUTPUT="xml:$REPORT_DIR/check_and_mutate.xml" GTEST_FILTER="check_and_mutate.*" ./$test_case $config_file $table_name
exit_if_fail $? "run test check_and_mutate failed: $test_case $config_file $table_name"
GTEST_OUTPUT="xml:$REPORT_DIR/scan.xml" GTEST_FILTER="scan.*" ./$test_case $config_file $table_name
exit_if_fail $? "run test scan failed: $test_case $config_file $table_name"
GTEST_OUTPUT="xml:$REPORT_DIR/ttl.xml" GTEST_FILTER="ttl.*" ./$test_case $config_file $table_name
exit_if_fail $? "run test ttl failed: $test_case $config_file $table_name"
GTEST_OUTPUT="xml:$REPORT_DIR/slog_log.xml" GTEST_FILTER="lost_log.*" ./$test_case $config_file $table_name
exit_if_fail $? "run test slog_lost failed: $test_case $config_file $table_name"
GTEST_OUTPUT="xml:$REPORT_DIR/recall.xml" GTEST_FILTER="drop_and_recall.*" ./$test_case $config_file $table_name
exit_if_fail $? "run test recall failed: $test_case $config_file $table_name"
if [ $on_travis == "NO" ]; then
    GTEST_OUTPUT="xml:$REPORT_DIR/restore.xml" GTEST_FILTER="restore_test.*" ./$test_case $config_file $table_name
    exit_if_fail $? "run test restore_test failed: $test_case $config_file $table_name"
    GTEST_OUTPUT="xml:$REPORT_DIR/recovery.xml" GTEST_FILTER="recovery_test.*" ./$test_case $config_file $table_name
    exit_if_fail $? "run test recovery failed: $test_case $config_file $table_name"
    GTEST_OUTPUT="xml:$REPORT_DIR/bulk_load.xml" GTEST_FILTER="bulk_load_test.*" ./$test_case $config_file $table_name
    exit_if_fail $? "run test bulk load failed: $test_case $config_file $table_name"
    GTEST_OUTPUT="xml:$REPORT_DIR/test_detect_hotspot.xml" GTEST_FILTER="test_detect_hotspot.*" ./$test_case $config_file $table_name
    exit_if_fail $? "run test test_detect_hotspot load failed: $test_case $config_file $table_name"
fi
