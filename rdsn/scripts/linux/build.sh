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

# !!! This script should be run in dsn project root directory (../../).
#
# Shell Options:
#    CLEAR          YES|NO
#    JOB_NUM        <num>
#    BUILD_TYPE     debug|release
#    C_COMPILER     <str>
#    CXX_COMPILER   <str>
#    RUN_VERBOSE    YES|NO
#    ENABLE_GCOV    YES|NO
#    TEST_MODULE    "<module1> <module2> ..."
#
# CMake options:
#    -DCMAKE_C_COMPILER=gcc|clang
#    -DCMAKE_CXX_COMPILER=g++|clang++
#    [-DCMAKE_BUILD_TYPE=Debug]
#    [-DENABLE_GCOV=TRUE]

ROOT=`pwd`
REPORT_DIR=$ROOT/test_reports
BUILD_DIR="$ROOT/builder"
GCOV_DIR="$ROOT/gcov_report"
TIME=`date '+%Y-%m-%d %H:%M:%S'`

echo "C_COMPILER=$C_COMPILER"
echo "CXX_COMPILER=$CXX_COMPILER"
CMAKE_OPTIONS="$CMAKE_OPTIONS -DCMAKE_C_COMPILER=$C_COMPILER -DCMAKE_CXX_COMPILER=$CXX_COMPILER"

echo "JOB_NUM=$JOB_NUM"
MAKE_OPTIONS="$MAKE_OPTIONS -j$JOB_NUM"

if [ "$CLEAR" == "YES" ]
then
    echo "CLEAR=YES"
else
    echo "CLEAR=NO"
fi

if [ "$BUILD_TYPE" == "debug" ]
then
    echo "BUILD_TYPE=debug"
    CMAKE_OPTIONS="$CMAKE_OPTIONS -DCMAKE_BUILD_TYPE=Debug"
else
    echo "BUILD_TYPE=release"
fi

if [ "$RUN_VERBOSE" == "YES" ]
then
    echo "RUN_VERBOSE=YES"
    MAKE_OPTIONS="$MAKE_OPTIONS VERBOSE=1"
else
    echo "RUN_VERBOSE=NO"
fi

if [ "$ENABLE_GCOV" == "YES" ]
then
    echo "ENABLE_GCOV=YES"
    CMAKE_OPTIONS="$CMAKE_OPTIONS -DENABLE_GCOV=TRUE"
else
    echo "ENABLE_GCOV=NO"
fi

if [ "$TEST" == "NO" ]
then
    echo "TEST=NO"
    CMAKE_OPTIONS="$CMAKE_OPTIONS -DBUILD_TEST=OFF"
else
    echo "TEST=YES"
fi

# valgrind can not work together with gpertools
# you may want to use this option when you want to run valgrind
if [ "$DISABLE_GPERF" == "YES" ]
then
    echo "DISABLE_GPERF=YES"
    CMAKE_OPTIONS="$CMAKE_OPTIONS -DENABLE_GPERF=Off"
else
    echo "DISABLE_GPERF=NO"
fi

if [ "$USE_JEMALLOC" == "YES" ]
then
    echo "USE_JEMALLOC=YES"
    CMAKE_OPTIONS="$CMAKE_OPTIONS -DUSE_JEMALLOC=ON"
else
    echo "USE_JEMALLOC=NO"
fi

if [ ! -z "$SANITIZER" ]
then
    echo "SANITIZER=$SANITIZER"
    CMAKE_OPTIONS="$CMAKE_OPTIONS -DSANITIZER=$SANITIZER"
else
    echo "Build without sanitizer"
fi

echo "CMAKE_OPTIONS=$CMAKE_OPTIONS"
echo "MAKE_OPTIONS=$MAKE_OPTIONS"

echo "#############################################################################"

if [ -f $BUILD_DIR/CMAKE_OPTIONS ]
then
    LAST_OPTIONS=`cat $BUILD_DIR/CMAKE_OPTIONS`
    if [ "$CMAKE_OPTIONS" != "$LAST_OPTIONS" ]
    then
        echo "WARNING: CMAKE_OPTIONS has changed from last build, clear environment first"
        CLEAR=YES
    fi
fi

if [ "$CLEAR" == "YES" -a -d "$BUILD_DIR" ]
then
    echo "Clear builder..."
    rm -rf $BUILD_DIR
fi

if [ ! -d "$BUILD_DIR" ]
then
    echo "Running cmake..."
    mkdir -p $BUILD_DIR
    cd $BUILD_DIR
    echo "$CMAKE_OPTIONS" >CMAKE_OPTIONS
    cmake .. -DOPENSSL_ROOT_DIR=$MACOS_OPENSSL_ROOT_DIR -DCMAKE_INSTALL_PREFIX=$BUILD_DIR/output $CMAKE_OPTIONS
    if [ $? -ne 0 ]
    then
        echo "ERROR: cmake failed"
        exit 1
    fi
    cd ..
fi

cd $BUILD_DIR
echo "[$(date)] Building..."
make install $MAKE_OPTIONS
if [ $? -ne 0 ]
then
    echo "ERROR: build failed"
    exit 1
else
    echo "[$(date)] Build succeed"
fi
cd ..

if [ "$TEST" == "NO" ]
then
    exit 0
fi

echo "################################# start testing ################################"

if [ -z "$TEST_MODULE" ]
then
    # supported test module
    TEST_MODULE="dsn_runtime_tests,dsn_utils_tests,dsn_perf_counter_test,dsn.zookeeper.tests,dsn_aio_test,dsn.failure_detector.tests,dsn_meta_state_tests,dsn_nfs_test,dsn_block_service_test,dsn.replication.simple_kv,dsn.rep_tests.simple_kv,dsn.meta.test,dsn.replica.test,dsn_http_test,dsn_replica_dup_test,dsn_replica_backup_test,dsn_replica_bulk_load_test,dsn_replica_split_test"
fi

echo "TEST_MODULE=$TEST_MODULE"

if [ ! -d "$REPORT_DIR" ]
then
    mkdir -p $REPORT_DIR
fi

for MODULE in `echo $TEST_MODULE | sed 's/,/ /g'`; do
    echo "====================== run $MODULE =========================="
    MODULE_DIR=$BUILD_DIR/bin/$MODULE
    if [ ! -d "$MODULE_DIR" ]
    then
        echo "ERROR: module dir $MODULE_DIR not exist"
        exit 1
    fi
    if [ ! -f "$MODULE_DIR/run.sh" ]
    then
        echo "ERROR: module test entrance script $MODULE_DIR/run.sh doesn't exist"
        exit 1
    fi
    cd $MODULE_DIR
    REPORT_DIR=$REPORT_DIR ./run.sh
    ret=$?
    if [ $ret -ne 0 ]
    then
        echo "ERROR: run $MODULE failed, return_code = $ret"
        exit 1
    fi
done

if [ "$ENABLE_GCOV" == "YES" ]
then
    echo "Generating gcov report..."
    cd $ROOT
    mkdir -p $GCOV_DIR

    echo "Running gcovr to produce HTML code coverage report."
    gcovr --html --html-details -r $ROOT --object-directory=$BUILD_DIR \
          -o $GCOV_DIR/index.html
    if [ $? -ne 0 ]
    then
        exit 1
    fi
fi

echo "Test succeed"

