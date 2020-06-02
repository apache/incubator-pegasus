#!/bin/bash
# !!! This script should be run in dsn project root directory (../../).
#
# Shell Options:
#    CLEAR          YES|NO
#    JOB_NUM        <num>
#    BUILD_TYPE     debug|release
#    C_COMPILER     <str>
#    CXX_COMPILER   <str>
#    ONLY_BUILD     YES|NO
#    RUN_VERBOSE    YES|NO
#    ENABLE_GCOV    YES|NO
#    BOOST_DIR      <dir>|""
#    TEST_MODULE    "<module1> <module2> ..."
#
# CMake options:
#    -DCMAKE_C_COMPILER=gcc|clang
#    -DCMAKE_CXX_COMPILER=g++|clang++
#    [-DCMAKE_BUILD_TYPE=Debug]
#    [-DENABLE_GCOV=TRUE]
#    [-DBoost_NO_BOOST_CMAKE=ON -DBOOST_ROOT=$BOOST_DIR -DBoost_NO_SYSTEM_PATHS=ON]

ROOT=`pwd`
REPORT_DIR=$ROOT/test_reports
BUILD_DIR="$ROOT/builder"
GCOV_DIR="$ROOT/gcov_report"
TIME=`date --rfc-3339=seconds`

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

if [ "$ONLY_BUILD" == "YES" ]
then
    echo "ONLY_BUILD=YES"
else
    echo "ONLY_BUILD=NO"
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

if [ "$NO_TEST" == "YES" ]
then
    echo "NO_TEST=YES"
    CMAKE_OPTIONS="$CMAKE_OPTIONS -DBUILD_TEST=OFF"
else
    echo "NO_TEST=NO"
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

if [ ! -z "$SANITIZER" ]
then
    echo "SANITIZER=$SANITIZER"
    CMAKE_OPTIONS="$CMAKE_OPTIONS -DSANITIZER=$SANITIZER"
else
    echo "Build without sanitizer"
fi

# You can specify customized boost by defining BOOST_DIR.
# Install boost like this:
#   wget http://downloads.sourceforge.net/project/boost/boost/1.54.0/boost_1_54_0.zip?r=&ts=1442891144&use_mirror=jaist
#   unzip -q boost_1_54_0.zip
#   cd boost_1_54_0
#   ./bootstrap.sh --with-libraries=system,filesystem --with-toolset=gcc
#   ./b2 toolset=gcc cxxflags="-std=c++11 -fPIC" -j8 -d0
#   ./b2 install --prefix=$DSN_ROOT -d0
# And set BOOST_DIR as:
#   export BOOST_DIR=/path/to/boost_1_54_0/output
if [ -n "$BOOST_DIR" ]
then
    echo "Use customized boost: $BOOST_DIR"
    CMAKE_OPTIONS="$CMAKE_OPTIONS -DBoost_NO_BOOST_CMAKE=ON -DBOOST_ROOT=$BOOST_DIR -DBoost_NO_SYSTEM_PATHS=ON"
else
    echo "Use system boost"
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
    cmake .. -DCMAKE_INSTALL_PREFIX=$BUILD_DIR/output $CMAKE_OPTIONS
    if [ $? -ne 0 ]
    then
        echo "ERROR: cmake failed"
        exit 1
    fi
    cd ..
fi

cd $ROOT
DSN_GIT_COMMIT=`git log | head -n 1 | awk '{print $2}'`
if [ $? -ne 0 ] || [ -z "$DSN_GIT_COMMIT" ] 
then
    echo "ERROR: get DSN_GIT_COMMIT failed"
    echo "HINT: check if rdsn is a git repo"
    echo "   or check gitdir in .git (edit it or use \"git --git-dir='../.git/modules/rdsn' log\" to get commit)"
    exit 1
fi
GIT_COMMIT_FILE=include/dsn/git_commit.h
if [ ! -f $GIT_COMMIT_FILE ] || ! grep $DSN_GIT_COMMIT $GIT_COMMIT_FILE
then
    echo "Generating $GIT_COMMIT_FILE..."
    echo "#pragma once" >$GIT_COMMIT_FILE
    echo "#define DSN_GIT_COMMIT \"$DSN_GIT_COMMIT\"" >>$GIT_COMMIT_FILE
fi

cd $BUILD_DIR
echo "Building..."
make install $MAKE_OPTIONS
if [ $? -ne 0 ]
then
    echo "ERROR: build failed"
    exit 1
else
    echo "Build succeed"
fi
cd ..

if [ "$ONLY_BUILD" == "YES" ]
then
    exit 0
fi

echo "################################# start testing ################################"

if [ -z "$TEST_MODULE" ]
then
    # supported test module
    TEST_MODULE="dsn.core.tests,dsn.tests,dsn_nfs_test,dsn_block_service_test,dsn.replication.simple_kv,dsn.rep_tests.simple_kv,dsn.meta.test,dsn.replica.test,dsn_http_test,dsn_replica_dup_test,dsn_replica_backup_test,dsn_replica_bulk_load_test"
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

