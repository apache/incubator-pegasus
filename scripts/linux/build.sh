#!/bin/bash
# !!! This script should be run in dsn project root directory (../../).
#
# Shell Options:
#    CLEAR          YES|NO
#    JOB_NUM        <num>
#    BUILD_TYPE     debug|release
#    ONLY_BUILD     YES|NO
#    RUN_VERBOSE    YES|NO
#    WARNING_ALL    YES|NO
#    ENABLE_GCOV    YES|NO
#    BOOST_DIR      <dir>|""
#    TEST_MODULE    "<module1> <module2> ..."
#
# CMake options:
#    -DCMAKE_C_COMPILER=gcc
#    -DCMAKE_CXX_COMPILER=g++
#    [-DCMAKE_BUILD_TYPE=Debug]
#    [-DDSN_GIT_SOURCE=github|xiaomi]
#    [-DWARNING_ALL=TRUE]
#    [-DENABLE_GCOV=TRUE]
#    [-DBoost_NO_BOOST_CMAKE=ON -DBOOST_ROOT=$BOOST_DIR -DBoost_NO_SYSTEM_PATHS=ON]

ROOT=`pwd`
REPORT_DIR=$ROOT/test_reports
BUILD_DIR="$ROOT/builder"
GCOV_DIR="$ROOT/gcov_report"
GCOV_TMP="$ROOT/.gcov_tmp"
GCOV_PATTERN=`find $ROOT/include $ROOT/src -name '*.h' -o -name '*.cpp'`
TIME=`date --rfc-3339=seconds`
CMAKE_OPTIONS="$CMAKE_OPTIONS -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++"
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

echo "SERIALIZE_TYPE=$SERIALIZE_TYPE"
if [ -n "$SERIALIZE_TYPE" ]
then
    CMAKE_OPTIONS="$CMAKE_OPTIONS -DDSN_SERIALIZATION_TYPE=$SERIALIZE_TYPE"
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

if [ "$WARNING_ALL" == "YES" ]
then
    echo "WARNING_ALL=YES"
    CMAKE_OPTIONS="$CMAKE_OPTIONS -DWARNING_ALL=TRUE"
else
    echo "WARNING_ALL=NO"
fi

if [ "$ENABLE_GCOV" == "YES" ]
then
    echo "ENABLE_GCOV=YES"
    CMAKE_OPTIONS="$CMAKE_OPTIONS -DENABLE_GCOV=TRUE"
else
    echo "ENABLE_GCOV=NO"
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

if [ ! -f "$ROOT/bin/Linux/thrift" ]
then
    echo "Downloading thrift..."
    wget --no-check-certificate https://github.com/imzhenyu/thrift/raw/master/pre-built/ubuntu14.04/thrift
    chmod u+x thrift
    mv thrift $ROOT/bin/Linux
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
        exit -1
    fi
    cd ..
fi

cd $ROOT
DSN_GIT_COMMIT=`git log | head -n 1 | awk '{print $2}'`
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
    exit -1
else
    echo "Build succeed"
fi
cd ..

if [ "$ONLY_BUILD" == "YES" ]
then
    exit 0
fi

echo "#############################################################################"

##############################################
## Supported test module:
##  - dsn.core.tests
##  - dsn.tests
##  - dsn.rep_tests.simple_kv
##  - dsn.replication.simple_kv
##############################################
if [ -z "$TEST_MODULE" ]
then
    TEST_MODULE="dsn.core.tests,dsn.tests,dsn.replication.simple_kv,dsn.rep_tests.simple_kv,dsn.meta.test,dsn.replica.test"
fi

echo "TEST_MODULE=$TEST_MODULE"

if [ "$ENABLE_GCOV" == "YES" ]
then
    echo "Initializing gcov..."
    cd $ROOT
    rm -rf $GCOV_TMP &>/dev/null
    mkdir -p $GCOV_TMP
    lcov -q -d $BUILD_DIR -z
    lcov -q -d $BUILD_DIR -b $ROOT --no-external --initial -c -o $GCOV_TMP/initial.info
    if [ $? -ne 0 ]
    then
        echo "ERROR: lcov init failed, maybe need to run again with --clear option"
        exit -1
    fi
    lcov -q -e $GCOV_TMP/initial.info $GCOV_PATTERN -o $GCOV_TMP/initial.extract.info
    if [ $? -ne 0 ]
    then
        echo "ERROR: lcov init extract failed"
        exit -1
    fi
fi

if [ ! -d "$REPORT_DIR" ]
then
    mkdir -p $REPORT_DIR
fi

for MODULE in `echo $TEST_MODULE | sed 's/,/ /g'`; do
    echo "====================== run $MODULE =========================="
    cd $BUILD_DIR/bin/$MODULE
    REPORT_DIR=$REPORT_DIR ./run.sh
    if [ $? -ne 0 ]
    then
        echo "ERROR: run $MODULE failed"
        exit -1
    fi
done

if [ "$ENABLE_GCOV" == "YES" ]
then
    echo "Generating gcov report..."
    cd $ROOT
    lcov -q -d $BUILD_DIR -b $ROOT --no-external -c -o $GCOV_TMP/gcov.info
    if [ $? -ne 0 ]
    then
        echo "ERROR: lcov generate failed"
        exit -1
    fi
    lcov -q -e $GCOV_TMP/gcov.info $GCOV_PATTERN -o $GCOV_TMP/gcov.extract.info
    if [ $? -ne 0 ]
    then
        echo "ERROR: lcov extract failed"
        exit -1
    fi
    genhtml $GCOV_TMP/*.extract.info --show-details --legend --title "GCOV report at $TIME" -o $GCOV_TMP/report
    if [ $? -ne 0 ]
    then
        echo "ERROR: gcov genhtml failed"
        exit -1
    fi
    rm -rf $GCOV_DIR &>/dev/null
    mv $GCOV_TMP/report $GCOV_DIR
    rm -rf $GCOV_TMP
    echo "View gcov report: firefox $GCOV_DIR/index.html"
fi

echo "Test succeed"

