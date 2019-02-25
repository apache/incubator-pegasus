#!/bin/bash
#
# Shell Options:
#    CLEAR          YES|NO
#    PART_CLEAR     YES|NO
#    JOB_NUM        <num>
#    BUILD_TYPE     debug|release
#    C_COMPILER     <str>
#    CXX_COMPILER   <str>
#    RUN_VERBOSE    YES|NO
#    WARNING_ALL    YES|NO
#    ENABLE_GCOV    YES|NO
#    BOOST_DIR      <dir>|""
#    TEST_MODULE    "<module1> <module2> ..."
#
# CMake options:
#    -DCMAKE_C_COMPILER=gcc|clang
#    -DCMAKE_CXX_COMPILER=g++|clang++
#    [-DCMAKE_BUILD_TYPE=Debug]
#    [-DWARNING_ALL=TRUE]
#    [-DENABLE_GCOV=TRUE]
#    [-DBoost_NO_BOOST_CMAKE=ON -DBOOST_ROOT=$BOOST_DIR -DBoost_NO_SYSTEM_PATHS=ON]

ROOT=`pwd`
BUILD_DIR="$ROOT/builder"

echo "DSN_ROOT=$DSN_ROOT"
echo "DSN_THIRDPARTY_ROOT=$DSN_THIRDPARTY_ROOT"
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

if [ "$PART_CLEAR" == "YES" ]
then
    echo "PART_CLEAR=YES"
else
    echo "PART_CLEAR=NO"
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

# valgrind can not work together with gpertools
# you may want to use this option when you want to run valgrind
if [ "$DISABLE_GPERF" == "YES" ]
then
    echo "DISABLE_GPERF=YES"
    CMAKE_OPTIONS="$CMAKE_OPTIONS -DENABLE_GPERF=Off"
else
    echo "DISABLE_GPERF=NO"
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
    # for makefile
    export BOOST_ROOT=$BOOST_DIR
else
    echo "Use system boost"
fi

echo "CMAKE_OPTIONS=$CMAKE_OPTIONS"

#rocksdb enable jemalloc by default, but we use regular malloc.
MAKE_OPTIONS="$MAKE_OPTIONS DISABLE_JEMALLOC=1"
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

if [ "$CLEAR" == "YES" -o "$PART_CLEAR" == "YES" ]
then
    echo "Clear $BUILD_DIR ..."
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
PEGASUS_GIT_COMMIT=`git log | head -n 1 | awk '{print $2}'`
GIT_COMMIT_FILE=include/pegasus/git_commit.h
if [ ! -f $GIT_COMMIT_FILE ] || ! grep $PEGASUS_GIT_COMMIT $GIT_COMMIT_FILE
then
    echo "Generating $GIT_COMMIT_FILE..."
    echo "#pragma once" >$GIT_COMMIT_FILE
    echo "#define PEGASUS_GIT_COMMIT \"$PEGASUS_GIT_COMMIT\"" >>$GIT_COMMIT_FILE
fi

cd $BUILD_DIR
echo "Building..."
make install $MAKE_OPTIONS
if [ $? -ne 0 ]
then
    echo "ERROR: build pegasus failed"
    exit 1
else
    echo "Build pegasus succeed"
fi
cd ..

