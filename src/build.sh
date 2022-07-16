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
#    TEST_MODULE    "<module1> <module2> ..."
#
# CMake options:
#    -DCMAKE_C_COMPILER=gcc|clang
#    -DCMAKE_CXX_COMPILER=g++|clang++
#    [-DCMAKE_BUILD_TYPE=Debug]
#    [-DWARNING_ALL=TRUE]
#    [-DENABLE_GCOV=TRUE]

ROOT=`pwd`
BUILD_DIR="$ROOT/builder"

echo "DSN_ROOT=$DSN_ROOT"
echo "THIRDPARTY_INSTALL_DIR=$THIRDPARTY_INSTALL_DIR"
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

if [ ! -z "$SANITIZER" ]
then
    echo "SANITIZER=$SANITIZER"
    CMAKE_OPTIONS="$CMAKE_OPTIONS -DSANITIZER=$SANITIZER"
else
    echo "Build without sanitizer"
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

CMAKE_OPTIONS="$CMAKE_OPTIONS -DBoost_NO_BOOST_CMAKE=ON -DBOOST_ROOT=${ROOT}/thirdparty/output -DBoost_NO_SYSTEM_PATHS=ON"

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
    cmake ../.. -DCMAKE_INSTALL_PREFIX=$BUILD_DIR/output $CMAKE_OPTIONS  -DBUILD_RDSN=OFF -DBUILD_PEGASUS=ON
    if [ $? -ne 0 ]
    then
        echo "ERROR: cmake failed"
        exit 1
    fi
    cd ..
fi

cd "$ROOT" || exit 1
PEGASUS_GIT_COMMIT="non-git-repo"
if git rev-parse HEAD; then # this is a git repo
    PEGASUS_GIT_COMMIT=$(git rev-parse HEAD)
fi
echo "PEGASUS_GIT_COMMIT=${PEGASUS_GIT_COMMIT}"
GIT_COMMIT_FILE=include/pegasus/git_commit.h
echo "Generating $GIT_COMMIT_FILE..."
echo "#pragma once" >$GIT_COMMIT_FILE
echo "#define PEGASUS_GIT_COMMIT \"$PEGASUS_GIT_COMMIT\"" >>$GIT_COMMIT_FILE

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
