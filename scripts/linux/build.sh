#!/bin/bash
# !!! This script should be run in dsn project root directory (../../).
#
# Options:
#    DEBUG          YES|NO
#    WARNING_ALL    YES|NO
#    RUN_VERBOSE    YES|NO
#    CLEAR          YES|NO
#    BOOST_DIR      <dir>|""

BUILD_DIR="./builder"
CMAKE_OPTIONS="$CMAKE_OPTIONS -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++"
MAKE_OPTIONS="$MAKE_OPTIONS -j8"

if [ "$DEBUG" == "YES" ]
then
    echo "DEBUG=YES"
    CMAKE_OPTIONS="$CMAKE_OPTIONS -DCMAKE_BUILD_TYPE=Debug"
else
    echo "DEBUG=NO"
fi

if [ "$WARNING_ALL" == "YES" ]
then
    echo "WARNING_ALL=YES"
    CMAKE_OPTIONS="$CMAKE_OPTIONS -DWARNING_ALL=TRUE"
else
    echo "WARNING_ALL=NO"
fi

if [ "$RUN_VERBOSE" == "YES" ]
then
    echo "RUN_VERBOSE=YES"
    CMAKE_OPTIONS="$CMAKE_OPTIONS -DDSN_DEBUG_CMAKE=TRUE"
    MAKE_OPTIONS="$MAKE_OPTIONS VERBOSE=1"
else
    echo "RUN_VERBOSE=NO"
fi

if [ "$CLEAR" == "YES" ]
then
    echo "CLEAR=YES"
else
    echo "CLEAR=NO"
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
fi

if [ "$CLEAR" == "YES" -a -d "$BUILD_DIR" ]
then
    echo "Clear builder..."
    rm -rf $BUILD_DIR
fi

if [ ! -d "$BUILD_DIR" ]
then
    echo "Running cmake..."
    echo "CMAKE_OPTIONS=$CMAKE_OPTIONS"
    mkdir -p $BUILD_DIR
    cd $BUILD_DIR
    cmake .. -DCMAKE_INSTALL_PREFIX=`pwd`/output $CMAKE_OPTIONS
    if [ $? -ne 0 ]
    then
        echo "ERROR: cmake failed"
        exit -1
    fi
    cd ..
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

