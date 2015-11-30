#!/bin/bash

CMAKE_OPTIONS="$CMAKE_OPTIONS -DCMAKE_BUILD_TYPE=Debug"
CMAKE_OPTIONS="$CMAKE_OPTIONS -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++"
#CMAKE_OPTIONS="$CMAKE_OPTIONS -DDSN_DEBUG_CMAKE=TRUE"
#CMAKE_OPTIONS="$CMAKE_OPTIONS -DWARNNING_ALL=TRUE"
#CMAKE_OPTIONS="$CMAKE_OPTIONS -DENABLE_GCOV=TRUE"

# You can specify customized boost by defining BOOST_DIR.
# Install boost like this:
#   wget http://downloads.sourceforge.net/project/boost/boost/1.54.0/boost_1_54_0.zip?r=&ts=1442891144&use_mirror=jaist
#   unzip -q boost_1_54_0.zip
#   cd boost_1_54_0
#   ./bootstrap.sh --with-libraries=system,filesystem --with-toolset=gcc
#   ./b2 toolset=gcc cxxflags="-std=c++11 -fPIC" -j8 -d0
#   ./b2 install --prefix=`pwd`/output -d0
# And set BOOST_DIR as:
#   export BOOST_DIR=/path/to/boost_1_54_0/output
if [ -n "$BOOST_DIR" ]
then
    echo "Use customized boost: $BOOST_DIR"
    CMAKE_OPTIONS="$CMAKE_OPTIONS -DBoost_NO_BOOST_CMAKE=ON -DBOOST_ROOT=$BOOST_DIR -DBoost_NO_SYSTEM_PATHS=ON"
fi

if [ -d "builder" -a $# -eq 1 -a "$1" == "true" ]
then
    echo "Clear builder..."
    rm -rf builder
fi

if [ ! -d "builder" ]
then
    echo "Running cmake..."
    mkdir -p builder
    cd builder
    cmake .. -DCMAKE_INSTALL_PREFIX=`pwd`/output $CMAKE_OPTIONS
    if [ $? -ne 0 ]
    then
        echo "ERROR: cmake failed"
        exit -1
    fi
    cd ..
fi

cd builder
echo "Building..."
make install -j8
if [ $? -ne 0 ]
then
    echo "ERROR: build failed"
    exit -1
else
    echo "Build succeed"
fi
cd ..

