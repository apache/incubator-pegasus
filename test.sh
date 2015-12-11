#!/bin/bash

if [ -n "$DSN_ROOT"]
then
    export DSN_ROOT=`pwd`/install
    echo export DSN_ROOT=$DSN_ROOT >> ~/.bashrc
    echo export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:\$DSN_ROOT/lib >> ~/.bashrc
    echo "================ THIS IS THE FIRST TIME REGISTER \$DSN_ROOT, please run source ~/.bashrc =========="
fi

CMAKE_OPTIONS="$CMAKE_OPTIONS -DCMAKE_BUILD_TYPE=Debug"
CMAKE_OPTIONS="$CMAKE_OPTIONS -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++"
CMAKE_OPTIONS="$CMAKE_OPTIONS -DDSN_DEBUG_CMAKE=TRUE"
#CMAKE_OPTIONS="$CMAKE_OPTIONS -DWARNNING_ALL=TRUE"
#CMAKE_OPTIONS="$CMAKE_OPTIONS -DENABLE_GCOV=TRUE"

# You can specify customized boost by defining BOOST_DIR.
# Install boost like this:
#   wget http://downloads.sourceforge.net/project/boost/boost/1.54.0/boost_1_54_0.zip?r=&ts=1442891144&use_mirror=jaist
#   unzip -q boost_1_54_0.zip
#   cd boost_1_54_0
#   ./bootstrap.sh --with-libraries=system,filesystem --with-toolset=gcc
#   ./b2 toolset=gcc cxxflags="-std=c++11 -fPIC" -j8 -d0
#   ./b2 install --prefix=$DSN_ROOT -d0
# And set BOOST_DIR as:
#   export BOOST_DIR=/path/to/boost_1_54_0/ouput
if [ -n "$BOOST_DIR" ]
then
    echo "Use customized boost: $BOOST_DIR"
    CMAKE_OPTIONS="$CMAKE_OPTIONS -DBoost_NO_BOOST_CMAKE=ON -DBOOST_ROOT=$BOOST_DIR -DBoost_NO_SYSTEM_PATHS=ON"
fi

ROOT=`pwd`

##############################################
## modify $TEST_MODULE to control test modules
##  - dsn.core.tests
##  - dsn.tests
##  - dsn.rep_tests.simple_kv
##  - dsn.replication.simple_kv
##############################################
TEST_MODULE='
dsn.core.tests
dsn.tests
dsn.replication.simple_kv
'

# if clear
cd $ROOT
if [ $# -eq 1 -a "$1" == "true" ]
then
    echo "Backup gcov..."
    if [ -d "gcov" ]
    then
        rm -rf gcov.last
        mv gcov gcov.last
    fi
    echo "Clear builder..."
    if [ -d "builder" ]
    then
        rm -rf builder
    fi
fi

# if cmake
cd $ROOT
if [ ! -d "builder" ]
then
    echo "Running cmake..."
    mkdir -p builder
    cd builder
    cmake .. -DCMAKE_INSTALL_PREFIX=$DSN_ROOT $CMAKE_OPTIONS
    if [ $? -ne 0 ]
    then
        echo "cmake failed"
        exit -1
    fi
fi

# make
cd $ROOT/builder
echo "Building..."
make install -j8
if [ $? -ne 0 ]
then
    echo "build failed"
    exit -1
fi

# run tests
for NAME in $TEST_MODULE; do
    echo "====================== run $NAME =========================="

    cd $ROOT/builder/bin/$NAME
    ./run.sh
    if [ $? -ne 0 ]
    then
        echo "run $NAME failed"
        exit -1
    fi
done

