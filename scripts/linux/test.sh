#!/bin/bash
# !!! This script should be run in dsn project root directory (../../).
#
# Options:
#    WARNING_ALL    YES|NO
#    ENABLE_GCOV    YES|NO
#    VERBOSE        YES|NO
#    CLEAR          YES|NO
#    BOOST_DIR      <dir>|""
#    TEST_MODULE    "<module1> <module2> ..."

ROOT=`pwd`
BUILD_DIR="$ROOT/builder_for_test"
GCOV_DIR="$ROOT/gcov_report"
GCOV_TMP="$ROOT/.gcov_tmp"
GCOV_PATTERN=`find $ROOT/include $ROOT/src -name '*.h' -o -name '*.cpp'`
TIME=`date --rfc-3339=seconds`
CMAKE_OPTIONS="$CMAKE_OPTIONS -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ -DCMAKE_BUILD_TYPE=Debug"

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

if [ "$VERBOSE" == "YES" ]
then
    echo "VERBOSE=YES"
    CMAKE_OPTIONS="$CMAKE_OPTIONS -DDSN_DEBUG_CMAKE=TRUE"
else
    echo "VERBOSE=NO"
fi

if [ "$CLEAR" == "YES" ]
then
    echo "CLEAR=YES"
else
    echo "CLEAR=NO"
fi

##############################################
## Supported test module:
##  - dsn.core.tests
##  - dsn.tests
##  - dsn.rep_tests.simple_kv
##  - dsn.replication.simple_kv
##############################################
if [ -z "$TEST_MODULE" ]
then
    TEST_MODULE='
        dsn.core.tests
        dsn.tests
        dsn.replication.simple_kv
    '
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
make install -j8
if [ $? -ne 0 ]
then
    echo "ERROR: build failed"
    exit -1
else
    echo "Build succeed"
fi
cd ..

if [ "$ENABLE_GCOV" == "YES" ]
then
    echo "Initializing gcov..."
    cd $ROOT
    rm -rf $GCOV_TMP &>/dev/null
    mkdir -p $GCOV_TMP
    lcov -q -d $BUILD_DIR -z
    lcov -q -d $BUILD_DIR -b $ROOT --no-external --initial -c -q -o $GCOV_TMP/initial.info
    if [ $? -ne 0 ]
    then
        echo "ERROR: lcov init failed, maybe need to run again with --clear option"
        exit -1
    fi
    lcov -q -e $GCOV_TMP/initial.info $GCOV_PATTERN -q -o $GCOV_TMP/initial.extract.info
    if [ $? -ne 0 ]
    then
        echo "ERROR: lcov init extract failed"
        exit -1
    fi
fi

for MODULE in $TEST_MODULE; do
    echo "====================== run $MODULE =========================="
    cd $BUILD_DIR/bin/$MODULE
    ./run.sh
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
    lcov -q -d $BUILD_DIR -b $ROOT --no-external -c -q -o $GCOV_TMP/gcov.info
    if [ $? -ne 0 ]
    then
        echo "ERROR: lcov generate failed"
        exit -1
    fi
    lcov -q -e $GCOV_TMP/gcov.info $GCOV_PATTERN -q -o $GCOV_TMP/gcov.extract.info
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

