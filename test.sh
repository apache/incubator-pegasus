#!/bin/bash

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
    BOOST_OPTIONS="-DBoost_NO_BOOST_CMAKE=ON -DBOOST_ROOT=$BOOST_DIR -DBoost_NO_SYSTEM_PATHS=ON"
else
    BOOST_OPTIONS=""
fi

ROOT=`pwd`
BASE=`basename $ROOT`
TIME=`date --rfc-3339=seconds`
TMP=.gcov_tmp

CORE_PATTERN=`ls \
    src/core/core/*.{h,cpp} \
    src/core/dll/*.{h,cpp} \
    src/core/tools/common/*.{h,cpp} \
    src/core/tools/hpc/*.{h,cpp} \
    src/core/tools/simulator/*.{h,cpp} \
    src/dev/cpp/*.{h,cpp} \
    src/tools/cli/*.{h,cpp} \
    src/tools/svchost/*.{h,cpp} \
    include/dsn/*.h \
    include/dsn/cpp/*.h \
    include/dsn/ext/hpc-locks/*.h \
    include/dsn/internal/*.h \
    include/dsn/tool/*.h \
`

REPLICATION_PATTERN=`ls \
    src/apps/replication/lib/*.{h,cpp} \
    src/apps/replication/client_lib/*.{h,cpp} \
    src/apps/replication/meta_server/*.{h,cpp} \
    include/dsn/dist/replication/*.h \
`

########################################
## modify $PATTERN to control gcov files 
##  - CORE_PATTERN
##  - REPLICATION_PATTERN
########################################
PATTERN=""
for i in $CORE_PATTERN $REPLICATION_PATTERN
do
    PATTERN="$PATTERN *$BASE/$i"
done

##############################################
## modify $TEST_MODULE to control test modules
##  - dsn.core.tests
##  - dsn.tests
##  - dsn.rep_tests.simple_kv
##############################################
TEST_MODULE='
dsn.core.tests
dsn.tests
dsn.rep_tests.simple_kv
'

# if clear
if [ $# -eq 1 -a "$1" == "true" ]
then
    echo "Backup gcov..."
    if [ -d gcov ]
    then
        rm -rf gcov.last
        mv gcov gcov.last
    fi
    echo "Clear builder..."
    cd $ROOT
    rm -rf builder
    mkdir -p builder
    cd builder
    cmake .. -DCMAKE_INSTALL_PREFIX=`pwd`/output -DCMAKE_BUILD_TYPE=Debug $BOOST_OPTIONS \
        -DDSN_DEBUG_CMAKE=TRUE -DWARNNING_ALL=TRUE -DENABLE_GCOV=TRUE
fi

# make
cd $ROOT/builder
make install -j8
if [ $? -ne 0 ]
then
    echo "build failed"
    exit -1
fi

# lcov init
cd $ROOT
rm -rf $TMP
mkdir $TMP
lcov -q -d builder -z
lcov -q -d builder -b . --no-external --initial -c -o $TMP/initial.info
if [ $? -ne 0 ]
then
    echo "lcov generate initial failed"
    exit -1
fi
lcov -q -e $TMP/initial.info $PATTERN -o $TMP/initial.extract.info
if [ $? -ne 0 ]; then
    echo "lcov extract initial failed"
    exit -1
fi

# run tests
for NAME in $TEST_MODULE; do
    echo "====================== run $NAME =========================="

    cd $ROOT
    lcov -q -d builder -z
    if [ $? -ne 0 ]; then
        echo "lcov zerocounters $NAME failed"
        exit -1
    fi

    cd $ROOT/builder/bin/$NAME
    ./run.sh
    if [ $? -ne 0 ]
    then
        echo "run $NAME failed"
        exit -1
    fi

    cd $ROOT
    lcov -q -d builder -b . --no-external -c -o $TMP/${NAME}.info
    if [ $? -ne 0 ]; then
        echo "lcov generate $NAME failed"
        exit -1
    fi
    lcov -q -e $TMP/${NAME}.info $PATTERN -o $TMP/${NAME}.extract.info
    if [ $? -ne 0 ]; then
        echo "lcov extract $NAME failed"
        exit -1
    fi
done

# genhtml
cd $ROOT
genhtml $TMP/*.extract.info --show-details --legend --title "$TIME" -o $TMP/report
if [ $? -ne 0 ]
then
    echo "genhtml failed"
    exit -1
fi

rm -rf gcov
mv $TMP gcov
echo "succeed"
echo "view report: firefox gcov/report/index.html"
 
