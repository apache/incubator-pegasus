#!/bin/sh

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
'

# check BOOST_DIR
if [ -z "$BOOST_DIR" ]
then
    echo 'BOOST_DIR not set'
    echo 'You can install boost by this:'
    echo '  wget http://downloads.sourceforge.net/project/boost/boost/1.54.0/boost_1_54_0.zip?r=&ts=1442891144&use_mirror=jaist'
    echo '  unzip -q boost_1_54_0.zip'
    echo '  cd boost_1_54_0'
    echo '  ./bootstrap.sh --with-libraries=system,filesystem --with-toolset=gcc'
    echo '  ./b2 toolset=gcc cxxflags="-std=c++11 -fPIC" -j8 -d0'
    echo '  ./b2 install --prefix=`pwd`/output -d0'
    echo 'And set BOOST_DIR as:'
    echo '  export BOOST_DIR=/path/to/boost_1_54_0/output'
    exit -1
fi

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
    cmake .. -DCMAKE_INSTALL_PREFIX=`pwd`/output -DCMAKE_BUILD_TYPE=Debug \
        -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ \
        -DBoost_NO_BOOST_CMAKE=ON -DBOOST_ROOT=$BOOST_DIR -DBoost_NO_SYSTEM_PATHS=ON \
        -DDSN_LINK_WRAPPER=TRUE -DDSN_DEBUG_CMAKE=TRUE \
        -DWARNNING_ALL=TRUE -DENABLE_GCOV=TRUE
fi

# make
cd $ROOT/builder
make -j4
if [ $? -ne 0 ]
then
    echo "make failed"
    exit -1
fi
make install
if [ $? -ne 0 ]
then
    echo "make install failed"
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
 
