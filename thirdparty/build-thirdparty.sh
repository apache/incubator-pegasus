#!/bin/bash

# $1 package_name
# $2 return-code
function exit_if_fail()
{
    if [ $2 -ne 0 ]; then
        echo "build $1 failed"
        echo "please check the command-line output of cmake or make"
        exit $2
    fi
}

TP_DIR=$( cd $( dirname $0 ) && pwd )
TP_SRC=$TP_DIR/src
TP_BUILD=$TP_DIR/build
TP_OUTPUT=$TP_DIR/output
# explicitly annouce the compilers in case that
# a machine has low version of gcc and user install a higher version manually
export CC=gcc
export CXX=g++

CLEAR_OLD_BUILD="NO"
BOOST_ROOT=""

while [[ $# > 0 ]]; do
    case $1 in
        -c|--clear)
            CLEAR_OLD_BUILD="YES"
            ;;
        -b|--boost_root)
            BOOST_ROOT="$2"
            shift
            ;;
        *)
            echo "Error: unknown option \"$1\""
            ;;
    esac
    shift
done

if [ $CLEAR_OLD_BUILD = "YES" ]; then
    rm -rf $TP_OUTPUT/*
    rm -rf $TP_BUILD/*
fi

mkdir -p $TP_OUTPUT/include
mkdir -p $TP_OUTPUT/lib
mkdir -p $TP_OUTPUT/bin

# build concurrentqueue
if [ ! -d $TP_OUTPUT/include/concurrentqueue ]; then
    cd $TP_SRC/concurrentqueue-1.0.1
    mkdir -p $TP_OUTPUT/include/concurrentqueue
    cp -R blockingconcurrentqueue.h concurrentqueue.h lightweightsemaphore.h internal/ $TP_OUTPUT/include/concurrentqueue
    cd $TP_DIR
    exit_if_fail "concurrentqueue" $?
else
    echo "skip build concurrentqueue"
fi

# build readerwriterqueue
if [ ! -d $TP_OUTPUT/include/readerwriterqueue ]; then
    cd $TP_SRC/readerwriterqueue-1.0.2
    mkdir -p $TP_OUTPUT/include/readerwriterqueue
    cp -R atomicops.h readerwriterqueue.h $TP_OUTPUT/include/readerwriterqueue
    cd $TP_DIR
    exit_if_fail "readerwriterqueue" $?
else
    echo "skip build readerwriterqueue"
fi

# build gtest
if [ ! -d $TP_OUTPUT/include/gtest ]; then
    mkdir -p $TP_BUILD/googletest
    cd $TP_BUILD/googletest
    cmake $TP_SRC/googletest-release-1.8.0
    make -j8
    res=$?
    cp -R $TP_SRC/googletest-release-1.8.0/googletest/include/gtest $TP_OUTPUT/include
    cp $TP_BUILD/googletest/googlemock/gtest/libgtest.a $TP_BUILD/googletest/googlemock/gtest/libgtest_main.a $TP_OUTPUT/lib
    cd $TP_DIR
    exit_if_fail "gtest" $res
else
    echo "skip build gtest"
fi

# gperftools
if [ ! -f $TP_OUTPUT/lib/libtcmalloc.so ]; then
    cd $TP_SRC/gperftools-2.7
    ./configure --prefix=$TP_OUTPUT --enable-static=no --enable-frame-pointers=yes
    make -j8 && make install
    res=$?
    cd $TP_DIR
    exit_if_fail "gperftools" $res
else
    echo "skip build gperftools"
fi

# build rapidjson
if [ ! -d $TP_OUTPUT/include/rapidjson ]; then
    cp -R $TP_SRC/rapidjson-1.1.0/include/rapidjson $TP_OUTPUT/include
fi

# build thrift
if [ ! -d $TP_OUTPUT/include/thrift ]; then
    mkdir -p $TP_BUILD/thrift-0.9.3
    cd $TP_BUILD/thrift-0.9.3
    CMAKE_FLAGS="-DCMAKE_BUILD_TYPE=release\
        -DWITH_JAVA=OFF\
        -DWITH_PYTHON=OFF\
        -DWITH_C_GLIB=OFF\
        -DWITH_CPP=ON\
        -DBUILD_TESTING=OFF\
        -DBUILD_EXAMPLES=OFF\
        -DWITH_QT5=OFF\
        -DWITH_QT4=OFF\
        -DWITH_OPENSSL=OFF\
        -DBUILD_COMPILER=ON\
        -DBUILD_TUTORIALS=OFF\
        -DWITH_LIBEVENT=OFF\
        -DCMAKE_INSTALL_PREFIX=$TP_OUTPUT\
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON\
        -DWITH_SHARED_LIB=OFF"

    if [ "x"$BOOST_ROOT != "x" ]; then
        CMAKE_FLAGS="$CMAKE_FLAGS -DBOOST_ROOT=$BOOST_ROOT"
    fi

    echo $CMAKE_FLAGS
    cmake $TP_SRC/thrift-0.9.3 $CMAKE_FLAGS

    make -j8 && make install
    res=$?
    cd $TP_DIR
    exit_if_fail "thrift" $res
else
    echo "skip build thrift"
fi

# build zookeeper c client
if [ ! -d $TP_OUTPUT/include/zookeeper ]; then
    cd $TP_SRC/zookeeper-3.4.10/src/c
    ./configure --enable-static=yes --enable-shared=no --prefix=$TP_OUTPUT --with-pic=yes
    make -j8 && make install
    res=$?
    cd $TP_DIR
    exit_if_fail "zookeeper-c-client" $res
else
    echo "skip build zookeeper-c-client"
fi

# build libevent
if [ ! -d $TP_OUTPUT/include/event2 ]; then
    cd $TP_SRC/libevent-release-2.1.8-stable
    ./autogen.sh
    ./configure --enable-shared=no --disable-debug-mode --prefix=$TP_OUTPUT --with-pic=yes
    make -j8 && make install
    res=$?
    cd $TP_DIR
    exit_if_fail "libevent" $res
else
    echo "skip build libevent"
fi

# build fmtlib
if [ ! -d $TP_OUTPUT/include/fmt ]; then
    mkdir -p $TP_BUILD/fmt-4.0.0
    cd $TP_BUILD/fmt-4.0.0
    cmake $TP_SRC/fmt-4.0.0 -DCMAKE_INSTALL_PREFIX=$TP_OUTPUT -DFMT_TEST=false -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    make -j8 && make install
    cd $TP_DIR
    exit_if_fail "fmtlib" $?
else
    echo "skip build fmtlib"
fi

# build poco
if [ ! -d $TP_OUTPUT/include/Poco ]; then
    mkdir -p $TP_BUILD/poco-1.7.8-release
    cd $TP_BUILD/poco-1.7.8-release
    CMAKE_FLAGS="
    -DENABLE_MONGODB=OFF\
    -DENABLE_PDF=OFF\
    -DENABLE_DATA=OFF\
    -DENABLE_DATA_SQLITE=OFF\
    -DENABLE_DATA_MYSQL=OFF\
    -DENABLE_DATA_ODBC=OFF\
    -DENABLE_SEVENZIP=OFF\
    -DENABLE_ZIP=OFF\
    -DENABLE_APACHECONNECTOR=OFF\
    -DENABLE_CPPPARSER=OFF\
    -DENABLE_POCODOC=OFF\
    -DENABLE_PAGECOMPILER=OFF\
    -DENABLE_PAGECOMPILER_FILE2PAGE=OFF\
    -DCMAKE_INSTALL_PREFIX=$TP_OUTPUT\
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON"
    #-DPOCO_STATIC=1"

    if [ "x"$BOOST_ROOT != "x" ]; then
        CMAKE_FLAGS="$CMAKE_FLAGS -DBOOST_ROOT=$BOOST_ROOT"
    fi

    echo $CMAKE_FLAGS
    cmake $TP_SRC/poco-poco-1.7.8-release $CMAKE_FLAGS
    make -j8 && make install
    res=$?
    cd $TP_DIR
    exit_if_fail "poco" $res
else
    echo "skip build Poco"
fi

# build fds
if [ ! -d $TP_OUTPUT/include/fds ]; then
    if [ ! -d $TP_OUTPUT/include/Poco -o ! -d $TP_OUTPUT/include/gtest ]; then
        echo "please build poco or gtest first"
        exit
    fi
    # when build fds, we need poco, gtest
    POCO_INCLUDE_DIR=$TP_OUTPUT/include
    POCO_LIB_DIR=$TP_OUTPUT/lib
    GTEST_INCLUDE_DIR=$TP_OUTPUT/include
    GTEST_LIB_DIR=$TP_OUTPUT/lib

    mkdir -p $TP_BUILD/fds
    cd $TP_BUILD/fds

    CMAKE_FLAGS="-DPOCO_INCLUDE=${POCO_INCLUDE_DIR}\
    -DPOCO_LIB=${POCO_LIB_DIR}\
    -DGTEST_INCLUDE=${GTEST_INCLUDE_DIR}\
    -DGTEST_LIB=${GTEST_LIB_DIR}\
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON"

    if [ "x"$BOOST_ROOT != "x" ]; then
        CMAKE_FLAGS="$CMAKE_FLAGS -DBOOST_ROOT=$BOOST_ROOT"
    fi

    echo $CMAKE_FLAGS
    cmake $TP_SRC/fds $CMAKE_FLAGS
    make -j8
    res=$?
    exit_if_fail "fds" $res
    mkdir -p $TP_OUTPUT/include/fds
    cd $TP_OUTPUT/include/fds
    cp -r  $TP_SRC/fds/include/* ./
    cd $TP_OUTPUT/lib
    cp $TP_BUILD/fds/libgalaxy-fds-sdk-cpp.a ./
    cd $TP_DIR
else
    echo "skip build fds"
fi

# build s2geometry
if [ ! -d $TP_OUTPUT/include/s2 ]; then
    mkdir -p  $TP_BUILD/s2geometry
    cd $TP_BUILD/s2geometry
    cmake $TP_SRC/s2geometry-0239455c1e260d6d2c843649385b4fb9f5b28dba -DGTEST_INCLUDE=$TP_OUTPUT/include -DCMAKE_INSTALL_PREFIX=$TP_OUTPUT
    make -j8 && make install
    res=$?
    exit_if_fail "s2geometry" $res
    cd $TP_DIR
else
    echo "skip build s2geometry"
fi

# build gflags
if [ ! -d $TP_OUTPUT/include/gflags ]; then
    mkdir -p $TP_BUILD/gflags-2.2.1
    cd $TP_BUILD/gflags-2.2.1
    cmake $TP_SRC/gflags-2.2.1 -DCMAKE_INSTALL_PREFIX=$TP_OUTPUT
    make -j8 && make install
    res=$?
    exit_if_fail "gflags" $res
    cd $TP_DIR
else
    echo "skip build gflags"
fi

#build curl
if [ ! -d $TP_OUTPUT/include/curl ]; then
    cd $TP_SRC/curl-7.47.0
    CONFIG_FLAGS="--prefix=$TP_OUTPUT \
    --disable-dict \
    --disable-file \
    --disable-ftp \
    --disable-gopher \
    --disable-imap \
    --disable-ipv6 \
    --disable-ldap \
    --disable-ldaps \
    --disable-manual \
    --disable-pop3 \
    --disable-rtsp \
    --disable-smtp \
    --disable-telnet \
    --disable-tftp \
    --disable-shared \
    --without-librtmp \
    --without-zlib \
    --without-libssh2 \
    --without-ssl \
    --without-libidn"

    ./configure $CONFIG_FLAGS
    make -j8 && make install
    res=$?
    cd $TP_DIR
    exit_if_fail "curl" $res
else
    echo "skip build curl"
fi

#build prometheus-cpp
if [ ! -d $TP_OUTPUT/include/prometheus ]; then
    mkdir -p $TP_BUILD/prometheus
    cd $TP_BUILD/prometheus
    cmake $TP_SRC/prometheus-cpp-0.7.0 -DCMAKE_INSTALL_PREFIX=$TP_OUTPUT -DENABLE_TESTING=OFF
    make -j8 && make install
    res=$?
    exit_if_fail "prometheus-cpp" $res
    cd $TP_DIR
else
    echo "skip build prometheus-cpp"
fi

#build nlohmann_json
if [ ! -d $TP_OUTPUT/include/nlohmann ]; then
    mv $TP_SRC/nlohmann_json-3.7.3/include/nlohmann $TP_OUTPUT/include
else
    echo "skip build nlohmann_json"
fi

#build rocksdb
if [ ! -d $TP_OUTPUT/include/rocksdb ]; then
    mkdir -p $TP_BUILD/rocksdb
    cd $TP_BUILD/rocksdb
    cmake $TP_SRC/pegasus-rocksdb-6.6.4-compatible -DCMAKE_INSTALL_PREFIX=$TP_OUTPUT \
                                                   -DWITH_LZ4=ON \
                                                   -DWITH_ZSTD=ON \
                                                   -DWITH_SNAPPY=ON \
                                                   -DWITH_BZ2=OFF \
                                                   -DWITH_TESTS=OFF \
                                                   -DWITH_GFLAGS=OFF \
                                                   -DUSE_RTTI=ON \
                                                   -DCMAKE_BUILD_TYPE=Release \
                                                   -DCMAKE_CXX_FLAGS=-g
    #rocksdb enable jemalloc by default, but we use regular malloc.
    make install -j4 DISABLE_JEMALLOC=1
else
    echo "skip build rocksdb"
fi
