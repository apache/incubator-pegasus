#!/bin/bash

# $1 package_name
# $2 return-code
function exit_if_fail()
{
    if [ $2 -ne 0 ]; then
        echo "build $1 failed"
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
    cd $TP_SRC/concurrentqueue-1.0.0-beta
    mkdir -p $TP_OUTPUT/include/concurrentqueue
    cp -R blockingconcurrentqueue.h concurrentqueue.h internal/ $TP_OUTPUT/include/concurrentqueue
    cd $TP_DIR
    exit_if_fail "concurrentqueue" $?
else
    echo "skip build concurrentqueue"
fi

## build gflags
#mkdir -p $TP_BUILD/gflags
#cd $TP_BUILD/gflags
#cmake $TP_SRC/gflags -DCMAKE_INSTALL_PREFIX=$TP_OUTPUT
#make -j8 && make install
#cd $TP_DIR

# build gtest
if [ ! -d $TP_OUTPUT/include/gtest ]; then
    mkdir -p $TP_BUILD/googletest
    cd $TP_BUILD/googletest
    cmake $TP_SRC/googletest-release-1.8.0
    make -j8
    cp -R $TP_SRC/googletest-release-1.8.0/googletest/include/gtest $TP_OUTPUT/include
    cp $TP_BUILD/googletest/googlemock/gtest/libgtest.a $TP_BUILD/googletest/googlemock/gtest/libgtest_main.a $TP_OUTPUT/lib
    cd $TP_DIR
    exit_if_fail "gtest" $?
else
    echo "skip build gtest"
fi

# build protobuf
if [ ! -d $TP_OUTPUT/include/google/protobuf ]; then
    mkdir -p $TP_BUILD/protobuf
    cd $TP_BUILD/protobuf
    cmake $TP_SRC/protobuf-3.5.0/cmake -Dprotobuf_BUILD_TESTS=OFF \
        -DCMAKE_INSTALL_PREFIX=$TP_OUTPUT \
        -DCMAKE_INSTALL_LIBDIR=$TP_OUTPUT/lib \
        -DCMAKE_BUILD_TYPE=release

    make -j8 && make install
    cd $TP_DIR
    exit_if_fail "protobuf" $?
else
    echo "skip build protobuf"
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
        -DBUILD_COMPILER=OFF\
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
    cd $TP_DIR
    exit_if_fail "thrift" $?
else
    echo "skip build thrift"
fi

# build zookeeper c client
if [ ! -d $TP_OUTPUT/include/zookeeper ]; then
    cd $TP_SRC/zookeeper-3.4.10/src/c
    ./configure --enable-static=yes --enable-shared=no --prefix=$TP_OUTPUT --with-pic=yes
    make -j8
    make install
    cd $TP_DIR
    exit_if_fail "zookeeper-c-client" $?
else
    echo "skip build zookeeper-c-client"
fi

# build libevent
if [ ! -d $TP_OUTPUT/include/event2 ]; then
    cd $TP_SRC/libevent-release-2.0.22-stable
    ./autogen.sh
    ./configure --enable-shared=no --disable-debug-mode --prefix=$TP_OUTPUT --with-pic=yes
    make -j8
    make install
    cd $TP_DIR
    exit_if_fail "libevent" $?
else
    echo "skip build libevent"
fi
