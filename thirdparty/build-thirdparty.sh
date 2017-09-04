#!/bin/bash

TP_DIR=$( cd $( dirname $0 ) && pwd )
TP_SRC=$TP_DIR/src
TP_BUILD=$TP_DIR/build
TP_OUTPUT=$TP_DIR/output

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
cd $TP_SRC/concurrentqueue-1.0.0-beta
mkdir -p $TP_OUTPUT/include/concurrentqueue
cp -R blockingconcurrentqueue.h concurrentqueue.h internal/ $TP_OUTPUT/include/concurrentqueue
cd $TP_DIR

## build gflags
#mkdir -p $TP_BUILD/gflags
#cd $TP_BUILD/gflags
#cmake $TP_SRC/gflags -DCMAKE_INSTALL_PREFIX=$TP_OUTPUT
#make -j8 && make install
#cd $TP_DIR

# build gtest
mkdir -p $TP_BUILD/googletest
cd $TP_BUILD/googletest
cmake $TP_SRC/googletest-release-1.8.0
make -j8
cp -R $TP_SRC/googletest-release-1.8.0/googletest/include/gtest $TP_OUTPUT/include
cp $TP_BUILD/googletest/googlemock/gtest/libgtest.a $TP_BUILD/googletest/googlemock/gtest/libgtest_main.a $TP_OUTPUT/lib
cd $TP_DIR

# build protobuf
mkdir -p $TP_BUILD/protobuf
cd $TP_BUILD/protobuf
cmake $TP_SRC/protobuf-3.5.0/cmake -Dprotobuf_BUILD_TESTS=OFF \
    -DCMAKE_INSTALL_PREFIX=$TP_OUTPUT \
    -DCMAKE_INSTALL_LIBDIR=$TP_OUTPUT/lib \
    -DCMAKE_BUILD_TYPE=release

make -j8 && make install
cd $TP_DIR

# build rapidjson
cp -R $TP_SRC/rapidjson-1.1.0/include/rapidjson $TP_OUTPUT/include

# build thrift
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
    -DWITH_STATIC_LIB=OFF"

if [ "x"$BOOST_ROOT != "x" ]; then
    CMAKE_FLAGS="$CMAKE_FLAGS -DBOOST_ROOT=$BOOST_ROOT"
fi

echo $CMAKE_FLAGS
cmake $TP_SRC/thrift-0.9.3 $CMAKE_FLAGS

make -j8 && make install
cd $TP_DIR

# build zookeeper c client
cd $TP_SRC/zookeeper-3.4.10/src/c
./configure --enable-static=yes --enable-shared=no --prefix=$TP_OUTPUT --with-pic=yes
make -j8
make install
cd $TP_DIR
