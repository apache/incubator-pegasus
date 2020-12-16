project(pegasus_thirdparties)
cmake_minimum_required(VERSION 3.0.2)

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    # require at least gcc 4.8
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.8)
        message(FATAL_ERROR "GCC version must be at least 4.8!")
    endif ()
endif ()

include(ExternalProject)

set(TP_DIR ${PROJECT_SOURCE_DIR})
set(TP_OUTPUT ${PROJECT_SOURCE_DIR}/output)

# TMP_DIR      = <base>/tmp/<name>
# STAMP_DIR    = <base>/Stamp/<name>
# DOWNLOAD_DIR = <base>/Download/<name>
# SOURCE_DIR   = <base>/Source/<name>
# BINARY_DIR   = <base>/Build/<name>
# INSTALL_DIR  = <base>/Install/<name>
# LOG_DIR      = <STAMP_DIR>
set_property(DIRECTORY PROPERTY EP_BASE ${TP_DIR}/build)

set(OSS_URL_PREFIX "http://pegasus-thirdparties.oss-cn-beijing.aliyuncs.com")

message(STATUS "Setting up third-parties...")

ExternalProject_Add(boost
        URL ${OSS_URL_PREFIX}/boost_1_69_0.tar.bz2
        https://dl.bintray.com/boostorg/release/1.69.0/source/boost_1_69_0.tar.bz2
        URL_MD5 a1332494397bf48332cb152abfefcec2
        CONFIGURE_COMMAND ./bootstrap.sh --prefix=. --with-libraries=system,filesystem,regex --with-toolset=gcc
        BUILD_COMMAND ./b2 toolset=gcc cxxflags=-fPIC cxxstd=11 install
        INSTALL_COMMAND cp -R include/boost ${TP_OUTPUT}/include && cp -R lib ${TP_OUTPUT}/
        BUILD_IN_SOURCE 1
        )

# header-only
file(MAKE_DIRECTORY ${TP_OUTPUT}/include/concurrentqueue)
ExternalProject_Add(concurrentqueue
        URL ${OSS_URL_PREFIX}/concurrentqueue-1.0.1.tar.gz
        https://codeload.github.com/cameron314/concurrentqueue/tar.gz/v1.0.1
        URL_MD5 80016b584fddffd67073349efd7b8958
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND cp -R blockingconcurrentqueue.h concurrentqueue.h lightweightsemaphore.h internal/ ${TP_OUTPUT}/include/concurrentqueue
        BUILD_IN_SOURCE 1
        )

# header-only
file(MAKE_DIRECTORY ${TP_OUTPUT}/include/readerwriterqueue)
ExternalProject_Add(readerwriterqueue
        URL ${OSS_URL_PREFIX}/readerwriterqueue-1.0.2.tar.gz
        https://codeload.github.com/cameron314/readerwriterqueue/tar.gz/v1.0.2
        URL_MD5 9e355a2660bd2810cb1874fb7366906e
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND cp -R atomicops.h readerwriterqueue.h ${TP_OUTPUT}/include/readerwriterqueue
        BUILD_IN_SOURCE 1
        )

ExternalProject_Add(googletest
        URL ${OSS_URL_PREFIX}/googletest-release-1.8.0.tar.gz
        https://codeload.github.com/google/googletest/tar.gz/release-1.8.0
        URL_MD5 16877098823401d1bf2ed7891d7dce36
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${TP_OUTPUT}
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        )
ExternalProject_Get_property(googletest SOURCE_DIR)
set(googletest_SRC ${SOURCE_DIR})

ExternalProject_Add(gperftools
        URL ${OSS_URL_PREFIX}/gperftools-2.7.tar.gz
        https://github.com/gperftools/gperftools/releases/download/gperftools-2.7/gperftools-2.7.tar.gz
        URL_MD5 c6a852a817e9160c79bdb2d3101b4601
        CONFIGURE_COMMAND ./configure --prefix=${TP_OUTPUT} --enable-static=no --enable-frame-pointers=yes
        BUILD_IN_SOURCE 1
        )

file(MAKE_DIRECTORY ${TP_OUTPUT}/include/hdfs)
ExternalProject_Add(hadoop
        URL ${OSS_URL_PREFIX}/hadoop-2.8.4.tar.gz
        https://archive.apache.org/dist/hadoop/common/hadoop-2.8.4/hadoop-2.8.4.tar.gz
        URL_MD5 b30b409bb69185003b3babd1504ba224
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND cp -R include/. ${TP_OUTPUT}/include/hdfs && cp -R lib/native/. ${TP_OUTPUT}/lib
        BUILD_IN_SOURCE 1
        )

# header-only
ExternalProject_Add(rapidjson
        URL ${OSS_URL_PREFIX}/rapidjson-1.1.0.tar.gz
        https://codeload.github.com/Tencent/rapidjson/tar.gz/v1.1.0
        URL_MD5 badd12c511e081fec6c89c43a7027bce
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND cp -R include/rapidjson ${TP_OUTPUT}/include
        BUILD_IN_SOURCE 1
        )

ExternalProject_Add(thrift
        URL ${OSS_URL_PREFIX}/thrift-0.9.3.tar.gz
        http://archive.apache.org/dist/thrift/0.9.3/thrift-0.9.3.tar.gz
        URL_MD5 88d667a8ae870d5adeca8cb7d6795442
        PATCH_COMMAND patch -p1 < ${TP_DIR}/fix_thrift_for_cpp11.patch
        CMAKE_ARGS -DCMAKE_BUILD_TYPE=release
        -DWITH_JAVA=OFF
        -DWITH_PYTHON=OFF
        -DWITH_C_GLIB=OFF
        -DWITH_CPP=ON
        -DBUILD_TESTING=OFF
        -DBUILD_EXAMPLES=OFF
        -DWITH_QT5=OFF
        -DWITH_QT4=OFF
        -DWITH_OPENSSL=OFF
        -DBUILD_COMPILER=ON
        -DBUILD_TUTORIALS=OFF
        -DWITH_LIBEVENT=OFF
        -DCMAKE_INSTALL_PREFIX=${TP_OUTPUT}
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DWITH_SHARED_LIB=OFF
        -DBOOST_ROOT=${TP_OUTPUT}
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        DEPENDS boost
        )

ExternalProject_Add(zookeeper
        URL ${OSS_URL_PREFIX}/zookeeper-3.4.10.tar.gz
        http://ftp.jaist.ac.jp/pub/apache/zookeeper/zookeeper-3.4.10/zookeeper-3.4.10.tar.gz
        URL_MD5 e4cf1b1593ca870bf1c7a75188f09678
        CONFIGURE_COMMAND cd src/c && CFLAGS=-Wno-error=format ./configure --enable-static=yes --enable-shared=no --prefix=${TP_OUTPUT} --with-pic=yes
        BUILD_COMMAND cd src/c && make
        INSTALL_COMMAND cd src/c && make install
        BUILD_IN_SOURCE 1
        )

ExternalProject_Add(libevent
        URL ${OSS_URL_PREFIX}/libevent-release-2.1.8-stable.tar.gz
        https://github.com/libevent/libevent/archive/release-2.1.8-stable.tar.gz
        URL_MD5 80f8652e4b08d2ec86a5f5eb46b74510
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${TP_OUTPUT}
        -DEVENT__DISABLE_DEBUG_MODE=On
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        )

ExternalProject_Add(poco
        URL ${OSS_URL_PREFIX}/poco-poco-1.7.8-release.tar.gz
        https://codeload.github.com/pocoproject/poco/tar.gz/poco-1.7.8-release
        URL_MD5 4dbf02e14b9f20940ca0e8c70d8f6036
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${TP_OUTPUT}
        -DENABLE_MONGODB=OFF
        -DENABLE_PDF=OFF
        -DENABLE_DATA=OFF
        -DENABLE_DATA_SQLITE=OFF
        -DENABLE_DATA_MYSQL=OFF
        -DENABLE_DATA_ODBC=OFF
        -DENABLE_SEVENZIP=OFF
        -DENABLE_ZIP=OFF
        -DENABLE_APACHECONNECTOR=OFF
        -DENABLE_CPPPARSER=OFF
        -DENABLE_POCODOC=OFF
        -DENABLE_PAGECOMPILER=OFF
        -DENABLE_PAGECOMPILER_FILE2PAGE=OFF
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DBOOST_ROOT=${TP_OUTPUT}
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        DEPENDS boost
        )

ExternalProject_Add(fds
        GIT_REPOSITORY https://github.com/XiaoMi/galaxy-fds-sdk-cpp.git
        GIT_TAG 2d813e0f0d1bae871f584030101366bdf29dc36c
        CMAKE_ARGS -DPOCO_INCLUDE=${TP_OUTPUT}/include
        -DPOCO_LIB=${TP_OUTPUT}/lib
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DBOOST_ROOT=${TP_OUTPUT}
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        INSTALL_COMMAND cp libgalaxy-fds-sdk-cpp.a ${TP_OUTPUT}/lib
        COMMAND rm -rf ${TP_OUTPUT}/include/fds
        COMMAND cp -r include fds-include
        COMMAND mv fds-include ${TP_OUTPUT}/include/fds # install fds headers into a stand-alone directory
        UPDATE_COMMAND "" # do not update
        DEPENDS googletest poco boost
        BUILD_IN_SOURCE 1
        )

# fmtlib >=6.x requires c++14 support, do not update this library for now
ExternalProject_Add(fmt
        URL ${OSS_URL_PREFIX}/fmt-5.3.0.zip
        https://github.com/fmtlib/fmt/releases/download/5.3.0/fmt-5.3.0.zip
        URL_MD5 56717619dcd9fa8c470533eb4d7d33aa
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${TP_OUTPUT}
        -DCMAKE_BUILD_TYPE=release
        -DFMT_TEST=false
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        )

ExternalProject_Add(gflags
        URL ${OSS_URL_PREFIX}/gflags-2.2.1.zip
        https://github.com/gflags/gflags/archive/v2.2.1.zip
        URL_MD5 2d988ef0b50939fb50ada965dafce96b
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${TP_OUTPUT}
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        )

# civetweb is one of the dependencies of promemetheus-cpp, do not build & install
ExternalProject_Add(civetweb
        URL ${OSS_URL_PREFIX}/civetweb-1.11.tar.gz
        https://codeload.github.com/civetweb/civetweb/tar.gz/v1.11
        URL_MD5 b6d2175650a27924bccb747cbe084cd4
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
        )
ExternalProject_Get_property(civetweb SOURCE_DIR)
set(civetweb_SRC ${SOURCE_DIR})

ExternalProject_Add(curl
        URL ${OSS_URL_PREFIX}/curl-7.47.0.tar.gz
        http://curl.haxx.se/download/curl-7.47.0.tar.gz
        URL_MD5 5109d1232d208dfd712c0272b8360393
        CONFIGURE_COMMAND ./configure --prefix=${TP_OUTPUT}
        --disable-dict
        --disable-file
        --disable-ftp
        --disable-gopher
        --disable-imap
        --disable-ipv6
        --disable-ldap
        --disable-ldaps
        --disable-manual
        --disable-pop3
        --disable-rtsp
        --disable-smtp
        --disable-telnet
        --disable-tftp
        --disable-shared
        --without-librtmp
        --without-zlib
        --without-libssh2
        --without-ssl
        --without-libidn
        BUILD_IN_SOURCE 1
        )

ExternalProject_Add(prometheus-cpp
        URL ${OSS_URL_PREFIX}/prometheus-cpp-0.7.0.tar.gz
        https://codeload.github.com/jupp0r/prometheus-cpp/tar.gz/v0.7.0
        URL_MD5 dc75c31ceaefd160e978365bdca8eb01
        DEPENDS civetweb curl
        PATCH_COMMAND rm -rf 3rdparty/civetweb && cp -R ${civetweb_SRC} 3rdparty/civetweb # replace the submodule
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${TP_OUTPUT}
        -DENABLE_TESTING=OFF
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        )

# header-only
ExternalProject_Add(nlohmann_json
        URL ${OSS_URL_PREFIX}/nlohmann_json-3.9.1.zip
        https://github.com/nlohmann/json/releases/download/v3.9.1/include.zip
        URL_MD5 d2f66c608af689e21d69a33c220e974e
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND cp -R include/nlohmann ${TP_OUTPUT}/include
        BUILD_IN_SOURCE 1
        )

ExternalProject_Add(s2geometry
        URL ${OSS_URL_PREFIX}/s2geometry-e8d16637a467d9f096a92a6d81a50a9b747ca828.zip
        https://github.com/neverchanje/s2geometry/archive/e8d16637a467d9f096a92a6d81a50a9b747ca828.zip
        URL_MD5 75cc44c9c31382083d8a2d0e42590788
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${TP_OUTPUT}
        -DGTEST_ROOT=${googletest_SRC}/googletest
        -DBUILD_SHARED_LIBS=OFF
        -DBUILD_PYTHON=OFF
        -DBUILD_TESTING=OFF
        -DBUILD_EXAMPLES=OFF
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        DEPENDS googletest
        )

ExternalProject_Add(rocksdb
        URL ${OSS_URL_PREFIX}/pegasus-rocksdb-6.6.4-compatible.zip
        https://github.com/XiaoMi/pegasus-rocksdb/archive/v6.6.4-compatible.zip
        URL_MD5 595b21fbe681dcf126c4cccda46f1cbb
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${TP_OUTPUT}
        -DWITH_LZ4=ON
        -DWITH_ZSTD=ON
        -DWITH_SNAPPY=ON
        -DWITH_BZ2=OFF
        -DWITH_TESTS=OFF
        -DWITH_GFLAGS=OFF
        -DUSE_RTTI=ON
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        )

# kerberos
ExternalProject_Add(krb5
        URL ${OSS_URL_PREFIX}/krb5-1.16.1.tar.gz
        http://web.mit.edu/kerberos/dist/krb5/1.16/krb5-1.16.1.tar.gz
        URL_MD5 848e9b80d6aaaa798e3f3df24b83c407
        CONFIGURE_COMMAND cd src && ./configure --prefix=${TP_OUTPUT}
        BUILD_COMMAND cd src && make
        INSTALL_COMMAND cd src && make install
        BUILD_IN_SOURCE 1
        )

# cyrus-sasl
ExternalProject_Add(cyrus-sasl
        URL ${OSS_URL_PREFIX}/cyrus-sasl-2.1.27.tar.gz
        http://www.cyrusimap.org/releases/cyrus-sasl-2.1.27.tar.gz
        URL_MD5 a33820c66e0622222c5aefafa1581083
        CONFIGURE_COMMAND ./configure --prefix=${TP_OUTPUT}
        --enable-gssapi=${TP_OUTPUT}
        --enable-scram=no
        --enable-digest=no
        --enable-cram=no
        --enable-otp=no
        BUILD_COMMAND make
        INSTALL_COMMAND make install
        BUILD_IN_SOURCE 1
        )
add_dependencies(cyrus-sasl krb5)

ExternalProject_Add(http-parser
        URL ${OSS_URL_PREFIX}/http-parser-2.9.4.zip
        https://github.com/nodejs/http-parser/archive/v2.9.4.zip
        URL_MD5 f6900b9209d3d6b80c70e050ac33b834
        CONFIGURE_COMMAND mkdir -p ${TP_OUTPUT}/include/nodejs
        BUILD_COMMAND ""
        INSTALL_COMMAND cp -R http_parser.h ${TP_OUTPUT}/include/nodejs/http_parser.h
        BUILD_IN_SOURCE 1
        )
