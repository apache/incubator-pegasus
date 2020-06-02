#!/bin/bash

# This URL corresponds to the Aliyun OSS bucket "pegasus-thirdparties" which
# stores the thirdparty packages necessary.
OSS_URL_PREFIX="http://pegasus-thirdparties.oss-cn-beijing.aliyuncs.com"

function auto_download()
{
    curl -L $1 > $2
    if [ $? -eq 0 ]; then return 0; fi
    echo "curl download $2 failed, try wget"

    wget $1 -O $2
    if [ $? -eq 0 ]; then return 0; fi
    echo "wget download $2 failed, no more download utilities to try"

    return 1
}

# check_and_download package_name url md5sum extracted_folder_name
# return:
#   0 if download and extract ok
#   1 if already downloaded
#   2 if download or extract fail
function check_and_download()
{
    package_name=$1
    url=$2
    correct_md5sum=$3
    extracted_folder_name=$4
    if [ -f $package_name ]; then
        echo "$package_name has already downloaded, skip it"

        if [ -d $extracted_folder_name ]; then
            echo "$package_name has been extracted"
            return 1
        else
            echo "extract package $package_name"
            extract_package $package_name
            local ret_code=$?
            if [ $ret_code -ne 0 ]; then
                return 2
            else
                return 0
            fi
        fi
    else
        echo "download package $package_name"
        auto_download "$url" "$package_name"
        local ret_code=$?
        if [ $ret_code -ne 0 ]; then
            rm -f $1
            echo "package $package_name download failed"
            return 2
        fi

        md5=`md5sum $1 | cut -d ' ' -f1`
        if [ "$md5"x != "$correct_md5sum"x ]; then
            #rm -f $1
            #echo "package $package_name is broken, already deleted"
            return 2
        fi

        extract_package $package_name
        local ret_code=$?
        if [ $ret_code -ne 0 ]; then
            return 2
        else
            return 0
        fi
   fi
}

function extract_package()
{
    package_name=$1
    is_tar_gz=$(echo $package_name | grep ".tar.gz")
    if [[ $is_tar_gz != "" ]]; then
        tar xf $package_name
    fi
    is_zip=$(echo $package_name | grep ".zip")
    if [[ $is_zip != "" ]]; then
        unzip -oq $package_name
    fi
    local ret_code=$?
    if [ $ret_code -ne 0 ]; then
        rm -f $package_name
        echo "extract $package_name failed, please delete the incomplete folder"
        return 2
    else
        return 0
    fi
}

function exit_if_fail()
{
    if [ $1 -eq 2 ]; then
        exit $1
    fi
}


TP_DIR=$( cd $( dirname $0 ) && pwd )
TP_SRC=$TP_DIR/src
TP_BUILD=$TP_DIR/build
TP_OUTPUT=$TP_DIR/output

if [ ! -d $TP_SRC ]; then
    mkdir $TP_SRC
fi

if [ ! -d $TP_BUILD ]; then
    mkdir $TP_BUILD
fi

if [ ! -d $TP_OUTPUT ]; then
    mkdir $TP_OUTPUT
fi

cd $TP_SRC

# concurrent queue
# from: https://codeload.github.com/cameron314/concurrentqueue/tar.gz/v1.0.1
CONCURRENT_QUEUE_NAME=concurrentqueue-1.0.1
CONCURRENT_QUEUE_PKG=${CONCURRENT_QUEUE_NAME}.tar.gz
check_and_download "${CONCURRENT_QUEUE_PKG}"\
    "${OSS_URL_PREFIX}/${CONCURRENT_QUEUE_PKG}"\
    "80016b584fddffd67073349efd7b8958"\
    "${CONCURRENT_QUEUE_NAME}"
exit_if_fail $?

# readwrite queue
# from: https://codeload.github.com/cameron314/readerwriterqueue/tar.gz/v1.0.2
READERWRITER_QUEUE_NAME=readerwriterqueue-1.0.2
READERWRITER_QUEUE_PKG=${READERWRITER_QUEUE_NAME}.tar.gz
check_and_download "${READERWRITER_QUEUE_PKG}"\
    "${OSS_URL_PREFIX}/${READERWRITER_QUEUE_PKG}"\
    "9e355a2660bd2810cb1874fb7366906e"\
    "${READERWRITER_QUEUE_NAME}"
exit_if_fail $?

# googletest
# from: https://codeload.github.com/google/googletest/tar.gz/release-1.8.0
GOOGLETEST_NAME=googletest-release-1.8.0
GOOGLETEST_PKG=${GOOGLETEST_NAME}.tar.gz
check_and_download "${GOOGLETEST_PKG}"\
    "${OSS_URL_PREFIX}/${GOOGLETEST_PKG}"\
    "16877098823401d1bf2ed7891d7dce36"\
    "${GOOGLETEST_NAME}"
exit_if_fail $?

# gperftools
# from: https://github.com/gperftools/gperftools/releases/download/gperftools-2.7/gperftools-2.7.tar.gz
GPERFTOOLS_NAME=gperftools-2.7
GPERFTOOLS_PKG=${GPERFTOOLS_NAME}.tar.gz
check_and_download "${GPERFTOOLS_PKG}"\
    "${OSS_URL_PREFIX}/${GPERFTOOLS_PKG}"\
    "c6a852a817e9160c79bdb2d3101b4601"\
    "${GPERFTOOLS_NAME}"
exit_if_fail $?

# rapidjson
# from: https://codeload.github.com/Tencent/rapidjson/tar.gz/v1.1.0
RAPIDJSON_NAME=rapidjson-1.1.0
RAPIDJSON_PKG=${RAPIDJSON_NAME}.tar.gz
check_and_download "${RAPIDJSON_PKG}"\
    "${OSS_URL_PREFIX}/${RAPIDJSON_PKG}"\
    "badd12c511e081fec6c89c43a7027bce"\
    "${RAPIDJSON_NAME}"
exit_if_fail $?

# thrift 0.9.3
# from: http://archive.apache.org/dist/thrift/0.9.3/thrift-0.9.3.tar.gz
THRIFT_NAME=thrift-0.9.3
THRIFT_PKG=${THRIFT_NAME}.tar.gz
check_and_download "${THRIFT_PKG}"\
    "${OSS_URL_PREFIX}/${THRIFT_PKG}"\
    "88d667a8ae870d5adeca8cb7d6795442"\
    "${THRIFT_NAME}"
ret_code=$?
if [ $ret_code -eq 2 ]; then
    exit 2
elif [ $ret_code -eq 0 ]; then
    echo "make patch to thrift"
    cd thrift-0.9.3
    patch -p1 < ../../fix_thrift_for_cpp11.patch
    if [ $? != 0 ]; then
        echo "ERROR: patch fix_thrift_for_cpp11.patch for thrift failed"
        exit 2
    fi
    cd ..
fi

# zookeeper c client
# from: http://ftp.jaist.ac.jp/pub/apache/zookeeper/zookeeper-3.4.10/zookeeper-3.4.10.tar.gz
ZOOKEEPER_NAME=zookeeper-3.4.10
ZOOKEEPER_PKG=${ZOOKEEPER_NAME}.tar.gz
check_and_download "${ZOOKEEPER_PKG}"\
    "${OSS_URL_PREFIX}/${ZOOKEEPER_PKG}"\
    "e4cf1b1593ca870bf1c7a75188f09678"\
    "${ZOOKEEPER_NAME}"
exit_if_fail $?

# libevent
# from: https://github.com/libevent/libevent/archive/release-2.1.8-stable.tar.gz
LIBEVENT_NAME=libevent-release-2.1.8-stable
LIBEVENT_PKG=${LIBEVENT_NAME}.tar.gz
check_and_download "${LIBEVENT_PKG}"\
    "${OSS_URL_PREFIX}/${LIBEVENT_PKG}"\
    "80f8652e4b08d2ec86a5f5eb46b74510"\
    "${LIBEVENT_NAME}"
exit_if_fail $?

# poco 1.7.8
# from: https://codeload.github.com/pocoproject/poco/tar.gz/poco-1.7.8-release
POCO_NAME=poco-poco-1.7.8-release
POCO_PKG=${POCO_NAME}.tar.gz
check_and_download "${POCO_PKG}"\
    "${OSS_URL_PREFIX}/${POCO_PKG}"\
    "4dbf02e14b9f20940ca0e8c70d8f6036"\
    "${POCO_NAME}"
exit_if_fail $?

# fds
if [ ! -d $TP_SRC/fds ]; then
    git clone https://github.com/XiaoMi/galaxy-fds-sdk-cpp.git
    if [ $? != 0 ]; then
        echo "ERROR: download fds wrong"
        exit 2
    fi
    echo "mv galaxy-fds-sdk-cpp fds"
    mv galaxy-fds-sdk-cpp fds
else
    echo "fds has already downloaded, skip it"
fi

# fmtlib
# from: https://codeload.github.com/fmtlib/fmt/tar.gz/4.0.0
FMTLIB_NAME=fmt-4.0.0
FMTLIB_PKG=${FMTLIB_NAME}.tar.gz
check_and_download "${FMTLIB_PKG}"\
    "${OSS_URL_PREFIX}/${FMTLIB_PKG}"\
    "c9be9a37bc85493d1116b0af59a25eba"\
    "${FMTLIB_NAME}"
exit_if_fail $?

# gflags
# from: https://github.com/gflags/gflags/archive/v2.2.1.zip
GFLAGS_NAME=gflags-2.2.1
GFLAGS_PKG=${GFLAGS_NAME}.zip
check_and_download "${GFLAGS_PKG}"\
    "${OSS_URL_PREFIX}/${GFLAGS_PKG}"\
    "2d988ef0b50939fb50ada965dafce96b"\
    "${GFLAGS_NAME}"
exit_if_fail $?

# civetweb
# from: https://codeload.github.com/civetweb/civetweb/tar.gz/v1.11
CIVETWEB_NAME=civetweb-1.11
CIVETWEB_PKG=${CIVETWEB_NAME}.tar.gz
check_and_download "${CIVETWEB_PKG}"\
    "${OSS_URL_PREFIX}/${CIVETWEB_PKG}"\
    "b6d2175650a27924bccb747cbe084cd4"\
    "${CIVETWEB_NAME}"
exit_if_fail $?

# prometheus-cpp
# from: https://codeload.github.com/jupp0r/prometheus-cpp/tar.gz/v0.7.0
PROMETHEUS_NAME=prometheus-cpp-0.7.0
PROMETHEUS_PKG=${PROMETHEUS_NAME}.tar.gz
check_and_download "${PROMETHEUS_PKG}"\
    "${OSS_URL_PREFIX}/${PROMETHEUS_PKG}"\
    "dc75c31ceaefd160e978365bdca8eb01"\
    "${PROMETHEUS_NAME}"
exit_if_fail $?

rm -rf $TP_SRC/prometheus-cpp-0.7.0/3rdparty/civetweb
cp -r $TP_SRC/civetweb-1.11 $TP_SRC/prometheus-cpp-0.7.0/3rdparty/
cd $TP_SRC/prometheus-cpp-0.7.0/3rdparty
mv civetweb-1.11 civetweb
cd $TP_SRC

# curl
# from: http://curl.haxx.se/download/curl-7.47.0.tar.gz
CURL_NAME=curl-7.47.0
CURL_PKG=${CURL_NAME}.tar.gz
check_and_download "${CURL_PKG}"\
    "${OSS_URL_PREFIX}/${CURL_PKG}"\
    "5109d1232d208dfd712c0272b8360393"\
    "${CURL_NAME}"
exit_if_fail $?

# nlohmann_json
# from: https://github.com/nlohmann/json/releases/download/v3.7.3/include.zip
NLOHMANN_JSON_NAME=nlohmann_json-3.7.3
NLOHMANN_JSON_PKG=${NLOHMANN_JSON_NAME}.zip
check_and_download "${NLOHMANN_JSON_PKG}" \
    "${OSS_URL_PREFIX}/${NLOHMANN_JSON_PKG}" \
    "7249387593792b565dcb30d87bca0de3" \
    "${NLOHMANN_JSON_NAME}"

# s2geometry
# from: https://github.com/google/s2geometry/archive/0239455c1e260d6d2c843649385b4fb9f5b28dba.zip
S2GEOMETRY_NAME=s2geometry-0239455c1e260d6d2c843649385b4fb9f5b28dba
S2GEOMETRY_PKG=${S2GEOMETRY_NAME}.zip
check_and_download "${S2GEOMETRY_PKG}" \
    "${OSS_URL_PREFIX}/${S2GEOMETRY_PKG}" \
    "bfa5f1c08f535a72fb2c92ec16332c64" \
    "${S2GEOMETRY_NAME}"

# rocksdb
# from: https://github.com/XiaoMi/pegasus-rocksdb/archive/v6.6.4-base.zip
ROCKSDB_NAME=pegasus-rocksdb-6.6.4-base
ROCKSDB_PKG=${ROCKSDB_NAME}.zip
check_and_download "${ROCKSDB_PKG}" \
    "${OSS_URL_PREFIX}/${ROCKSDB_PKG}" \
    "454b17946ad66e1d70ab130bb1244edf" \
    "${ROCKSDB_NAME}"

ret_code=$?
if [ $ret_code -eq 2 ]; then
    exit 2
elif [ $ret_code -eq 0 ]; then
    echo "make patch to s2geometry"
    cd s2geometry-0239455c1e260d6d2c843649385b4fb9f5b28dba
    patch -p1 < ../../fix_s2_for_pegasus.patch
    if [ $? != 0 ]; then
        echo "ERROR: patch fix_s2_for_pegasus.patch for s2geometry failed"
        exit 2
    fi
    cd ..
fi

cd $TP_DIR
