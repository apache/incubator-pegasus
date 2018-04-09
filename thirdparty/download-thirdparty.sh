#!/bin/bash

# check_and_download package_name url
# return:
#   1 if already downloaded
#   0 if download and extract ok
#  -1 if download or extract fail
function check_and_download()
{
    package_name=$1
    url=$2
    if [ -f $package_name ]; then
        echo "$package_name has already downloaded, skip it"
        return 1
    else
        echo "download package $package_name"
        curl $url > $package_name
        tar xf $package_name
        local ret_code=$?
        if [ $ret_code -ne 0 ]; then
            return -1
        else
            return 0
        fi
    fi
}

function exit_if_fail()
{
    if [ $1 -eq -1 ]; then
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
check_and_download "concurrentqueue-v1.0.0-beta.tar.gz" "https://codeload.github.com/cameron314/concurrentqueue/tar.gz/v1.0.0-beta"
exit_if_fail $?

# googletest
check_and_download "googletest-1.8.0.tar.gz" "https://codeload.github.com/google/googletest/tar.gz/release-1.8.0"
exit_if_fail $?

## protobuf
#check_and_download "protobuf-v3.5.0.tar.gz" "https://codeload.github.com/google/protobuf/tar.gz/v3.5.0"
#exit_if_fail $?

#rapidjson
check_and_download "rapidjson-v1.1.0.tar.gz" "https://codeload.github.com/Tencent/rapidjson/tar.gz/v1.1.0"

# thrift 0.9.3
check_and_download "thrift-0.9.3.tar.gz" "http://archive.apache.org/dist/thrift/0.9.3/thrift-0.9.3.tar.gz"
ret_code=$?
if [ $ret_code -eq -1 ]; then
    exit -1
elif [ $ret_code -eq 0 ]; then
    echo "make patch to thrift"
    cd thrift-0.9.3
    patch -p1 < ../../fix_thrift_for_cpp11.patch
    if [ $? != 0 ]; then
        echo "ERROR: patch fix_thrift_for_cpp11.patch for thrift failed"
        exit -1
    fi
    cd ..
fi

# use zookeeper c client
check_and_download "zookeeper-3.4.10.tar.gz" "https://mirrors.tuna.tsinghua.edu.cn/apache/zookeeper/stable/zookeeper-3.4.10.tar.gz"
exit_if_fail $?

# libevent for send http request
check_and_download "libevent-2.0.22.tar.gz" "https://codeload.github.com/libevent/libevent/tar.gz/release-2.0.22-stable"
exit_if_fail $?

# poco 1.8.0.1-all
check_and_download "poco-1.7.8.tar.gz" "https://codeload.github.com/pocoproject/poco/tar.gz/poco-1.7.8-release"
exit_if_fail $?

# fds
if [ ! -d $TP_SRC/fds ]; then
    git clone https://github.com/XiaoMi/galaxy-fds-sdk-cpp.git
    if [ $? != 0 ]; then
        echo "ERROR: download fds wrong"
        exit -1
    fi
    echo "mv galaxy-fds-sdk-cpp fds"
    mv galaxy-fds-sdk-cpp fds
else
    echo "fds has already downloaded, skip it"
fi

# fmtlib
check_and_download "fmt-4.0.0.tar.gz" "https://codeload.github.com/fmtlib/fmt/tar.gz/4.0.0"
exit_if_fail $?

cd $TP_DIR
