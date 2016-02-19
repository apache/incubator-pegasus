#!/bin/bash

TMP_DIR=./tmp
PRE=""

if [ $# -ne 1 ]; then
    echo "usage: ./recompile_thrift.sh <prefix>"
    exit -1
fi

PRE=$1

../../../bin/dsn.cg.sh ${PRE}.thrift cpp $TMP_DIR replication
echo "cp $TMP_DIR/${PRE}.types.h ./"
cp $TMP_DIR/${PRE}.types.h ./ 

rm -rf $TMP_DIR
echo "output directory '$TMP_DIR' deleted"

echo "generate thrift types"
../../../bin/Linux/thrift -r --gen cpp ${PRE}.thrift
cp gen-cpp/${PRE}_types.h ./
cp gen-cpp/${PRE}_types.cpp ./
echo "done"
rm -rf gen-cpp
