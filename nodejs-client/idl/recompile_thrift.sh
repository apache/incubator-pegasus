#!/bin/bash

# complier require: thrift 0.10.0
 
thrift=thrift

if ! $thrift -version | grep '0.10.0' 
then
    echo "Should use thrift 0.10.0"
    exit -1
fi

TMP_DIR=./gen-nodejs
rm -rf $TMP_DIR

mkdir -p $TMP_DIR
$thrift --gen js:node rrdb.thrift
$thrift --gen js:node replication.thrift 
$thrift --gen js:node base.thrift

cp -v -r $TMP_DIR/* ../dsn
rm -rf $TMP_DIR

echo "done"
