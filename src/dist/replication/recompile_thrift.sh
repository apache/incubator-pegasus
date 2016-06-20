#!/bin/bash

TMP_DIR=./tmp

../../../bin/dsn.cg.sh replication.thrift cpp $TMP_DIR

echo "cp $TMP_DIR/replication.types.h ../../../include/dsn/dist/replication/replication.types.h"
cp $TMP_DIR/replication.types.h ../../../include/dsn/dist/replication/replication.types.h 

echo "cp $TMP_DIR/thrift/replication_types.h ../../../include/dsn/dist/replication/replication_types.h"
cp $TMP_DIR/thrift/replication_types.h ../../../include/dsn/dist/replication/

echo "cp $TMP_DIR/thrift/replication_types.cpp ./client_lib/replication_types.cpp"
cp $TMP_DIR/thrift/replication_types.cpp ./client_lib/

rm -rf $TMP_DIR
echo "output directory '$TMP_DIR' deleted"
