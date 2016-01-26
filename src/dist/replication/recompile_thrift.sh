#!/bin/bash

TMP_DIR=./tmp

../../../bin/dsn.cg.sh replication.thrift cpp $TMP_DIR replication
echo "cp $TMP_DIR/replication.types.h ../../../include/dsn/dist/replication/replication.types.h"
cp $TMP_DIR/replication.types.h ../../../include/dsn/dist/replication/replication.types.h 

rm -rf $TMP_DIR
echo "output directory '$TMP_DIR' deleted"
