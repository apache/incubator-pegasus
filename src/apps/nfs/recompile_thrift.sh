#!/bin/bash

TMP_DIR=./tmp

../../../bin/dsn.cg.sh nfs.thrift cpp $TMP_DIR replication
echo "cp $TMP_DIR/nfs.types.h ./"
cp $TMP_DIR/nfs.types.h ./ 

rm -rf $TMP_DIR
echo "output directory '$TMP_DIR' deleted"
