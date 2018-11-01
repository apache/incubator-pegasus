#!/bin/bash
# recommand thrift-0.9.3

cd `dirname $0`
DSN_ROOT=../../rdsn

if [ ! -d "$DSN_ROOT" ]; then
  echo "ERROR: DSN_ROOT not set"
  exit 1
fi

TMP_DIR=./tmp
rm -rf $TMP_DIR

mkdir -p $TMP_DIR
$DSN_ROOT/bin/Linux/thrift --gen cpp:moveable_types -out $TMP_DIR rrdb.thrift

sed 's/#include "dsn_types.h"/#include <dsn\/service_api_cpp.h>/' $TMP_DIR/rrdb_types.h > ../include/rrdb/rrdb_types.h
sed 's/#include "rrdb_types.h"/#include <rrdb\/rrdb_types.h>/' $TMP_DIR/rrdb_types.cpp > ../base/rrdb_types.cpp

rm -rf $TMP_DIR

echo
echo "You should manually modify these files:"
echo "  src/include/rrdb/rrdb.code.definition.h"
echo "  src/include/rrdb/rrdb.client.h"
echo "  src/include/rrdb/rrdb.server.h"
echo
echo "done"
