#!/bin/bash
# recommand thrift-0.9.3

if [ ! -d "$DSN_ROOT" ]; then
  echo "ERROR: DSN_ROOT not set"
  exit 1
fi

TMP_DIR=./tmp
rm -rf $TMP_DIR

mkdir -p $TMP_DIR
sh $DSN_ROOT/bin/dsn.cg.sh rrdb.thrift cpp $TMP_DIR
cp -v $TMP_DIR/rrdb.types.h ../include/rrdb/
#cp -v $TMP_DIR/rrdb.code.definition.h ../include/rrdb/
#cp -v $TMP_DIR/rrdb.client.h ../include/rrdb/
#sed 's/# include "rrdb.code.definition.h"/# include <rrdb\/rrdb.code.definition.h>/' $TMP_DIR/rrdb.server.h > ../include/rrdb/rrdb.server.h
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
