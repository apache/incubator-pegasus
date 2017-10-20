#!/bin/bash

# please install thrift before you use this package. In following steps, we will install thrift-0.9.3 from source
# 1. get the source code from http://archive.apache.org/dist/thrift/0.9.3/thrift-0.9.3.tar.gz
# 2. tar xvf thrift-0.9.3.tar.gz & cd thrift-0.9.3
# 3. ./configure --with-csharp=no --with-java=no --with-python=no --with-erlang=no --with-perl=no --with-php=no --with-ruby=no --with-haskell=no --with-php_extension=no
# 4. make && sudo make install
# 
# Note: by default the thrift will be installed in /usr/local. If you'd like to install it somewhere else, you'd better with --prefix=... in step 3
 
thrift=thrift

TMP_DIR=./gen-java
rm -rf $TMP_DIR

mkdir -p $TMP_DIR
$thrift --gen java rrdb.thrift
$thrift --gen java replication.thrift 

cp -v -r $TMP_DIR/* ../src/main/java/ 
rm -rf $TMP_DIR

echo "done"
