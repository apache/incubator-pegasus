#!/bin/bash

# method to get thrift 0.11.0:
#   wget http://mirrors.tuna.tsinghua.edu.cn/apache/thrift/0.11.0/thrift-0.11.0.tar.gz
#   tar xvf thrift-0.11.0.tar.gz
#   cd thrift-0.11.0
#   ./configure --with-csharp=no --with-java=no --with-python=no --with-erlang=no --with-perl=no \
#       --with-php=no --with-ruby=no --with-haskell=no --with-php_extension=no --with-rs=no --with-go=no
#   make
#   sudo make install
# 
# Note: by default the thrift will be installed in /usr/local. If you'd like to install it somewhere else,
# you'd better provide install path with option `--prefix=xxx'.
 
thrift=thrift
if ! $thrift -version | grep "0.11.0" ; then
    echo "ERROR: thrift version should be 0.11.0"
    exit 1
fi

TMP_DIR=./gen-java
rm -rf $TMP_DIR

mkdir -p $TMP_DIR
$thrift --gen java rrdb.thrift
$thrift --gen java replication.thrift 
$thrift --gen java security.thrift 

for gen_file in `find $TMP_DIR -name "*.java"`; do
    cat apache-licence-template $gen_file > $gen_file.tmp
    mv $gen_file.tmp $gen_file
done

cp -v -r $TMP_DIR/* ../src/main/java/
rm -rf $TMP_DIR

echo "done"
