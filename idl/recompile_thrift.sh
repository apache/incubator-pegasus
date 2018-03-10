#!/bin/bash

# method to get thrift 0.11.0:
# 1. get the source code from http://mirrors.tuna.tsinghua.edu.cn/apache/thrift/0.11.0/thrift-0.11.0.tar.gz
# 2. tar xvf thrift-0.11.0.tar.gz & cd thrift-0.11.0
# 3. ./configure --with-csharp=no --with-java=no --with-python=no --with-erlang=no --with-perl=no --with-php=no --with-ruby=no --with-haskell=no --with-php_extension=no --with-rs=no
# 4. make && sudo make install
# 
# Note: by default the thrift will be installed in /usr/local. If you'd like to install it somewhere else, you'd better with --prefix=... in step 3
 
thrift=thrift

TMP_DIR=./gen-java
rm -rf $TMP_DIR

mkdir -p $TMP_DIR
$thrift --gen java rrdb.thrift
$thrift --gen java replication.thrift 

# as we pack the thrift source in our project, so we need to replace the package name
find $TMP_DIR -name "*.java" | xargs sed -i -e "s/org.apache.thrift/com.xiaomi.infra.pegasus.thrift/g"
for gen_file in `find $TMP_DIR -name "*.java"`; do
    cat apache-licence-template $gen_file > $gen_file.tmp
    mv $gen_file.tmp $gen_file
done

cp -v -r $TMP_DIR/* ../src/main/java/
rm -rf $TMP_DIR

echo "done"
