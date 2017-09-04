TP_DIR=$( cd $( dirname $0 ) && pwd )
TP_SRC=$TP_DIR/src
TP_BUILD=$TP_DIR/build
TP_OUTPUT=$TP_DIR/output

if [ ! -d $TP_SRC ]; then
    mkdir $TP_SRC
fi

if [ ! -d $TP_BUILD ]; then
    mkdir $TP_BUILD
fi

if [ ! -d $TP_OUTPUT ]; then
    mkdir $TP_OUTPUT
fi

cd $TP_SRC
# concurrent queue
echo "download concurrentqueue"
curl https://codeload.github.com/cameron314/concurrentqueue/tar.gz/v1.0.0-beta > concurrentqueue-v1.0.0-beta.tar.gz
tar xf concurrentqueue-v1.0.0-beta.tar.gz

# googletest
echo "download googletest"
curl https://codeload.github.com/google/googletest/tar.gz/release-1.8.0 > googletest-1.8.0.tar.gz
tar xf googletest-1.8.0.tar.gz

# protobuf
echo "download protobuf"
curl https://codeload.github.com/google/protobuf/tar.gz/v3.5.0 > protobuf-v3.5.0.tar.gz
tar xf protobuf-v3.5.0.tar.gz

#rapidjson
echo "download rapidjson"
curl https://codeload.github.com/Tencent/rapidjson/tar.gz/v1.1.0 > rapidjson-v1.1.0.tar.gz
tar xf rapidjson-v1.1.0.tar.gz

# thrift 0.9.3
echo "download thrift"
curl http://archive.apache.org/dist/thrift/0.9.3/thrift-0.9.3.tar.gz > thrift-0.9.3.tar.gz
tar xf thrift-0.9.3.tar.gz
cd thrift-0.9.3
echo "make patch to thrift"
patch -p1 < ../../fix_thrift_for_cpp11.patch
cd ..

# use zookeeper c client
echo "download zookeeper"
curl https://mirrors.tuna.tsinghua.edu.cn/apache/zookeeper/stable/zookeeper-3.4.10.tar.gz > zookeeper-3.4.10.tar.gz
tar xf zookeeper-3.4.10.tar.gz

cd $TP_DIR
