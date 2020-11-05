#!/bin/bash
#
# Options:
#    INSTALL_DIR    <dir>
#    PORT           <port>

PROJECT_DIR=$(realpath $(dirname $(dirname $(dirname "${BASH_SOURCE[0]}"))))

if [ -z "$INSTALL_DIR" ]
then
    echo "ERROR: no INSTALL_DIR specified"
    exit 1
fi

if [ -z "$PORT" ]
then
    echo "ERROR: no PORT specified"
    exit 1
fi

mkdir -p $INSTALL_DIR
if [ $? -ne 0 ]
then
    echo "ERROR: mkdir $PREFIX failed"
    exit 1
fi

cd $INSTALL_DIR

ZOOKEEPER_PKG=${PROJECT_DIR}/thirdparty/build/Download/zookeeper/zookeeper-3.4.10.tar.gz
if [ ! -f ${ZOOKEEPER_PKG} ]; then
    echo "no such file \"${ZOOKEEPER_PKG}\""
    echo "please install third-parties first"
    exit 1
fi

if [ ! -d zookeeper-3.4.6 ]; then
    echo "Decompressing zookeeper..."
    cp ${ZOOKEEPER_PKG} .
    tar xf zookeeper-3.4.10.tar.gz
    if [ $? -ne 0 ]; then
        echo "ERROR: decompress zookeeper failed"
        exit 1
    fi
fi

ZOOKEEPER_HOME=`pwd`/zookeeper-3.4.10
ZOOKEEPER_PORT=$PORT

cp $ZOOKEEPER_HOME/conf/zoo_sample.cfg $ZOOKEEPER_HOME/conf/zoo.cfg
sed -i "s@dataDir=/tmp/zookeeper@dataDir=$ZOOKEEPER_HOME/data@" $ZOOKEEPER_HOME/conf/zoo.cfg
sed -i "s@clientPort=2181@clientPort=$ZOOKEEPER_PORT@" $ZOOKEEPER_HOME/conf/zoo.cfg

mkdir -p $ZOOKEEPER_HOME/data
$ZOOKEEPER_HOME/bin/zkServer.sh start
sleep 1

zk_check_count=0
while true; do
    sleep 1 # wait until zookeeper bootstrapped
    if echo ruok | nc localhost "$ZOOKEEPER_PORT" | grep -q imok; then
        echo "Zookeeper started at port $ZOOKEEPER_PORT"
        exit 0
    fi
    zk_check_count=$((zk_check_count+1))
    echo "ERROR: starting zookeeper has failed ${zk_check_count} times"
    if [ $zk_check_count -gt 30 ]; then
        echo "ERROR: failed to start zookeeper in 30 seconds"
        exit 1
    fi
done
