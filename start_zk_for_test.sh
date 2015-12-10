#!/bin/bash

if [ ! -f zookeeper-3.4.6.tar.gz ]; then
    wget https://github.com/shengofsun/packages/raw/master/zookeeper-3.4.6.tar.gz
    if [ $? -ne 0 ]; then
        echo "download zookeeper failed"
        exit -1
    fi
fi

if [ ! -d zookeeper-3.4.6 ]; then
    tar xfz zookeeper-3.4.6.tar.gz
    if [ $? -ne 0 ]; then
        echo "decompress zookeeper failed"
        exit -1
    fi
fi

ZOOKEEPER_HOME=`pwd`/zookeeper-3.4.6
ZOOKEEPER_PORT=12181

cp $ZOOKEEPER_HOME/conf/zoo_sample.cfg $ZOOKEEPER_HOME/conf/zoo.cfg
sed -i "s@dataDir=/tmp/zookeeper@dataDir=$ZOOKEEPER_HOME/data@" $ZOOKEEPER_HOME/conf/zoo.cfg
sed -i "s@clientPort=2181@clientPort=$ZOOKEEPER_PORT@" $ZOOKEEPER_HOME/conf/zoo.cfg

mkdir -p $ZOOKEEPER_HOME/data
$ZOOKEEPER_HOME/bin/zkServer.sh start

sleep 3
if echo ruok | nc localhost $ZOOKEEPER_PORT | grep imok; then
    echo "zookeeper started at port $ZOOKEEPER_PORT"
    exit 0
else
    echo "start zookeeper failed"
    exit -1
fi
 
