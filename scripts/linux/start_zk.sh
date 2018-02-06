#!/bin/bash
#
# Options:
#    DOWNLOADED_DIR    <dir>
#    PORT           <port>

if [ -z "$DOWNLOADED_DIR" ]
then
    echo "ERROR: no INSTALL_DIR specified"
    exit -1
fi

if [ -z "$PORT" ]
then
    echo "ERROR: no PORT specified"
    exit -1
fi

cd $DOWNLOADED_DIR

ZOOKEEPER_HOME=`pwd`/zookeeper-3.4.10
ZOOKEEPER_PORT=$PORT

cp $ZOOKEEPER_HOME/conf/zoo_sample.cfg $ZOOKEEPER_HOME/conf/zoo.cfg
sed -i "s@dataDir=/tmp/zookeeper@dataDir=$ZOOKEEPER_HOME/data@" $ZOOKEEPER_HOME/conf/zoo.cfg
sed -i "s@clientPort=2181@clientPort=$ZOOKEEPER_PORT@" $ZOOKEEPER_HOME/conf/zoo.cfg

mkdir -p $ZOOKEEPER_HOME/data
$ZOOKEEPER_HOME/bin/zkServer.sh start

sleep 2
if echo ruok | nc localhost $ZOOKEEPER_PORT | grep -q imok; then
    echo "Zookeeper started at port $ZOOKEEPER_PORT"
    exit 0
else
    echo "ERROR: start zookeeper failed"
    exit -1
fi
