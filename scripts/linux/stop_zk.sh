#!/bin/bash
#
# Options:
#    DOWNLOADED_DIR    <dir>

if [ -z "$DOWNLOADED_DIR" ]
then
    echo "ERROR: no DOWNLOADED_DIR specified"
    exit -1
fi

cd $DOWNLOADED_DIR

ZOOKEEPER_HOME=`pwd`/zookeeper-3.4.10

if [ -d "$ZOOKEEPER_HOME" ]
then
    $ZOOKEEPER_HOME/bin/zkServer.sh stop
fi

