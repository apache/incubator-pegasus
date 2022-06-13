#!/bin/bash
#
# Options:
#    INSTALL_DIR    <dir>

if [ -z "$INSTALL_DIR" ]
then
    echo "ERROR: no INSTALL_DIR specified"
    exit 1
fi

cd $INSTALL_DIR

ZOOKEEPER_HOME=`pwd`/apache-zookeeper-3.7.0-bin

if [ -d "$ZOOKEEPER_HOME" ]
then
    $ZOOKEEPER_HOME/bin/zkServer.sh stop
    rm -rf $ZOOKEEPER_HOME/data &>/dev/null
    echo "Clearing zookeeper ... CLEARED"
fi
