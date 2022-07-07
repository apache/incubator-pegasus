#!/bin/bash

root=$(dirname $0)
root=$(cd $root; pwd)

action=build
if [ x"$1" == x ]; then
    echo "You can input an action: build/install/package/test"
    echo "Default action: build, equivalent to run the cmd: sh build.sh build"
    action=build
else
    action=$2
fi

pushd $root/idl
sh recompile_thrift.sh
popd
mvn spotless:apply
if [ $action == "build" ]; then
    mvn clean package -DskipTests
elif [ $action == "install" ]; then
    mvn clean install -DskipTests
elif [ $action == "test" ]; then
    mvn clean package
elif [ $action == "onetest" ]; then
    mvn clean package -Dtest=$2
else
    echo "unsupport action: "$action
fi

# vim: set expandtab ts=4 sw=4 sts=4 tw=100: #
