#!/bin/bash

pushd ../../../
while [ 0 -lt 1 ]; do
    ./run.sh stop_zk
    ./run.sh start_zk
    sleep 3
done
popd
