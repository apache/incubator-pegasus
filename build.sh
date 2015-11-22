#!/bin/bash

if [ $# -eq 1 -a "$1" == "true" ]
then
    echo "Clear builder..."
    rm -rf builder
    mkdir -p builder
    cd builder
    cmake .. -DCMAKE_INSTALL_PREFIX=`pwd`/output -DCMAKE_BUILD_TYPE=Debug
    cd ..
fi

cd builder
make -j4
make install
cd ..

