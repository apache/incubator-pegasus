#!/bin/bash
set -e
git clone https://github.com/Microsoft/rDSN.git 
cd rDSN 
mkdir mybuilddir 
cd mybuilddir 
cmake .. -DCMAKE_INSTALL_PREFIX=/home/rdsn 
make -j $(grep -c ^processor /proc/cpuinfo) 
make install
