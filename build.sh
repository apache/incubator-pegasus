#!/bin/sh

if [ -z "$BOOST_DIR" ]
then
    echo 'BOOST_DIR not set'
    echo 'You can install boost by this:'
    echo '  wget http://downloads.sourceforge.net/project/boost/boost/1.54.0/boost_1_54_0.zip?r=&ts=1442891144&use_mirror=jaist'
    echo '  unzip -q boost_1_54_0.zip'
    echo '  cd boost_1_54_0'
    echo '  ./bootstrap.sh --with-libraries=system,filesystem --with-toolset=gcc'
    echo '  ./b2 toolset=gcc cxxflags="-std=c++11 -fPIC" -j8 -d0'
    echo '  ./b2 install --prefix=`pwd`/output -d0'
    echo 'And set BOOST_DIR as:'
    echo '  export BOOST_DIR=/path/to/boost_1_54_0/output'
    exit -1
fi

if [ $# -eq 1 -a "$1" == "true" ]
then
    echo "Clear builder..."
    rm -rf builder
    mkdir -p builder
    cd builder
    cmake .. -DCMAKE_INSTALL_PREFIX=`pwd`/output -DCMAKE_BUILD_TYPE=Debug \
        -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ \
        -DBoost_NO_BOOST_CMAKE=ON -DBOOST_ROOT=$BOOST_DIR -DBoost_NO_SYSTEM_PATHS=ON \
        -DDSN_LINK_WRAPPER=TRUE -DDSN_DEBUG_CMAKE=TRUE
    cd ..
fi

cd builder
make -j4
make install
cd ..

