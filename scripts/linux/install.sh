#!/bin/bash
# The MIT License (MIT)
#
# Copyright (c) 2015 Microsoft Corporation
#
# -=- Robust Distributed System Nucleus (rDSN) -=-
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

# !!! This script should be run in dsn project root directory (../../).
#
# Options:
#    INSTALL_DIR    <dir>

if [ -z "$INSTALL_DIR" ]
then
    echo "ERROR: no INSTALL_DIR specified"
    exit 1
fi

if [ ! -f "builder/output/lib/libdsn.core.so" ]
then
    echo "ERROR: not build yet"
    exit 1
fi

mkdir -p $INSTALL_DIR
if [ $? -ne 0 ]
then
    echo "ERROR: mkdir $INSTALL_DIR failed"
    exit 1
fi
INSTALL_DIR=`cd $INSTALL_DIR; pwd`
echo "INSTALL_DIR=$INSTALL_DIR"

echo "Copying files..."
cp -r -v `pwd`/builder/output/* $INSTALL_DIR
echo "Install succeed"
if [ -z "$DSN_ROOT" -o "$DSN_ROOT" != "$INSTALL_DIR" ]
then
    if ! grep -q '^export DSN_ROOT=' ~/.bashrc
    then
        echo "export DSN_ROOT=$INSTALL_DIR" >>~/.bashrc
    else
        sed -i "s@^export DSN_ROOT=.*@export DSN_ROOT=$INSTALL_DIR@" ~/.bashrc
    fi
    if ! grep -q '^export LD_LIBRARY_PATH=.*DSN_ROOT' ~/.bashrc
    then
        echo 'export LD_LIBRARY_PATH=$DSN_ROOT/lib:$LD_LIBRARY_PATH' >>~/.bashrc
    fi
    echo "====== ENVIRONMENT VARIABLE \$DSN_ROOT SET OR CHANGED, please run 'source ~/.bashrc' ======"
fi

