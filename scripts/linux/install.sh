#!/bin/bash
# !!! This script should be run in dsn project root directory (../../).
#
# Options:
#    INSTALL_DIR    <dir>

if [ -z "$INSTALL_DIR" ]
then
    echo "ERROR: no INSTALL_DIR specified"
    exit -1
fi

if [ ! -f "builder/output/lib/libdsn.core.so" ]
then
    echo "ERROR: not build yet"
    exit -1
fi

mkdir -p $INSTALL_DIR
if [ $? -ne 0 ]
then
    echo "ERROR: mkdir $INSTALL_DIR failed"
    exit -1
fi

echo "Copying files..."
cp -r -v builder/output/* $INSTALL_DIR
echo "Install succeed"

