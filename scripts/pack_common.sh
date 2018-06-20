#!/bin/bash

function get_boost_lib()
{
    libname=`ldd ./DSN_ROOT/bin/pegasus_server/pegasus_server 2>/dev/null | grep boost_$2`
    libname=`echo $libname | cut -f1 -d" "`
    if [ $1 = "true" ]; then
        echo $BOOST_DIR/lib/$libname
    else
        echo `ldconfig -p|grep $libname|awk '{print $NF}'`
    fi
}

function get_stdcpp_lib()
{
    libname=`ldd ./DSN_ROOT/bin/pegasus_server/pegasus_server 2>/dev/null | grep libstdc++`
    libname=`echo $libname | cut -f1 -d" "`
    if [ $1 = "true" ]; then
        gcc_path=`which gcc`
        echo `dirname $gcc_path`/../lib64/$libname
    else
        echo `ldconfig -p|grep $libname|awk '{print $NF}'`
    fi
}

# USAGE: get_system_lib server snappy
function get_system_lib()
{
    libname=`ldd ./DSN_ROOT/bin/pegasus_$1/pegasus_$1 2>/dev/null | grep "lib${2}\.so"`
    libname=`echo $libname | cut -f1 -d" "`
    echo `ldconfig -p|grep $libname|awk '{print $NF}'`
}

#USAGE: copy_file src [src...] dest
function copy_file()
{
    if [ $# -lt 2 ]; then
        echo "ERROR: invalid copy file command: cp $*"
        exit 1
    fi
    cp -v $*
    if [ $? -ne 0 ]; then
        echo "ERROR: copy file failed: cp $*"
        exit 1
    fi
}

