#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

function get_stdcpp_lib()
{
    libname=`ldd ./DSN_ROOT/bin/pegasus_server/pegasus_server 2>/dev/null | grep libstdc++`
    libname=`echo $libname | cut -f1 -d" "`
    if [ $1 = "true" ]; then
        gcc_path=`which gcc`
        echo `dirname $gcc_path`/../lib64/$libname #TODO need fix
    else
        libs=(`ldconfig -p|grep $libname|awk '{print $NF}'`)

        for lib in ${libs[*]}; do
            if [ "`check_bit $lib`" = "true" ]; then
                echo "$lib"
                return
            fi
        done;
    fi
}

# USAGE: get_system_lib server snappy
function get_system_lib()
{
    libname=`ldd ./DSN_ROOT/bin/pegasus_$1/pegasus_$1 2>/dev/null | grep "lib${2}\.so"`
    libname=`echo $libname | cut -f1 -d" "`
    libs=(`ldconfig -p|grep $libname|awk '{print $NF}'`)

    bit_mode=`getconf LONG_BIT`
    for lib in ${libs[*]}; do
        if [ "`check_bit $lib`" = "true" ]; then
            echo "$lib"
            return
        fi
    done;

    # if get failed by ldconfig, then just extract lib from ldd result
    libname=`ldd ./DSN_ROOT/bin/pegasus_$1/pegasus_$1 2>/dev/null | grep "lib${2}\.so"`
    libname=`echo $libname | cut -f3 -d" "`
    if echo "$libname" | grep -q "lib${2}\.so"; then
        echo "$libname"
    fi
}

# USAGE: get_system_libname server snappy
function get_system_libname()
{
    libname=`ldd ./DSN_ROOT/bin/pegasus_$1/pegasus_$1 2>/dev/null | grep "lib${2}\.so"`
    libname=`echo $libname | cut -f1 -d" "`
    echo "$libname"
}

#USAGE: copy_file src [src...] dest
function copy_file()
{
    if [ $# -lt 2 ]; then
        echo "ERROR: invalid copy file command: cp $*"
        exit 1
    fi
    cp -Lrv $*
    if [ $? -ne 0 ]; then
        echo "ERROR: copy file failed: cp $*"
        exit 1
    fi
}

function check_bit()
{
    bit_mode=`getconf LONG_BIT`
    lib=$1
    check_bit=""
    is_softlink=`file $lib | grep "symbolic link"`
    
    if [ -z "$is_softlink" ]; then
        check_bit=`file $lib |grep "$bit_mode-bit"`
    else
        real_lib_name=`ls -l $lib |awk '{print $NF}'`
        lib_path=${lib%/*} 
        real_lib=${lib_path}"/"${real_lib_name}
        check_bit=`file $real_lib |grep "$bit_mode-bit"`
    fi
    if [ -n "$check_bit" ]; then
        echo "true"
    fi
}

