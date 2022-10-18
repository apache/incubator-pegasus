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

set -e

source $(dirname $0)/pack_common.sh

function usage()
{
    echo "Options for subcommand 'pack_tools':"
    echo "  -h"
    echo "  -p|--update-package-template <minos-package-template-file-path>"
    echo "  -g|--custom-gcc"
    echo "  -j|--use-jemalloc"
    exit 0
}

pwd="$( cd "$( dirname "$0"  )" && pwd )"
shell_dir="$( cd $pwd/.. && pwd )"
cd $shell_dir

if [ ! -f src/include/pegasus/git_commit.h ]
then
    echo "ERROR: src/include/pegasus/git_commit.h not found"
    exit 1
fi

if [ ! -f ${BUILD_DIR}/output/bin/pegasus_shell/pegasus_shell ]
then
    echo "ERROR: ${BUILD_DIR}/output/bin/pegasus_shell/pegasus_shell not found"
    exit 1
fi

if [ ! -f src/builder/CMakeCache.txt ]
then
    echo "ERROR: src/builder/CMakeCache.txt not found"
    exit 1
fi

if egrep -i "CMAKE_BUILD_TYPE:STRING\=debug" src/builder/CMakeCache.txt;
then
    build_type=debug
else
    build_type=release
fi
version=`grep "VERSION" src/include/pegasus/version.h | cut -d "\"" -f 2`
commit_id=`grep "GIT_COMMIT" src/include/pegasus/git_commit.h | cut -d "\"" -f 2`
glibc_ver=`ldd --version | grep ldd | grep -Eo "[0-9]+.[0-9]+$"`
echo "Packaging pegasus tools $version ($commit_id) glibc-$glibc_ver $build_type ..."

pack_version=tools-$version-${commit_id:0:7}-glibc${glibc_ver}-${build_type}
pack=pegasus-$pack_version

if [ -f ${pack}.tar.gz ]
then
    rm -f ${pack}.tar.gz
fi

if [ -d ${pack} ]
then
    rm -rf ${pack}
fi

pack_template=""
if [ -n "$MINOS_CONFIG_FILE" ]; then
    pack_template=`dirname $MINOS_CONFIG_FILE`/xiaomi-config/package/pegasus.yaml
fi

custom_gcc="false"
use_jemalloc="off"

while [[ $# > 0 ]]; do
    option_key="$1"
    case $option_key in
        -p|--update-package-template)
            pack_template="$2"
            shift
            ;;
        -g|--custom-gcc)
            custom_gcc="true"
            ;;
        -h|--help)
            usage
            ;;
        -j|--use-jemalloc)
            use_jemalloc="on"
            ;;
        *)
            echo "ERROR: unknown option \"$option_key\""
            echo
            usage
            exit 1
            ;;
    esac
    shift
done

mkdir -p ${pack}
copy_file ./run.sh ${pack}/

mkdir -p ${pack}/bin
cp -v -r ${BUILD_DIR}/output/bin/pegasus_server ${pack}/bin/
cp -v -r ${BUILD_DIR}/output/bin/pegasus_shell ${pack}/bin/
cp -v -r ${BUILD_DIR}/output/bin/pegasus_bench ${pack}/bin/
cp -v -r ${BUILD_DIR}/output/bin/pegasus_kill_test ${pack}/bin/
cp -v -r ${BUILD_DIR}/output/bin/pegasus_rproxy ${pack}/bin/
cp -v -r ${BUILD_DIR}/output/bin/pegasus_pressureclient ${pack}/bin/

mkdir -p ${pack}/lib
copy_file ${BUILD_DIR}/output/lib/*.so* ${pack}/lib/
copy_file ./thirdparty/output/lib/libPoco*.so.* ${pack}/lib/

if [ "$use_jemalloc" == "on" ]; then
    copy_file ./thirdparty/output/lib/libjemalloc.so.2 ${pack}/lib/
    copy_file ./thirdparty/output/lib/libprofiler.so.0 ${pack}/lib/
else
    copy_file ./thirdparty/output/lib/libtcmalloc_and_profiler.so.4 ${pack}/lib/
fi

copy_file ./thirdparty/output/lib/libboost*.so.1.69.0 ${pack}/lib/
copy_file ./thirdparty/output/lib/libhdfs* ${pack}/lib
copy_file `get_stdcpp_lib $custom_gcc` ${pack}/lib/

pack_tools_lib() {
    pack_system_lib "${pack}/lib" shell "$1"
}

pack_tools_lib snappy
pack_tools_lib crypto
pack_tools_lib ssl
pack_tools_lib zstd
pack_tools_lib lz4

chmod -x ${pack}/lib/*

mkdir -p ${pack}/scripts
copy_file ./scripts/* ${pack}/scripts/
chmod +x ${pack}/scripts/*.sh

mkdir -p ${pack}/src/server
copy_file ./src/server/*.ini ${pack}/src/server/

mkdir -p ${pack}/src/shell
copy_file ./src/shell/*.ini ${pack}/src/shell/

mkdir -p ${pack}/src/test/kill_test
copy_file ./src/test/kill_test/*.ini ${pack}/src/test/kill_test/

echo "Pegasus Tools $version ($commit_id) $platform $build_type" >${pack}/VERSION

tar cfz ${pack}.tar.gz ${pack}

if [ -f "$pack_template" ]; then
    echo "Modifying $pack_template ..."
    sed -i "/^version:/c version: \"$pack_version\"" $pack_template
    sed -i "/^build:/c build: \"\.\/run.sh pack_tools\"" $pack_template
    sed -i "/^source:/c source: \"$PEGASUS_ROOT\"" $pack_template
fi

echo "Done"
