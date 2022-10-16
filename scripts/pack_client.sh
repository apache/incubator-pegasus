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
    echo "Options for subcommand 'pack_client':"
    echo "  -h"
    echo "  -p|--update-package-template <minos-package-template-file-path>"
    echo "  -g|--custom-gcc"
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

if [ ! -f ${BUILD_DIR}/output/bin/pegasus_server/pegasus_server ]
then
    echo "ERROR: ${BUILD_DIR}/output/bin/pegasus_server/pegasus_server not found"
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
echo "Packaging pegasus client $version ($commit_id) glibc-$glibc_ver $build_type ..."

pack_version=client-$version-${commit_id:0:7}-glibc${glibc_ver}-${build_type}
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
        *)
            echo "ERROR: unknown option \"$option_key\""
            echo
            usage
            exit 1
            ;;
    esac
    shift
done

mkdir -p ${pack}/lib
copy_file ${BUILD_DIR}/output/lib/libpegasus_client_static.a ${pack}/lib
# TODO(yingchun): make sure shared lib works well too
# copy_file ${BUILD_DIR}/output/lib/libpegasus_client_shared.so ${pack}/lib
copy_file ./thirdparty/output/lib/libboost*.so.1.69.0 ${pack}/lib
ln -sf `ls ${pack}/lib | grep libboost_system` ${pack}/lib/libboost_system.so
ln -sf `ls ${pack}/lib | grep libboost_filesystem` ${pack}/lib/libboost_filesystem.so
ln -sf `ls ${pack}/lib | grep libboost_regex` ${pack}/lib/libboost_regex.so

cp -v -r ./src/include ${pack}

echo "Pegasus Client $version ($commit_id) $platform $build_type" >${pack}/VERSION

tar cfz ${pack}.tar.gz ${pack}

if [ -f "$pack_template" ]; then
    echo "Modifying $pack_template ..."
    sed -i "/^version:/c version: \"$pack_version\"" $pack_template
    sed -i "/^build:/c build: \"\.\/run.sh pack\"" $pack_template
    sed -i "/^source:/c source: \"$PEGASUS_ROOT\"" $pack_template
fi

echo "Done"
