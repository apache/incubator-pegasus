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

function usage() {
    echo "Options for subcommand 'pack_server':"
    echo "  -h"
    echo "  -p|--update-package-template <minos-package-template-file-path>"
    echo "  -g|--custom-gcc"
    echo "  -k|--keytab-file"
    echo "  -j|--use-jemalloc"
    echo "  -s|--separate_servers"
    exit 0
}

pwd="$(cd "$(dirname "$0")" && pwd)"
shell_dir="$(cd $pwd/.. && pwd)"
cd "$shell_dir" || exit 1

if [ ! -f src/include/pegasus/git_commit.h ]; then
    echo "ERROR: src/include/pegasus/git_commit.h not found"
    exit 1
fi

if [ ! -f ${BUILD_LATEST_DIR}/CMakeCache.txt ]; then
    echo "ERROR: ${BUILD_LATEST_DIR}/CMakeCache.txt not found"
    exit 1
fi

if egrep -i "CMAKE_BUILD_TYPE:STRING\=debug" ${BUILD_LATEST_DIR}/CMakeCache.txt; then
    build_type=debug
else
    build_type=release
fi
version=$(grep "VERSION" src/include/pegasus/version.h | cut -d "\"" -f 2)
commit_id=$(grep "GIT_COMMIT" src/include/pegasus/git_commit.h | cut -d "\"" -f 2)
glibc_ver=$(ldd --version | grep ldd | grep -Eo "[0-9]+.[0-9]+$")
echo "Packaging pegasus server $version ($commit_id) glibc-$glibc_ver $build_type ..."

pack_version=server-$version-${commit_id:0:7}-glibc${glibc_ver}-${build_type}
pack=pegasus-$pack_version

if [ -f ${pack}.tar.gz ]; then
    rm -f ${pack}.tar.gz
fi

if [ -d ${pack} ]; then
    rm -rf ${pack}
fi

pack_template=""
if [ -n "$MINOS_CONFIG_FILE" ]; then
    pack_template=$(dirname $MINOS_CONFIG_FILE)/xiaomi-config/package/pegasus.yaml
fi

custom_gcc="false"
keytab_file=""
use_jemalloc="false"
separate_servers="false"

while [[ $# > 0 ]]; do
    option_key="$1"
    case $option_key in
    -p | --update-package-template)
        pack_template="$2"
        shift
        ;;
    -g | --custom-gcc)
        custom_gcc="true"
        ;;
    -h | --help)
        usage
        ;;
    -k | --keytab-file)
        keytab_file="$2"
        shift
        ;;
    -j | --use-jemalloc)
        use_jemalloc="true"
        ;;
    -s | --separate_servers)
        separate_servers="true"
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

mkdir -p ${pack}/bin
if [[ $separate_servers == "false" ]]; then
    copy_file ${BUILD_LATEST_DIR}/output/bin/pegasus_server/pegasus_server ${pack}/bin
else
    copy_file ${BUILD_LATEST_DIR}/output/bin/pegasus_meta_server/pegasus_meta_server ${pack}/bin
    copy_file ${BUILD_LATEST_DIR}/output/bin/pegasus_replica_server/pegasus_replica_server ${pack}/bin
fi
copy_file ${BUILD_LATEST_DIR}/output/lib/libdsn_meta_server.so ${pack}/bin
copy_file ${BUILD_LATEST_DIR}/output/lib/libdsn_replica_server.so ${pack}/bin
copy_file ${BUILD_LATEST_DIR}/output/lib/libdsn_utils.so ${pack}/bin

if [ "$use_jemalloc" == "true" ]; then
    copy_file ${THIRDPARTY_ROOT}/output/lib/libjemalloc.so.2 ${pack}/bin
    copy_file ${THIRDPARTY_ROOT}/output/lib/libprofiler.so.0 ${pack}/bin
else
    copy_file ${THIRDPARTY_ROOT}/output/lib/libtcmalloc_and_profiler.so.4 ${pack}/bin
fi

copy_file ${THIRDPARTY_ROOT}/output/lib/libhdfs* ${pack}/bin
copy_file ${THIRDPARTY_ROOT}/output/lib/librocksdb.so.8 ${pack}/bin
copy_file ./admin_tools/config_hdfs.sh ${pack}/bin
copy_file ./admin_tools/sendmail.sh ${pack}/bin
copy_file ./src/server/config.ini ${pack}/bin
copy_file ./src/server/config.min.ini ${pack}/bin

copy_file "$(get_stdcpp_lib $custom_gcc $separate_servers)" "${pack}/bin"

pack_server_lib() {
    if [[ $2 == "false" ]]; then
        pack_system_lib "${pack}/bin" server "$1"
    else
        pack_system_lib "${pack}/bin" meta_server "$1"
    fi
}

pack_server_lib crypto $separate_servers
pack_server_lib ssl $separate_servers

# Pack hadoop-related files.
# If you want to use hdfs service to backup/restore/bulkload pegasus tables, you need to
# set env ${HADOOP_HOME} to the proper directory where contains Hadoop *.jar files.
if [ -n "$HADOOP_HOME" ]; then
    # Verify one of the jars.
    arch_output=$(arch)
    if [ "$arch_output"x == "aarch64"x ]; then
        HDFS_JAR_MD5="fcc09dbed936cd8673918774cc3ead6b"
    else
      if [ "$arch_output"x != "x86_64"x ]; then
          echo "WARNING: unrecognized CPU architecture '$arch_output', use 'x86_64' as default"
      fi
      HDFS_JAR_MD5="f67f3a5613c885e1622b1056fd94262b"
    fi
    HDFS_JAR=${HADOOP_HOME}/share/hadoop/hdfs/hadoop-hdfs-3.3.6.jar
    if [ "$(md5sum "${HDFS_JAR}" | awk '{print$1}')" != "${HDFS_JAR_MD5}" ]; then
        echo "check file ${HDFS_JAR} md5sum failed!"
        exit 1
    fi
    # Pack the jars.
    mkdir -p ${pack}/hadoop
    for f in ${HADOOP_HOME}/share/hadoop/common/lib/*.jar; do
        copy_file $f ${pack}/hadoop
    done
    for f in ${HADOOP_HOME}/share/hadoop/common/*.jar; do
        copy_file $f ${pack}/hadoop
    done
    for f in ${HADOOP_HOME}/share/hadoop/hdfs/lib/*.jar; do
        copy_file $f ${pack}/hadoop
    done
    for f in ${HADOOP_HOME}/share/hadoop/hdfs/*.jar; do
        copy_file $f ${pack}/hadoop
    done
else
    echo "Couldn't find env HADOOP_HOME, hadoop-related files were not packed."
fi

DISTRIB_ID=$(cat /etc/*-release | grep DISTRIB_ID | awk -F'=' '{print $2}')
DISTRIB_RELEASE=$(cat /etc/*-release | grep DISTRIB_RELEASE | awk -F'=' '{print $2}')
if [ -n "$DISTRIB_ID" ] && [ -n "$DISTRIB_RELEASE" ]; then
    if [ "$DISTRIB_ID" == "Ubuntu" ] && [ "$DISTRIB_RELEASE" == "18.04" ]; then
        pack_server_lib icui18n
        pack_server_lib icuuc
        pack_server_lib icudata
    fi
    # more cases can be added here.
fi

chmod +x ${pack}/bin/pegasus_* ${pack}/bin/*.sh
chmod -x ${pack}/bin/lib*

echo "Pegasus Server $version ($commit_id) $platform $build_type" >${pack}/VERSION

tar cfz ${pack}.tar.gz ${pack}

if [ -f "$pack_template" ]; then
    echo "Modifying $pack_template ..."
    sed -i "/^version:/c version: \"$pack_version\"" $pack_template
    sed -i "/^build:/c build: \"\.\/run.sh pack\"" $pack_template
    sed -i "/^source:/c source: \"$PEGASUS_ROOT\"" $pack_template
fi

echo ${pack} >PACKAGE

echo "Done"
