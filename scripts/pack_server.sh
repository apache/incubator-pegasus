#!/bin/bash

source $(dirname $0)/pack_common.sh

function usage()
{
    echo "Options for subcommand 'pack_server':"
    echo "  -h"
    echo "  -p|--update-package-template <minos-package-template-file-path>"
    echo "  -b|--custom-boost-lib"
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

if [ ! -f DSN_ROOT/bin/pegasus_server/pegasus_server ]
then
    echo "ERROR: DSN_ROOT/bin/pegasus_server/pegasus_server not found"
    exit 1
fi

if [ ! -f src/builder/CMAKE_OPTIONS ]
then
    echo "ERROR: src/builder/CMAKE_OPTIONS not found"
    exit 1
fi

if grep -q Debug src/builder/CMAKE_OPTIONS
then
    build_type=debug
else
    build_type=release
fi
version=`grep "VERSION" src/include/pegasus/version.h | cut -d "\"" -f 2`
commit_id=`grep "GIT_COMMIT" src/include/pegasus/git_commit.h | cut -d "\"" -f 2`
platform=`lsb_release -a 2>/dev/null | grep "Distributor ID" | awk '{print $3}' | tr '[A-Z]' '[a-z]'`
echo "Packaging pegasus server $version ($commit_id) $platform $build_type ..."

pack_version=server-$version-${commit_id:0:7}-${platform}-${build_type}
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

custom_boost_lib="false"
custom_gcc="false"

while [[ $# > 0 ]]; do
    option_key="$1"
    case $option_key in
        -p|--update-package-template)
            pack_template="$2"
            shift
            ;;
        -b|--custom-boost-lib)
            custom_boost_lib="true"
            ;;
        -g|--custom-gcc)
            custom_gcc="true"
            ;;
        -h|--help)
            usage
            ;;
    esac
    shift
done

mkdir -p ${pack}/bin
copy_file ./DSN_ROOT/bin/pegasus_server/pegasus_server ${pack}/bin
copy_file ./DSN_ROOT/lib/libdsn_meta_server.so ${pack}/bin
copy_file ./DSN_ROOT/lib/libdsn_replica_server.so ${pack}/bin
copy_file ./rdsn/thirdparty/output/lib/libPoco*.so.48 ${pack}/bin
copy_file ./rdsn/thirdparty/output/lib/libtcmalloc.so.4 ${pack}/bin
copy_file ./scripts/sendmail.sh ${pack}/bin
copy_file ./src/server/config.ini ${pack}/bin

copy_file `get_boost_lib $custom_boost_lib system` ${pack}/bin
copy_file `get_boost_lib $custom_boost_lib filesystem` ${pack}/bin
copy_file `get_stdcpp_lib $custom_gcc` ${pack}/bin
copy_file `get_system_lib server snappy` ${pack}/bin/`get_system_libname server snappy`
copy_file `get_system_lib server crypto` ${pack}/bin/`get_system_libname server crypto`
copy_file `get_system_lib server ssl` ${pack}/bin/`get_system_libname server ssl`
copy_file `get_system_lib server aio` ${pack}/bin/`get_system_libname server aio`
copy_file `get_system_lib server zstd` ${pack}/bin/`get_system_libname server zstd`
copy_file `get_system_lib server lz4` ${pack}/bin/`get_system_libname server lz4`

chmod +x ${pack}/bin/pegasus_* ${pack}/bin/*.sh
chmod -x ${pack}/bin/lib*

echo "Pegasus Server $version ($commit_id) $platform $build_type" >${pack}/VERSION

tar cfz ${pack}.tar.gz ${pack}

if [ -f $pack_template ]; then
    echo "Modifying $pack_template ..."
    sed -i "/^version:/c version: \"$pack_version\"" $pack_template
    sed -i "/^build:/c build: \"\.\/run.sh pack\"" $pack_template
    sed -i "/^source:/c source: \"$PEGASUS_ROOT\"" $pack_template
fi

echo "Done"
