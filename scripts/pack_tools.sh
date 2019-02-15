#!/bin/bash

source $(dirname $0)/pack_common.sh

function usage()
{
    echo "Options for subcommand 'pack_tools':"
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

if [ ! -f DSN_ROOT/bin/pegasus_shell/pegasus_shell ]
then
    echo "ERROR: DSN_ROOT/bin/pegasus_shell/pegasus_shell not found"
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
echo "Packaging pegasus tools $version ($commit_id) $platform $build_type ..."

pack_version=tools-$version-${commit_id:0:7}-${platform}-${build_type}
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

mkdir -p ${pack}
copy_file ./run.sh ${pack}/

mkdir -p ${pack}/DSN_ROOT/bin
cp -v -r ./DSN_ROOT/bin/pegasus_server ${pack}/DSN_ROOT/bin/
cp -v -r ./DSN_ROOT/bin/pegasus_shell ${pack}/DSN_ROOT/bin/
cp -v -r ./DSN_ROOT/bin/pegasus_bench ${pack}/DSN_ROOT/bin/
cp -v -r ./DSN_ROOT/bin/pegasus_kill_test ${pack}/DSN_ROOT/bin/
cp -v -r ./DSN_ROOT/bin/pegasus_rproxy ${pack}/DSN_ROOT/bin/
cp -v -r ./DSN_ROOT/bin/pegasus_pressureclient ${pack}/DSN_ROOT/bin/

mkdir -p ${pack}/DSN_ROOT/lib
copy_file ./DSN_ROOT/lib/*.so* ${pack}/DSN_ROOT/lib/
copy_file ./rdsn/thirdparty/output/lib/libPoco*.so.48 ${pack}/DSN_ROOT/lib/
copy_file ./rdsn/thirdparty/output/lib/libtcmalloc.so.4 ${pack}/DSN_ROOT/lib/
copy_file `get_boost_lib $custom_boost_lib system` ${pack}/DSN_ROOT/lib/
copy_file `get_boost_lib $custom_boost_lib filesystem` ${pack}/DSN_ROOT/lib/
copy_file `get_stdcpp_lib $custom_gcc` ${pack}/DSN_ROOT/lib/
copy_file `get_system_lib shell snappy` ${pack}/DSN_ROOT/lib/`get_system_libname shell snappy`
copy_file `get_system_lib shell crypto` ${pack}/DSN_ROOT/lib/`get_system_libname shell crypto`
copy_file `get_system_lib shell ssl` ${pack}/DSN_ROOT/lib/`get_system_libname shell ssl`
copy_file `get_system_lib shell aio` ${pack}/DSN_ROOT/lib/`get_system_libname shell aio`
copy_file `get_system_lib shell zstd` ${pack}/DSN_ROOT/lib/`get_system_libname shell zstd`
copy_file `get_system_lib shell lz4` ${pack}/DSN_ROOT/lib/`get_system_libname shell lz4`
chmod -x ${pack}/DSN_ROOT/lib/*

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

if [ -f $pack_template ]; then
    echo "Modifying $pack_template ..."
    sed -i "/^version:/c version: \"$pack_version\"" $pack_template
    sed -i "/^build:/c build: \"\.\/run.sh pack_tools\"" $pack_template
    sed -i "/^source:/c source: \"$PEGASUS_ROOT\"" $pack_template
fi

echo "Done"
