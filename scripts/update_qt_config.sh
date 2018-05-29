#!/bin/bash
# This is used for updating the meta-data of Qt Creator IDE.

PREFIX=pegasus
if [ $# -eq 1 ]
then
    PREFIX=$1
fi

pwd="$( cd "$( dirname "$0"  )" && pwd )"
shell_dir="$( cd $pwd/.. && pwd )"
cd $shell_dir

# config
CONFIG_OUT="${PREFIX}.config"
echo "Generate $CONFIG_OUT"
rm $CONFIG_OUT &>/dev/null
echo "#define __cplusplus 201103L" >>$CONFIG_OUT
echo "#define _DEBUG" >>$CONFIG_OUT
echo "#define DSN_USE_THRIFT_SERIALIZATION" >>$CONFIG_OUT
echo "#define DSN_ENABLE_THRIFT_RPC" >>$CONFIG_OUT
echo "#define DSN_BUILD_TYPE" >>$CONFIG_OUT
echo "#define DSN_BUILD_HOSTNAME" >>$CONFIG_OUT
echo "#define ROCKSDB_PLATFORM_POSIX" >>$CONFIG_OUT
echo "#define OS_LINUX" >>$CONFIG_OUT
echo "#define ROCKSDB_FALLOCATE_PRESENT" >>$CONFIG_OUT
echo "#define GFLAGS google" >>$CONFIG_OUT
echo "#define ZLIB" >>$CONFIG_OUT
echo "#define BZIP2" >>$CONFIG_OUT
echo "#define ROCKSDB_MALLOC_USABLE_SIZE" >>$CONFIG_OUT
#echo "#define __FreeBSD__" >>$CONFIG_OUT
#echo "#define _WIN32" >>$CONFIG_OUT

# includes
INCLUDES_OUT="${PREFIX}.includes"
echo "Generate $INCLUDES_OUT"
rm $INCLUDES_OUT &>/dev/null
echo "/usr/include" >>$INCLUDES_OUT
echo "/usr/include/c++/4.8" >>$INCLUDES_OUT
echo "/usr/include/x86_64-linux-gnu" >>$INCLUDES_OUT
echo "/usr/include/x86_64-linux-gnu/c++/4.8" >>$INCLUDES_OUT
echo "rdsn/include" >>$INCLUDES_OUT
echo "rdsn/thirdparty/output/include" >>$INCLUDES_OUT
echo "rdsn/include/dsn/dist/failure_detector" >>$INCLUDES_OUT
echo "rdsn/src/dist/replication/client_lib" >>$INCLUDES_OUT
echo "rdsn/src/dist/replication/lib" >>$INCLUDES_OUT
echo "rdsn/src/dist/replication/meta_server" >>$INCLUDES_OUT
echo "rdsn/src/dist/replication/zookeeper" >>$INCLUDES_OUT
echo "rdsn/thirdparty/output/include" >>$INCLUDES_OUT
echo "rdsn/src/dist/block_service/fds" >>$INCLUDES_OUT
echo "rdsn/src/dist/block_service/local" >>$INCLUDES_OUT
echo "rdsn/src" >> $INCLUDES_OUT
echo "rocksdb" >>$INCLUDES_OUT
echo "rocksdb/include" >>$INCLUDES_OUT
echo "src" >>$INCLUDES_OUT
echo "src/include" >>$INCLUDES_OUT
echo "src/redis_protocol/proxy_lib" >>$INCLUDES_OUT

# files
FILES_OUT="${PREFIX}.files"
echo "Generate $FILES_OUT"
rm $FILES_OUT >&/dev/null
echo "build.sh" >>$FILES_OUT
echo "rdsn/CMakeLists.txt" >>$FILES_OUT
echo "rdsn/bin/dsn.cmake" >>$FILES_OUT
FILES_DIR="
src rocksdb rdsn scripts
"
for i in $FILES_DIR
do
    find $i -name '*.h' -o -name '*.cpp' -o -name '*.c' -o -name '*.cc' \
        -o -name '*.thrift' -o -name '*.ini' -o -name '*.act' \
        -o -name 'CMakeLists.txt' -o -name '*.sh' \
        | grep -v '\<builder\>\|rdsn\/thirdparty\|\.zk_install' >>$FILES_OUT
done
