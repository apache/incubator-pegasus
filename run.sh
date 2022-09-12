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

PID=$$
ROOT=`pwd`
LOCAL_HOSTNAME=`hostname -f`
export REPORT_DIR="$ROOT/test_report"
export DSN_ROOT=$ROOT/DSN_ROOT
export THIRDPARTY_ROOT=$ROOT/thirdparty
export LD_LIBRARY_PATH=$DSN_ROOT/lib:$THIRDPARTY_ROOT/output/lib:$LD_LIBRARY_PATH

function usage()
{
    echo "usage: run.sh <command> [<args>]"
    echo
    echo "Command list:"
    echo "   help                      print the help info"
    echo "   build                     build the system"
    echo
    echo "   start_zk                  start local single zookeeper server"
    echo "   stop_zk                   stop local zookeeper server"
    echo "   clear_zk                  stop local zookeeper server and clear data"
    echo
    echo "   start_onebox              start pegasus onebox"
    echo "   stop_onebox               stop pegasus onebox"
    echo "   list_onebox               list pegasus onebox"
    echo "   clear_onebox              clear pegasus onebox"
    echo
    echo "   start_onebox_instance     start pegasus onebox instance"
    echo "   stop_onebox_instance      stop pegasus onebox instance"
    echo "   restart_onebox_instance   restart pegasus onebox instance"
    echo
    echo "   start_kill_test           start pegasus kill test"
    echo "   stop_kill_test            stop pegasus kill test"
    echo "   list_kill_test            list pegasus kill test"
    echo "   clear_kill_test           clear pegasus kill test"
    echo
    echo "   bench                     run benchmark test"
    echo "   shell                     run pegasus shell"
    echo "   migrate_node              migrate primary replicas out of specified node"
    echo "   downgrade_node            downgrade replicas to inactive on specified node"
    echo
    echo "   test                      run unit test"
    echo
    echo "   pack_server               generate pegasus server package for deploy with minos"
    echo "   pack_client               generate pegasus client package"
    echo "   pack_tools                generate pegasus tools package for shell and benchmark test"
    echo
    echo "   bump_version              change the version of the project"
    echo "Command 'run.sh <command> -h' will print help for subcommands."
}

#####################
## build
#####################
function usage_build()
{
    echo "Options for subcommand 'build':"
    echo "   -h|--help             print the help info"
    echo "   -t|--type             build type: debug|release, default is release"
    echo "   -c|--clear            clear pegasus before building, not clear thirdparty"
    echo "   --clear_thirdparty    clear thirdparty/pegasus before building"
    echo "   --compiler            specify c and cxx compiler, sperated by ','"
    echo "                         e.g., \"gcc,g++\" or \"clang-3.9,clang++-3.9\""
    echo "                         default is \"gcc,g++\""
    echo "   -j|--jobs <num>       the number of jobs to run simultaneously, default 8"
    echo "   --enable_gcov         generate gcov code coverage report, default no"
    echo "   -v|--verbose          build in verbose mode, default no"
    echo "   --disable_gperf       build without gperftools, this flag is mainly used"
    echo "                         to enable valgrind memcheck, default no"
    echo "   --use_jemalloc        build with jemalloc"
    echo "   --sanitizer <type>    build with sanitizer to check potential problems,
                                   type: address|leak|thread|undefined"
    echo "   --skip_thirdparty     whether to skip building thirdparties, default no"
    echo "   --enable_rocksdb_portable      build a portable rocksdb binary"
    echo "   --test                whether to build test binaries"
}

function exit_if_fail() {
    if [ $1 != 0 ]; then
        exit $1
    fi
}

function run_build()
{
    # Note(jiashuo1): No "memory" check mode, because MemorySanitizer is only available in Clang for Linux x86_64 targets
    # # https://www.jetbrains.com/help/clion/google-sanitizers.html
    SANITIZERS=("address" "leak" "thread" "undefined")

    C_COMPILER="gcc"
    CXX_COMPILER="g++"
    BUILD_TYPE="release"
    CLEAR=NO
    CLEAR_THIRDPARTY=NO
    JOB_NUM=8
    ENABLE_GCOV=NO
    RUN_VERBOSE=NO
    ENABLE_GPERF=ON
    SKIP_THIRDPARTY=NO
    SANITIZER=""
    ROCKSDB_PORTABLE=OFF
    USE_JEMALLOC=OFF
    BUILD_TEST=OFF
    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_build
                exit 0
                ;;
            -t|--type)
                BUILD_TYPE="$2"
                shift
                ;;
            -c|--clear)
                CLEAR=YES
                ;;
            --clear_thirdparty)
                CLEAR_THIRDPARTY=YES
                ;;
            --compiler)
                C_COMPILER=`echo $2 | awk -F',' '{print $1}'`
                CXX_COMPILER=`echo $2 | awk -F',' '{print $2}'`
                if [ "x"$C_COMPILER == "x" -o "x"$CXX_COMPILER == "x" ]; then
                    echo "ERROR: invalid compiler option: $2"
                    echo
                    usage_build
                    exit 1
                fi
                shift
                ;;
            -j|--jobs)
                JOB_NUM="$2"
                shift
                ;;
            --enable_gcov)
                ENABLE_GCOV=YES
                ;;
            --sanitizer)
                IS_SANITIZERS=`echo ${SANITIZERS[@]} | grep -w $2`
                if [[ -z ${IS_SANITIZERS} ]]; then
                    echo "ERROR: unknown sanitizer type \"$2\""
                    usage_build
                    exit 1
                fi
                SANITIZER="$2"
                shift
                ;;
            -v|--verbose)
                RUN_VERBOSE=YES
                ;;
            --disable_gperf)
                ENABLE_GPERF=OFF
                ;;
            --skip_thirdparty)
                SKIP_THIRDPARTY=YES
                ;;
            --enable_rocksdb_portable)
                ROCKSDB_PORTABLE=ON
                ;;
            --use_jemalloc)
                ENABLE_GPERF=OFF
                USE_JEMALLOC=ON
                ;;
            --test)
                BUILD_TEST=ON
                ;;
            *)
                echo "ERROR: unknown option \"$key\""
                echo
                usage_build
                exit 1
                ;;
        esac
        shift
    done

    if [ "$BUILD_TYPE" != "debug" -a "$BUILD_TYPE" != "release" ]; then
        echo "ERROR: invalid build type \"$BUILD_TYPE\""
        echo
        usage_build
        exit 1
    fi

    CMAKE_OPTIONS="-DCMAKE_C_COMPILER=${C_COMPILER}
                   -DCMAKE_CXX_COMPILER=${CXX_COMPILER}
                   -DUSE_JEMALLOC=${USE_JEMALLOC}
                   -DCMAKE_BUILD_TYPE=${BUILD_TYPE}
                   -DENABLE_GCOV=${ENABLE_GCOV}
                   -DENABLE_GPERF=${ENABLE_GPERF}
                   -DBoost_NO_BOOST_CMAKE=ON
                   -DBOOST_ROOT=${THIRDPARTY_ROOT}/output
                   -DBoost_NO_SYSTEM_PATHS=ON"

    if [ "$(uname)" == "Darwin" ]; then
        CMAKE_OPTIONS="${CMAKE_OPTIONS} -DMACOS_OPENSSL_ROOT_DIR=/usr/local/opt/openssl"
    fi

    if [ ! -z "${SANITIZER}" ]; then
        CMAKE_OPTIONS="${CMAKE_OPTIONS} -DSANITIZER=${SANITIZER}"
    fi

    MAKE_OPTIONS="-j$JOB_NUM"
    if [ "$RUN_VERBOSE" == "YES" ]; then
        MAKE_OPTIONS="${MAKE_OPTIONS} VERBOSE=1"
    fi

    echo "Build start time: `date`"
    start_time=`date +%s`

    if [[ ${SKIP_THIRDPARTY} == "YES" ]]; then
        echo "Skip building third-parties..."
    else
        cd ${THIRDPARTY_ROOT}
        if [[ "$CLEAR_THIRDPARTY" == "YES" ]]; then
            echo "Clear third-parties..."
            rm -rf build
            rm -rf output
        fi
        echo "Start building third-parties..."
        mkdir -p build
        pushd build
        CMAKE_OPTIONS="${CMAKE_OPTIONS} -DROCKSDB_PORTABLE=${ROCKSDB_PORTABLE}"
        cmake .. ${CMAKE_OPTIONS}
        make -j$JOB_NUM
        exit_if_fail $?
        popd
        cd ..
    fi

    echo "INFO: start build Pegasus..."
    BUILD_DIR="${ROOT}/src/${BUILD_TYPE}_${SANITIZER}_builder"
    if [ "$CLEAR" == "YES" ]; then
        echo "Clear $BUILD_DIR ..."
        rm -rf $BUILD_DIR
    fi

    pushd ${ROOT}
    echo "Gen thrift"
    # TODO(yingchun): should be optimized
    python3 $ROOT/scripts/compile_thrift.py
    sh ${ROOT}/scripts/recompile_thrift.sh

    mkdir -p ${DSN_ROOT}
    if [ ! -d "$BUILD_DIR" ]; then
        mkdir -p $BUILD_DIR

        echo "Running cmake Pegasus..."
        pushd $BUILD_DIR
        CMAKE_OPTIONS="${CMAKE_OPTIONS} -DBUILD_TEST=${BUILD_TEST}"
        cmake ../.. -DCMAKE_INSTALL_PREFIX=$BUILD_DIR/output $CMAKE_OPTIONS
        exit_if_fail $?
    fi

    echo "Gen git_commit.h ..."
    pushd "$ROOT/src"
    PEGASUS_GIT_COMMIT="non-git-repo"
    if git rev-parse HEAD; then # this is a git repo
        PEGASUS_GIT_COMMIT=$(git rev-parse HEAD)
    fi
    echo "PEGASUS_GIT_COMMIT=${PEGASUS_GIT_COMMIT}"
    GIT_COMMIT_FILE=$ROOT/src/include/pegasus/git_commit.h
    echo "Generating $GIT_COMMIT_FILE..."
    echo "#pragma once" >$GIT_COMMIT_FILE
    echo "#define PEGASUS_GIT_COMMIT \"$PEGASUS_GIT_COMMIT\"" >>$GIT_COMMIT_FILE

    # rebuild link
    rm -f ${ROOT}/src/builder
    ln -s ${BUILD_DIR} ${ROOT}/src/builder

    echo "[$(date)] Building Pegasus ..."
    pushd $BUILD_DIR
    make install $MAKE_OPTIONS
    exit_if_fail $?

    echo "Build finish time: `date`"
    finish_time=`date +%s`
    used_time=$((finish_time-start_time))
    echo "Build elapsed time: $((used_time/60))m $((used_time%60))s"
}

#####################
## test
#####################
function usage_test()
{
    echo "Options for subcommand 'test':"
    echo "   -h|--help         print the help info"
    echo "   -m|--modules      specify modules to test, split by ',',"
    echo "                     e.g., \"pegasus_unit_test,dsn_runtime_tests,dsn_meta_state_tests\","
    echo "                     if not set, then run all tests"
    echo "   -k|--keep_onebox  whether keep the onebox after the test[default false]"
}
function run_test()
{
    local test_modules=""
    local clear_flags="1"
    local enable_gcov="no"
    local all_tests=(
      backup_restore_test
      base_api_test
      base_test
      bulk_load_test
      detect_hotspot_test
      dsn_aio_test
      dsn_block_service_test
      dsn.failure_detector.tests
      dsn_http_test
      dsn_meta_state_tests
      dsn.meta.test
      dsn_nfs_test
      dsn_perf_counter_test
      dsn_replica_backup_test
      dsn_replica_bulk_load_test
      dsn_replica_dup_test
      dsn_replica_split_test
      dsn.replica.test
      dsn_replication_common_test
      dsn.replication.simple_kv
      dsn.rep_tests.simple_kv
      dsn_runtime_tests
      dsn_utils_tests
      dsn.zookeeper.tests
      partition_split_test
      pegasus_geo_test
      pegasus_rproxy_test
      pegasus_unit_test
      recovery_test
      restore_test
      throttle_test
    )
    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_test
                exit 0
                ;;
            -m|--modules)
                test_modules=$2
                shift
                ;;
            -k|--keep_onebox)
                clear_flags=""
                ;;
            --enable_gcov)
                enable_gcov="yes"
                ;;
            *)
                echo "Error: unknown option \"$key\""
                echo
                usage_test
                exit 1
                ;;
        esac
        shift
    done

    echo "Test start time: `date`"
    start_time=`date +%s`

    if [ ! -d "$REPORT_DIR" ]; then
        mkdir -p $REPORT_DIR
    fi

    BUILD_DIR=$ROOT/src/builder
    if [ "$test_modules" == "" ]; then
        test_modules=$(IFS=,; echo "${all_tests[*]}")
    fi
    echo "test_modules=$test_modules"

    # download bulk load test data
    if [[ "$test_modules" =~ "bulk_load_test" && ! -d "$ROOT/src/test/function_test/bulk_load_test/pegasus-bulk-load-function-test-files" ]]; then
        echo "Start to download files used for bulk load function test"
        wget "https://github.com/XiaoMi/pegasus-common/releases/download/deps/pegasus-bulk-load-function-test-files.zip"
        unzip "pegasus-bulk-load-function-test-files.zip" -d "$ROOT/src/test/function_test/bulk_load_test"
        rm "pegasus-bulk-load-function-test-files.zip"
        echo "Prepare files used for bulk load function test succeed"
    fi

    for module in `echo $test_modules | sed 's/,/ /g'`; do
        echo "====================== run $module =========================="
        # restart onebox when test pegasus
        local need_onebox_tests=(
          backup_restore_test
          base_api_test
          bulk_load_test
          detect_hotspot_test
          partition_split_test
          pegasus_geo_test
          pegasus_rproxy_test
          recovery_test
          restore_test
          throttle_test
        )
        if [[ "${need_onebox_tests[@]}" =~ "${test_modules}" ]]; then
            run_clear_onebox
            m_count=3
            if [ "${test_modules}" == "recovery_test" ]; then
                m_count=1
                opts="meta_state_service_type=meta_state_service_simple,distributed_lock_service_type=distributed_lock_service_simple"
            fi
            if [ "${test_modules}" == "backup_restore_test" ]; then
                opts="cold_backup_disabled=false,cold_backup_checkpoint_reserve_minutes=0,cold_backup_root=onebox"
            fi
            if [ "${test_modules}" == "restore_test" ]; then
                opts="cold_backup_disabled=false,cold_backup_checkpoint_reserve_minutes=0,cold_backup_root=mycluster"
            fi
            if ! run_start_onebox -m ${m_count} -w -c --opts ${opts}; then
                echo "ERROR: unable to continue on testing because starting onebox failed"
                exit 1
            fi
            # TODO(yingchun): remove it?
            sed -i "s/@LOCAL_HOSTNAME@/${LOCAL_HOSTNAME}/g"  $ROOT/src/builder/src/server/test/config.ini
        else
            run_stop_zk
            run_start_zk
        fi
        pushd $ROOT/src/builder/bin/$module
        REPORT_DIR=$REPORT_DIR TEST_BIN=$module ./run.sh
        if [ $? != 0 ]; then
            echo "run test \"$module\" in `pwd` failed"
            exit 1
        fi
        # clear onebox if needed
        if [[ "${need_onebox_tests[@]}"  =~ "${test_modules}" ]]; then
            if [ "$clear_flags" == "1" ]; then
                run_clear_onebox
            fi
        fi
        popd
    done

    echo "Test finish time: `date`"
    finish_time=`date +%s`
    used_time=$((finish_time-start_time))
    echo "Test elapsed time: $((used_time/60))m $((used_time%60))s"

    if [ "$enable_gcov" == "yes" ]; then
        echo "Generating gcov report..."
        cd $ROOT
        mkdir -p "$ROOT/gcov_report"

        echo "Running gcovr to produce HTML code coverage report."
        $BUILD_DIR
        gcovr --html --html-details -r $ROOT --object-directory=$BUILD_DIR \
              -o $GCOV_DIR/index.html
        if [ $? -ne 0 ]; then
            exit 1
        fi
    fi
}

#####################
## start_zk
#####################
function usage_start_zk()
{
    echo "Options for subcommand 'start_zk':"
    echo "   -h|--help         print the help info"
    echo "   -d|--install_dir <dir>"
    echo "                     zookeeper install directory,"
    echo "                     if not set, then default is './.zk_install'"
    echo "   -p|--port <port>  listen port of zookeeper, default is 22181"
}

function run_start_zk()
{
    # first we check the environment that zk need: java and nc command
    # check java
    type java >/dev/null 2>&1 || { echo >&2 "start zk failed, need install jre..."; exit 1;}

    # check nc command
    type nc >/dev/null 2>&1 || { echo >&2 "start zk failed, need install netcat command..."; exit 1;}

    INSTALL_DIR=`pwd`/.zk_install
    PORT=22181
    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_start_zk
                exit 0
                ;;
            -d|--install_dir)
                INSTALL_DIR=$2
                shift
                ;;
            -p|--port)
                PORT=$2
                shift
                ;;
            *)
                echo "ERROR: unknown option \"$key\""
                echo
                usage_start_zk
                exit 1
                ;;
        esac
        shift
    done

    INSTALL_DIR="$INSTALL_DIR" PORT="$PORT" $ROOT/scripts/start_zk.sh
}

#####################
## stop_zk
#####################
function usage_stop_zk()
{
    echo "Options for subcommand 'stop_zk':"
    echo "   -h|--help         print the help info"
    echo "   -d|--install_dir <dir>"
    echo "                     zookeeper install directory,"
    echo "                     if not set, then default is './.zk_install'"
}
function run_stop_zk()
{
    INSTALL_DIR=`pwd`/.zk_install
    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_stop_zk
                exit 0
                ;;
            -d|--install_dir)
                INSTALL_DIR=$2
                shift
                ;;
            *)
                echo "ERROR: unknown option \"$key\""
                echo
                usage_stop_zk
                exit 1
                ;;
        esac
        shift
    done
    INSTALL_DIR="$INSTALL_DIR" $ROOT/scripts/stop_zk.sh
}

#####################
## clear_zk
#####################
function usage_clear_zk()
{
    echo "Options for subcommand 'clear_zk':"
    echo "   -h|--help         print the help info"
    echo "   -d|--install_dir <dir>"
    echo "                     zookeeper install directory,"
    echo "                     if not set, then default is './.zk_install'"
}
function run_clear_zk()
{
    INSTALL_DIR=`pwd`/.zk_install
    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_clear_zk
                exit 0
                ;;
            -d|--install_dir)
                INSTALL_DIR=$2
                shift
                ;;
            *)
                echo "ERROR: unknown option \"$key\""
                echo
                usage_clear__zk
                exit 1
                ;;
        esac
        shift
    done
    INSTALL_DIR="$INSTALL_DIR" $ROOT/scripts/clear_zk.sh
}

#####################
## start_onebox
#####################
function usage_start_onebox()
{
    echo "Options for subcommand 'start_onebox':"
    echo "   -h|--help         print the help info"
    echo "   -m|--meta_count <num>"
    echo "                     meta server count, default is 3"
    echo "   -r|--replica_count <num>"
    echo "                     replica server count, default is 3"
    echo "   -c|--collector"
    echo "                     start the collector server, default not start"
    echo "   -a|--app_name <str>"
    echo "                     default app name, default is temp"
    echo "   -p|--partition_count <num>"
    echo "                     default app partition count, default is 8"
    echo "   -w|--wait_healthy"
    echo "                     wait cluster to become healthy, default not wait"
    echo "   -s|--server_path <str>"
    echo "                     server binary path, default is ${DSN_ROOT}/bin/pegasus_server"
    echo "   --config_path"
    echo "                     specify the config template path, default is ./src/server/config.min.ini in non-production env"
    echo "                                                                  ./src/server/config.ini in production env"
    echo "   --use_product_config"
    echo "                     use the product config template"
    echo "   --opts"
    echo "                     update configs before start onebox, the configs are in the form of 'key1=value1,key2=value2'"
}

function run_start_onebox()
{
    META_COUNT=3
    REPLICA_COUNT=3
    COLLECTOR_COUNT=0
    APP_NAME=temp
    PARTITION_COUNT=8
    WAIT_HEALTHY=false
    SERVER_PATH=${DSN_ROOT}/bin/pegasus_server
    CONFIG_FILE=""
    USE_PRODUCT_CONFIG=false
    OPTS=""

    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_start_onebox
                exit 0
                ;;
            -m|--meta_count)
                META_COUNT="$2"
                shift
                ;;
            -r|--replica_count)
                REPLICA_COUNT="$2"
                shift
                ;;
            -c|--collector)
                COLLECTOR_COUNT=1
                ;;
            -a|--app_name)
                APP_NAME="$2"
                shift
                ;;
            -p|--partition_count)
                PARTITION_COUNT="$2"
                shift
                ;;
            -w|--wait_healthy)
                WAIT_HEALTHY=true
                ;;
            -s|--server_path)
                SERVER_PATH="$2"
                shift
                ;;
            --config_path)
                CONFIG_FILE="$2"
                shift
                ;;
            --use_product_config)
                USE_PRODUCT_CONFIG=true
                ;;
            --opts)
                OPTS="$2"
                shift
                ;;
            *)
                echo "ERROR: unknown option \"$key\""
                echo
                usage_start_onebox
                exit 1
                ;;
        esac
        shift
    done

    if [ ! -f ${SERVER_PATH}/pegasus_server ]; then
        echo "ERROR: file ${SERVER_PATH}/pegasus_server not exist"
        exit 1
    fi
    if ps -ef | grep '/pegasus_server config.ini' | grep -E -q 'app_list meta|app_list replica|app_list collector'; then
        echo "ERROR: some onebox processes are running, start failed"
        exit 1
    fi
    ln -s -f "${SERVER_PATH}/pegasus_server" "${ROOT}"

    if ! run_start_zk; then
        echo "ERROR: unable to setup onebox because zookeeper can not be started"
        exit 1
    fi

    if [ $USE_PRODUCT_CONFIG == "true" ]; then
        [ -z "${CONFIG_FILE}" ] && CONFIG_FILE=${ROOT}/src/server/config.ini
        [ ! -f "${CONFIG_FILE}" ] && { echo "${CONFIG_FILE} is not exist"; exit 1; }
        cp -f ${CONFIG_FILE} ${ROOT}/config-server.ini
        sed -i 's/\<34601\>/@META_PORT@/' ${ROOT}/config-server.ini
        sed -i 's/\<34801\>/@REPLICA_PORT@/' ${ROOT}/config-server.ini
        sed -i 's/%{cluster.name}/onebox/g' ${ROOT}/config-server.ini
        sed -i 's/%{network.interface}/eth0/g' ${ROOT}/config-server.ini
        sed -i 's/%{email.address}//g' ${ROOT}/config-server.ini
        sed -i 's/%{app.dir}/.\/data/g' ${ROOT}/config-server.ini
        sed -i 's/%{slog.dir}//g' ${ROOT}/config-server.ini
        sed -i 's/%{data.dirs}//g' ${ROOT}/config-server.ini
        sed -i 's@%{home.dir}@'"$HOME"'@g' ${ROOT}/config-server.ini
        for i in $(seq ${META_COUNT})
        do
            meta_port=$((34600+i))
            if [ $i -eq 1 ]; then
                meta_list="${LOCAL_HOSTNAME}:$meta_port"
            else
                meta_list="$meta_list,${LOCAL_HOSTNAME}:$meta_port"
            fi
        done
        sed -i 's/%{meta.server.list}/'"$meta_list"'/g' ${ROOT}/config-server.ini
        sed -i 's/%{zk.server.list}/'"${LOCAL_HOSTNAME}"':22181/g' ${ROOT}/config-server.ini
        sed -i 's/app_name = .*$/app_name = '"$APP_NAME"'/' ${ROOT}/config-server.ini
        sed -i 's/partition_count = .*$/partition_count = '"$PARTITION_COUNT"'/' ${ROOT}/config-server.ini
    else
        [ -z "${CONFIG_FILE}" ] && CONFIG_FILE=${ROOT}/src/server/config.min.ini
        [ ! -f "${CONFIG_FILE}" ] && { echo "${CONFIG_FILE} is not exist"; exit 1; }
        sed "s/@LOCAL_HOSTNAME@/${LOCAL_HOSTNAME}/g;s/@APP_NAME@/${APP_NAME}/g;s/@PARTITION_COUNT@/${PARTITION_COUNT}/g" \
            ${CONFIG_FILE} >${ROOT}/config-server.ini
    fi

    OPTS=`echo $OPTS | xargs`
    config_kvs=(${OPTS//,/ })
    for config_kv in ${config_kvs[@]}; do
        config_kv=`echo $config_kv | xargs`
        kv=(${config_kv//=/ })
        if [ ! ${#kv[*]} -eq 2 ]; then
            echo "Invalid --opts arguments!"
            exit 1
        fi
        sed -i '/^\s*'"${kv[0]}"'/c '"${kv[0]}"' = '"${kv[1]}" ${ROOT}/config-server.ini
    done

    echo "starting server"
    ld_library_path=${SERVER_PATH}:$LD_LIBRARY_PATH
    export LD_LIBRARY_PATH=$ld_library_path
    mkdir -p onebox
    cd onebox
    for i in $(seq ${META_COUNT})
    do
        meta_port=$((34600+i))
        prometheus_port=$((9091+i))
        mkdir -p meta$i;
        cd meta$i
        ln -s -f ${SERVER_PATH}/pegasus_server pegasus_server
        sed "s/@META_PORT@/$meta_port/;s/@REPLICA_PORT@/34800/;s/@PROMETHEUS_PORT@/$prometheus_port/" ${ROOT}/config-server.ini >config.ini
        echo "cd `pwd` && $PWD/pegasus_server config.ini -app_list meta &>result &"
        $PWD/pegasus_server config.ini -app_list meta &>result &
        PID=$!
        ps -ef | grep '/pegasus_server config.ini' | grep "\<$PID\>"
        cd ..
    done
    for j in $(seq ${REPLICA_COUNT})
    do
        prometheus_port=$((9091+${META_COUNT}+j))
        replica_port=$((34800+j))
        mkdir -p replica$j
        cd replica$j
        ln -s -f ${SERVER_PATH}/pegasus_server pegasus_server
        sed "s/@META_PORT@/34600/;s/@REPLICA_PORT@/$replica_port/;s/@PROMETHEUS_PORT@/$prometheus_port/" ${ROOT}/config-server.ini >config.ini
        echo "cd `pwd` && $PWD/pegasus_server config.ini -app_list replica &>result &"
        $PWD/pegasus_server config.ini -app_list replica &>result &
        PID=$!
        ps -ef | grep '/pegasus_server config.ini' | grep "\<$PID\>"
        cd ..
    done
    if [ $COLLECTOR_COUNT -eq 1 ]
    then
        mkdir -p collector
        cd collector
        ln -s -f ${SERVER_PATH}/pegasus_server pegasus_server
        sed "s/@META_PORT@/34600/;s/@REPLICA_PORT@/34800/;s/@PROMETHEUS_PORT@/9091/" ${ROOT}/config-server.ini >config.ini
        echo "cd `pwd` && $PWD/pegasus_server config.ini -app_list collector &>result &"
        $PWD/pegasus_server config.ini -app_list collector &>result &
        PID=$!
        ps -ef | grep '/pegasus_server config.ini' | grep "\<$PID\>"
        cd ..
    fi

    if [ $WAIT_HEALTHY == "true" ]; then
        cd $ROOT
        echo "Wait cluster to become healthy..."
        sleeped=0
        while true; do
            sleep 1
            sleeped=$((sleeped+1))
            echo "Sleeped for $sleeped seconds"
            unhealthy_count=`echo "ls -d" | ./run.sh shell | awk 'f{ if(NF<7){f=0} else if($3!=$4){print} } / fully_healthy /{print;f=1}' | wc -l`
            if [ $unhealthy_count -eq 1 ]; then
                echo "Cluster becomes healthy."
                break
            fi
        done
    fi
}

#####################
## stop_onebox
#####################
function usage_stop_onebox()
{
    echo "Options for subcommand 'stop_onebox':"
    echo "   -h|--help         print the help info"
}

function run_stop_onebox()
{
    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_stop_onebox
                exit 0
                ;;
            *)
                echo "ERROR: unknown option \"$key\""
                echo
                usage_stop_onebox
                exit 1
                ;;
        esac
        shift
    done
    ps -ef | grep '/pegasus_server config.ini' | grep -E 'app_list meta|app_list replica|app_list collector' | awk '{print $2}' | xargs kill &>/dev/null || true
}

#####################
## list_onebox
#####################
function usage_list_onebox()
{
    echo "Options for subcommand 'list_onebox':"
    echo "   -h|--help         print the help info"
}

function run_list_onebox()
{
    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_list_onebox
                exit 0
                ;;
            *)
                echo "ERROR: unknown option \"$key\""
                echo
                usage_list_onebox
                exit 1
                ;;
        esac
        shift
    done
    ps -ef | grep '/pegasus_server config.ini' | grep -E 'app_list meta|app_list replica|app_list collector' | sort -k11
}

#####################
## clear_onebox
#####################
function usage_clear_onebox()
{
    echo "Options for subcommand 'clear_onebox':"
    echo "   -h|--help         print the help info"
}

function run_clear_onebox()
{
    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_clear_onebox
                exit 0
                ;;
            *)
                echo "ERROR: unknown option \"$key\""
                echo
                usage_clear_onebox
                exit 1
                ;;
        esac
        shift
    done
    run_stop_onebox
    run_clear_zk
    rm -rf onebox *.log *.data config-*.ini &>/dev/null
    sleep 1
}

#####################
## start_onebox_instance
#####################
function usage_start_onebox_instance()
{
    echo "Options for subcommand 'start_onebox_instance':"
    echo "   -h|--help              print the help info"
    echo "   -m|--meta_id <num>     start meta server with id"
    echo "   -r|--replica_id <num>  start replica server with id"
    echo "   -c|--collector         start collector server"
}

function run_start_onebox_instance()
{
    META_ID=0
    REPLICA_ID=0
    COLLECTOR_ID=0
    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_start_onebox_instance
                exit 0
                ;;
            -m|--meta_id)
                META_ID="$2"
                shift
                ;;
            -r|--replica_id)
                REPLICA_ID="$2"
                shift
                ;;
            -c|--collector)
                COLLECTOR_ID=1
                ;;
            *)
                echo "ERROR: unknown option \"$key\""
                echo
                usage_start_onebox_instance
                exit 1
                ;;
        esac
        shift
    done
    if [ $META_ID = "0" -a $REPLICA_ID = "0" -a $COLLECTOR_ID = "0" ]; then
        echo "ERROR: no meta_id or replica_id or collector set"
        exit 1
    fi
    if [ $META_ID != "0" ]; then
        dir=onebox/meta$META_ID
        if [ ! -d $dir ]; then
            echo "ERROR: invalid meta_id"
            exit 1
        fi
        if ps -ef | grep "/meta$META_ID/pegasus_server config.ini" | grep "app_list meta" ; then
            echo "INFO: meta@$META_ID already running"
            exit 1
        fi
        cd $dir
        echo "cd `pwd` && $PWD/pegasus_server config.ini -app_list meta &>result &"
        $PWD/pegasus_server config.ini -app_list meta &>result &
        PID=$!
        ps -ef | grep '/pegasus_server config.ini' | grep "\<$PID\>"
        cd ..
        echo "INFO: meta@$META_ID started"
    fi
    if [ $REPLICA_ID != "0" ]; then
        dir=onebox/replica$REPLICA_ID
        if [ ! -d $dir ]; then
            echo "ERROR: invalid replica_id"
            exit 1
        fi
        if ps -ef | grep "/replica$REPLICA_ID/pegasus_server config.ini" | grep "app_list replica" ; then
            echo "INFO: replica@$REPLICA_ID already running"
            exit 1
        fi
        cd $dir
        echo "cd `pwd` && $PWD/pegasus_server config.ini -app_list replica &>result &"
        $PWD/pegasus_server config.ini -app_list replica &>result &
        PID=$!
        ps -ef | grep '/pegasus_server config.ini' | grep "\<$PID\>"
        cd ..
        echo "INFO: replica@$REPLICA_ID started"
    fi
    if [ $COLLECTOR_ID != "0" ]; then
        dir=onebox/collector
        if [ ! -d $dir ]; then
            echo "ERROR: collector dir $dir not exist"
            exit 1
        fi
        if ps -ef | grep "/collector/pegasus_server config.ini" | grep "app_list collector" ; then
            echo "INFO: collector already running"
            exit 1
        fi
        cd $dir
        echo "cd `pwd` && $PWD/pegasus_server config.ini -app_list collector &>result &"
        $PWD/pegasus_server config.ini -app_list collector &>result &
        PID=$!
        ps -ef | grep '/pegasus_server config.ini' | grep "\<$PID\>"
        cd ..
        echo "INFO: collector started"
    fi
}

#####################
## stop_onebox_instance
#####################
function usage_stop_onebox_instance()
{
    echo "Options for subcommand 'stop_onebox_instance':"
    echo "   -h|--help              print the help info"
    echo "   -m|--meta_id <num>     stop meta server with id"
    echo "   -r|--replica_id <num>  stop replica server with id"
    echo "   -c|--collector         stop collector server"
}

function run_stop_onebox_instance()
{
    META_ID=0
    REPLICA_ID=0
    COLLECTOR_ID=0
    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_stop_onebox_instance
                exit 0
                ;;
            -m|--meta_id)
                META_ID="$2"
                shift
                ;;
            -r|--replica_id)
                REPLICA_ID="$2"
                shift
                ;;
            -c|--collector)
                COLLECTOR_ID=1
                ;;
            *)
                echo "ERROR: unknown option \"$key\""
                echo
                usage_stop_onebox_instance
                exit 1
                ;;
        esac
        shift
    done
    if [ $META_ID = "0" -a $REPLICA_ID = "0" -a $COLLECTOR_ID = "0" ]; then
        echo "ERROR: no meta_id or replica_id or collector set"
        exit 1
    fi
    if [ $META_ID != "0" ]; then
        dir=onebox/meta$META_ID
        if [ ! -d $dir ]; then
            echo "ERROR: invalid meta_id"
            exit 1
        fi
        if ! ps -ef | grep "/meta$META_ID/pegasus_server config.ini" | grep "app_list meta" ; then
            echo "INFO: meta@$META_ID is not running"
            exit 1
        fi
        ps -ef | grep "/meta$META_ID/pegasus_server config.ini" | grep "app_list meta" | awk '{print $2}' | xargs kill &>/dev/null || true
        echo "INFO: meta@$META_ID stopped"
    fi
    if [ $REPLICA_ID != "0" ]; then
        dir=onebox/replica$REPLICA_ID
        if [ ! -d $dir ]; then
            echo "ERROR: invalid replica_id"
            exit 1
        fi
        if ! ps -ef | grep "/replica$REPLICA_ID/pegasus_server config.ini" | grep "app_list replica" ; then
            echo "INFO: replica@$REPLICA_ID is not running"
            exit 1
        fi
        ps -ef | grep "/replica$REPLICA_ID/pegasus_server config.ini" | grep "app_list replica" | awk '{print $2}' | xargs kill &>/dev/null || true
        echo "INFO: replica@$REPLICA_ID stopped"
    fi
    if [ $COLLECTOR_ID != "0" ]; then
        if ! ps -ef | grep "/collector/pegasus_server config.ini" | grep "app_list collector" ; then
            echo "INFO: collector is not running"
            exit 1
        fi
        ps -ef | grep "/collector/pegasus_server config.ini" | grep "app_list collector" | awk '{print $2}' | xargs kill &>/dev/null || true
        echo "INFO: collector stopped"
    fi
}

#####################
## restart_onebox_instance
#####################
function usage_restart_onebox_instance()
{
    echo "Options for subcommand 'restart_onebox_instance':"
    echo "   -h|--help              print the help info"
    echo "   -m|--meta_id <num>     restart meta server with id"
    echo "   -r|--replica_id <num>  restart replica server with id"
    echo "   -c|--collector         restart collector server"
    echo "   -s|--sleep <num>       sleep time in seconds between stop and start, default is 1"
}

function run_restart_onebox_instance()
{
    META_ID=0
    REPLICA_ID=0
    COLLECTOR_OPT=""
    SLEEP=1
    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_restart_onebox_instance
                exit 0
                ;;
            -m|--meta_id)
                META_ID="$2"
                shift
                ;;
            -r|--replica_id)
                REPLICA_ID="$2"
                shift
                ;;
            -c|--collector)
                COLLECTOR_OPT="-c"
                shift
                ;;
            -s|--sleep)
                SLEEP="$2"
                shift
                ;;
            *)
                echo "ERROR: unknown option \"$key\""
                echo
                usage_restart_onebox_instance
                exit 1
                ;;
        esac
        shift
    done
    if [ $META_ID = "0" -a $REPLICA_ID = "0" -a "x$COLLECTOR_OPT" = "x" ]; then
        echo "ERROR: no meta_id or replica_id or collector set"
        exit 1
    fi
    run_stop_onebox_instance -m $META_ID -r $REPLICA_ID $COLLECTOR_OPT
    echo "sleep $SLEEP"
    sleep $SLEEP
    run_start_onebox_instance -m $META_ID -r $REPLICA_ID $COLLECTOR_OPT
}

#####################
## start_kill_test
#####################
function usage_start_kill_test()
{
    echo "Options for subcommand 'start_kill_test':"
    echo "   -h|--help         print the help info"
    echo "   -m|--meta_count <num>"
    echo "                     meta server count, default is 3"
    echo "   -r|--replica_count <num>"
    echo "                     replica server count, default is 5"
    echo "   -a|--app_name <str>"
    echo "                     app name, default is temp"
    echo "   -p|--partition_count <num>"
    echo "                     app partition count, default is 16"
    echo "   -t|--kill_type <str>"
    echo "                     kill type: meta | replica | all, default is all"
    echo "   -s|--sleep_time <num>"
    echo "                     max sleep time before next kill, default is 10"
    echo "                     actual sleep time will be a random value in range of [1, sleep_time]"
    echo "   -w|--worker_count <num>"
    echo "                     worker count for concurrently setting value, default is 10"
    echo "   -k|--killer_type <str>"
    echo "                     killer type: process_killer | partition_killer, default is process_killer"
}

function run_start_kill_test()
{
    META_COUNT=3
    REPLICA_COUNT=5
    APP_NAME=temp
    PARTITION_COUNT=16
    KILL_TYPE=all
    SLEEP_TIME=10
    THREAD_COUNT=10
    KILLER_TYPE=process_killer
    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_start_kill_test
                exit 0
                ;;
            -m|--meta_count)
                META_COUNT="$2"
                shift
                ;;
            -r|--replica_count)
                REPLICA_COUNT="$2"
                shift
                ;;
            -a|--app_name)
                APP_NAME="$2"
                shift
                ;;
            -p|--partition_count)
                PARTITION_COUNT="$2"
                shift
                ;;
            -t|--kill_type)
                KILL_TYPE="$2"
                shift
                ;;
            -s|--sleep_time)
                SLEEP_TIME="$2"
                shift
                ;;
            -w|--worker_count)
                THREAD_COUNT="$2"
                shift
                ;;
            -k|--killer_type)
                KILLER_TYPE="$2"
                shift
                ;;
            *)
                echo "ERROR: unknown option \"$key\""
                echo
                usage_start_kill_test
                exit 1
                ;;
        esac
        shift
    done

    run_start_onebox -m $META_COUNT -r $REPLICA_COUNT -a $APP_NAME -p $PARTITION_COUNT

    cd $ROOT
    CONFIG=config-kill-test.ini

    sed "s/@LOCAL_HOSTNAME@/${LOCAL_HOSTNAME}/g;\
s/@META_COUNT@/${META_COUNT}/g;\
s/@REPLICA_COUNT@/${REPLICA_COUNT}/g;\
s/@ZK_COUNT@/1/g;s/@APP_NAME@/${APP_NAME}/g;\
s/@SET_THREAD_COUNT@/${THREAD_COUNT}/g;\
s/@GET_THREAD_COUNT@/${THREAD_COUNT}/g;\
s+@ONEBOX_RUN_PATH@+`pwd`+g" ${ROOT}/src/test/kill_test/config.ini >$CONFIG

    # start verifier
    mkdir -p onebox/verifier && cd onebox/verifier
    ln -s -f ${DSN_ROOT}/bin/pegasus_kill_test/pegasus_kill_test
    ln -s -f ${ROOT}/$CONFIG config.ini
    echo "$PWD/pegasus_kill_test config.ini verifier &>/dev/null &"
    $PWD/pegasus_kill_test config.ini verifier &>/dev/null &
    PID=$!
    ps -ef | grep '/pegasus_kill_test config.ini verifier' | grep "\<$PID\>"
    sleep 0.2
    cd ${ROOT}

    #start killer
    mkdir -p onebox/killer && cd onebox/killer
    ln -s -f ${DSN_ROOT}/bin/pegasus_kill_test/pegasus_kill_test
    ln -s -f ${ROOT}/$CONFIG config.ini
    echo "$PWD/pegasus_kill_test config.ini $KILLER_TYPE &>/dev/null &"
    $PWD/pegasus_kill_test config.ini $KILLER_TYPE &>/dev/null &
    PID=$!
    ps -ef | grep '/pegasus_kill_test config.ini p*' | grep "\<$PID\>"
    sleep 0.2
    cd ${ROOT}
    run_list_kill_test
}

#####################
## stop_kill_test
#####################
function usage_stop_kill_test()
{
    echo "Options for subcommand 'stop_kill_test':"
    echo "   -h|--help         print the help info"
}

function run_stop_kill_test()
{
    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_stop_kill_test
                exit 0
                ;;
            *)
                echo "ERROR: unknown option \"$key\""
                echo
                usage_stop_kill_test
                exit 1
                ;;
        esac
        shift
    done

    ps -ef | grep '/pegasus_kill_test ' | awk '{print $2}' | xargs kill &>/dev/null || true
    run_stop_onebox
}

#####################
## list_kill_test
#####################
function usage_list_kill_test()
{
    echo "Options for subcommand 'list_kill_test':"
    echo "   -h|--help         print the help info"
}

function run_list_kill_test()
{
    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_list_kill_test
                exit 0
                ;;
            *)
                echo "ERROR: unknown option \"$key\""
                echo
                usage_list_kill_test
                exit 1
                ;;
        esac
        shift
    done
    echo "------------------------------"
    run_list_onebox
    ps -ef | grep '/pegasus_kill_test ' | grep -v grep
    echo "------------------------------"
    echo "Server dir: ./onebox"
    echo "------------------------------"
}

#####################
## clear_kill_test
#####################
function usage_clear_kill_test()
{
    echo "Options for subcommand 'clear_kill_test':"
    echo "   -h|--help         print the help info"
}

function run_clear_kill_test()
{
    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_clear_kill_test
                exit 0
                ;;
            *)
                echo "ERROR: unknown option \"$key\""
                echo
                usage_clear_kill_test
                exit 1
                ;;
        esac
        shift
    done
    run_stop_kill_test
    run_clear_onebox
    rm -rf kill_history.txt *.data config-*.ini &>/dev/null
}

#####################
## start_upgrade_test
#####################
function usage_start_upgrade_test()
{
    echo "Options for subcommand 'start_upgrade_test':"
    echo "   -h|--help         print the help info"
    echo "   -m|--meta_count <num>"
    echo "                     meta server count, default is 3"
    echo "   -r|--replica_count <num>"
    echo "                     replica server count, default is 5"
    echo "   -a|--app_name <str>"
    echo "                     app name, default is temp"
    echo "   -p|--partition_count <num>"
    echo "                     app partition count, default is 16"
    echo "   --upgrade_type <str>"
    echo "                     upgrade type: meta | replica | all, default is all"
    echo "   -o|--old_version_path <str>"
    echo "                     old server binary and library path"
    echo "   -n|--new_version_path <str>"
    echo "                     new server binary and library path"
    echo "   -s|--sleep_time <num>"
    echo "                     max sleep time before next update, default is 10"
    echo "                     actual sleep time will be a random value in range of [1, sleep_time]"
    echo "   -w|--worker_count <num>"
    echo "                     worker count for concurrently setting value, default is 10"
}

function run_start_upgrade_test()
{
    META_COUNT=3
    REPLICA_COUNT=5
    APP_NAME=temp
    PARTITION_COUNT=16
    UPGRADE_TYPE=replica
    OLD_VERSION_PATH=
    NEW_VERSION_PATH=
    SLEEP_TIME=10
    THREAD_COUNT=10
    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_start_upgrade_test
                exit 0
                ;;
            -m|--meta_count)
                META_COUNT="$2"
                shift
                ;;
            -r|--replica_count)
                REPLICA_COUNT="$2"
                shift
                ;;
            -a|--app_name)
                APP_NAME="$2"
                shift
                ;;
            -p|--partition_count)
                PARTITION_COUNT="$2"
                shift
                ;;
            -t|--upgrade_type)
                UPGRADE_TYPE="$2"
                shift
                ;;
            -o|--old_version_path)
                OLD_VERSION_PATH="${ROOT}/$2"
                shift
                ;;
            -n|--new_version_path)
                NEW_VERSION_PATH="${ROOT}/$2"
                shift
                ;;
            -s|--sleep_time)
                SLEEP_TIME="$2"
                shift
                ;;
            -w|--worker_count)
                THREAD_COUNT="$2"
                shift
                ;;
            *)
                echo "ERROR: unknown option \"$key\""
                echo
                usage_start_upgrade_test
                exit 1
                ;;
        esac
        shift
    done

    run_start_onebox -m $META_COUNT -r $REPLICA_COUNT -a $APP_NAME -p $PARTITION_COUNT -s $OLD_VERSION_PATH
    echo

    cd $ROOT
    CONFIG=config-upgrade-test.ini

    sed "s/@LOCAL_HOSTNAME@/${LOCAL_HOSTNAME}/g;\
s/@META_COUNT@/${META_COUNT}/g;\
s/@REPLICA_COUNT@/${REPLICA_COUNT}/g;\
s/@ZK_COUNT@/1/g;s/@APP_NAME@/${APP_NAME}/g;\
s/@SET_THREAD_COUNT@/${THREAD_COUNT}/g;\
s/@GET_THREAD_COUNT@/${THREAD_COUNT}/g;\
s+@ONEBOX_RUN_PATH@+`pwd`+g; \
s+@OLD_VERSION_PATH@+${OLD_VERSION_PATH}+g;\
s+@NEW_VERSION_PATH@+${NEW_VERSION_PATH}+g " ${ROOT}/src/test/upgrade_test/config.ini >$CONFIG

    # start verifier
    mkdir -p onebox/verifier && cd onebox/verifier
    ln -s -f ${DSN_ROOT}/bin/pegasus_upgrade_test/pegasus_upgrade_test
    ln -s -f ${ROOT}/$CONFIG config.ini
    echo "./pegasus_upgrade_test config.ini verifier &>/dev/null &"
    ./pegasus_upgrade_test config.ini verifier &>/dev/null &
    sleep 0.2
    echo
    cd ${ROOT}

    #start upgrader
    mkdir -p onebox/upgrader && cd onebox/upgrader
    ln -s -f ${DSN_ROOT}/bin/pegasus_upgrade_test/pegasus_upgrade_test
    ln -s -f ${ROOT}/$CONFIG config.ini
    echo "./pegasus_upgrade_test config.ini upgrader &>/dev/null &"
    ./pegasus_upgrade_test config.ini upgrader &>/dev/null &
    sleep 0.2
    echo
    cd ${ROOT}

    run_list_upgrade_test
}

#####################
## stop_upgrade_test
#####################
function usage_stop_upgrade_test()
{
    echo "Options for subcommand 'stop_upgrade_test':"
    echo "   -h|--help         print the help info"
}

function run_stop_upgrade_test()
{
    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_stop_upgrade_test
                exit 0
                ;;
            *)
                echo "ERROR: unknown option \"$key\""
                echo
                usage_stop_upgrade_test
                exit 1
                ;;
        esac
        shift
    done

    ps -ef | grep ' \./pegasus_upgrade_test ' | awk '{print $2}' | xargs kill &>/dev/null || true
    run_stop_onebox
}

#####################
## list_upgrade_test
#####################
function usage_list_upgrade_test()
{
    echo "Options for subcommand 'list_upgrade_test':"
    echo "   -h|--help         print the help info"
}

function run_list_upgrade_test()
{
    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_list_upgrade_test
                exit 0
                ;;
            *)
                echo "ERROR: unknown option \"$key\""
                echo
                usage_list_upgrade_test
                exit 1
                ;;
        esac
        shift
    done
    echo "------------------------------"
    run_list_onebox
    ps -ef | grep ' \./pegasus_upgrade_test ' | grep -v grep
    echo "------------------------------"
    echo "Server dir: ./onebox"
    echo "------------------------------"
}

#####################
## clear_upgrade_test
#####################
function usage_clear_upgrade_test()
{
    echo "Options for subcommand 'clear_upgrade_test':"
    echo "   -h|--help         print the help info"
}

function run_clear_upgrade_test()
{
    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_clear_upgrade_test
                exit 0
                ;;
            *)
                echo "ERROR: unknown option \"$key\""
                echo
                usage_clear_upgrade_test
                exit 1
                ;;
        esac
        shift
    done
    run_stop_upgrade_test
    run_clear_onebox
    rm -rf upgrade_history.txt *.data config-*.ini &>/dev/null
}

#####################
## bench
#####################
function usage_bench()
{
    echo "Options for subcommand 'bench':"
    echo "   -h|--help                 print the help info"
    echo "   --type                    benchmark type, supporting:"
    echo "                             fillrandom_pegasus       --pegasus write N random values with random keys list"
    echo "                             readrandom_pegasus       --pegasus read N times with random keys list"
    echo "                             deleterandom_pegasus     --pegasus delete N entries with random keys list"
    echo "                             Comma-separated list of operations is going to run in the specified order."
    echo "                             default is 'fillrandom_pegasus,readrandom_pegasus,deleterandom_pegasus'"
    echo "   --num <num>               number of key/value pairs, default is 10000"
    echo "   --cluster <str>           cluster meta lists, default is '127.0.0.1:34601,127.0.0.1:34602,127.0.0.1:34603'"
    echo "   --app_name <str>          app name, default is 'temp'"
    echo "   --thread_num <num>        number of threads, default is 1"
    echo "   --hashkey_size <num>      hashkey size in bytes, default is 16"
    echo "   --sortkey_size <num>      sortkey size in bytes, default is 16"
    echo "   --value_size <num>        value size in bytes, default is 100"
    echo "   --timeout <num>           timeout in milliseconds, default is 1000"
    echo "   --seed <num>              seed base for random number generator, When 0 it is specified as 1000. default is 1000"
}

function fill_bench_config() {
    sed -i "s/@TYPE@/$TYPE/g" ./config-bench.ini
    sed -i "s/@NUM@/$NUM/g" ./config-bench.ini
    sed -i "s/@CLUSTER@/$CLUSTER/g" ./config-bench.ini
    sed -i "s/@APP@/$APP/g" ./config-bench.ini
    sed -i "s/@THREAD@/$THREAD/g" ./config-bench.ini
    sed -i "s/@HASHKEY_SIZE@/$HASHKEY_SIZE/g" ./config-bench.ini
    sed -i "s/@SORTKEY_SIZE@/$SORTKEY_SIZE/g" ./config-bench.ini
    sed -i "s/@VALUE_SIZE@/$VALUE_SIZE/g" ./config-bench.ini
    sed -i "s/@TIMEOUT_MS@/$TIMEOUT_MS/g" ./config-bench.ini
    sed -i "s/@SEED@/$SEED/g" ./config-bench.ini
}

function run_bench()
{
    TYPE=fillrandom_pegasus,readrandom_pegasus,deleterandom_pegasus
    NUM=10000
    CLUSTER=127.0.0.1:34601,127.0.0.1:34602,127.0.0.1:34603
    APP=temp
    THREAD=1
    HASHKEY_SIZE=16
    SORTKEY_SIZE=16
    VALUE_SIZE=100
    TIMEOUT_MS=1000
    SEED=1000
    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_bench
                exit 0
                ;;
            --type)
                TYPE="$2"
                shift
                ;;
            --num)
                NUM="$2"
                shift
                ;;
            --cluster)
                CLUSTER="$2"
                shift
                ;;
            --app_name)
                APP="$2"
                shift
                ;;
            --thread_num)
                THREAD="$2"
                shift
                ;;
            --hashkey_size)
                HASHKEY_SIZE="$2"
                shift
                ;;
            --sortkey_size)
                SORTKEY_SIZE="$2"
                shift
                ;;
            --value_size)
                VALUE_SIZE="$2"
                shift
                ;;
            --timeout)
                TIMEOUT_MS="$2"
                shift
                ;;
            --seed)
                SEED="$2"
                shift
                ;;
            *)
                echo "ERROR: unknown option \"$key\""
                echo
                usage_bench
                exit 1
                ;;
        esac
        shift
    done
    cd ${ROOT}
    cp ${DSN_ROOT}/bin/pegasus_bench/config.ini ./config-bench.ini
    fill_bench_config
    ln -s -f ${DSN_ROOT}/bin/pegasus_bench/pegasus_bench
    ./pegasus_bench ./config-bench.ini
    rm -f ./config-bench.ini
}

#####################
## shell
#####################
function usage_shell()
{
    echo "Options for subcommand 'shell':"
    echo "   -h|--help            print the help info"
    echo "   -c|--config <path>   config file path, default './config-shell.ini.{PID}'"
    echo "   --cluster <str>      cluster meta lists, default '127.0.0.1:34601,127.0.0.1:34602,127.0.0.1:34603'"
    echo "   -n <cluster-name>    cluster name. Will try to get a cluster ip_list"
    echo "                        from your minos config (\$MINOS2_CONFIG_FILE or \$MINOS_CONFIG_FILE) or"
    echo "                        from [uri-resolve.dsn://<cluster-name>] of your config-file"
}

function run_shell()
{
    CONFIG=${ROOT}/config-shell.ini.$PID
    CONFIG_SPECIFIED=0
    CLUSTER=127.0.0.1:34601,127.0.0.1:34602,127.0.0.1:34603
    CLUSTER_SPECIFIED=0
    CLUSTER_NAME=onebox
    CLUSTER_NAME_SPECIFIED=0
    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_shell
                exit 0
                ;;
            -c|--config)
                CONFIG="$2"
                CONFIG_SPECIFIED=1
                shift
                ;;
            -m|--cluster)
                CLUSTER="$2"
                CLUSTER_SPECIFIED=1
                shift
                ;;
            -n|--cluster_name)
                CLUSTER_NAME="$2"
                CLUSTER_NAME_SPECIFIED=1
                shift
                ;;
            *)
                echo "ERROR: unknown option \"$key\""
                echo
                usage_shell
                exit 1
                ;;
        esac
        shift
    done

    if [ ${CLUSTER_SPECIFIED} -eq 1 -a ${CLUSTER_NAME_SPECIFIED} -eq 1 ]; then
        echo "ERROR: can not specify both cluster and cluster_name at the same time"
        echo
        usage_shell
        exit 1
    fi

    if [ ${CLUSTER_SPECIFIED} -eq 1 ]; then
        CLUSTER_NAME="unknown"
    fi

    if [ $CLUSTER_NAME_SPECIFIED -eq 1 ]; then
        meta_section="/tmp/minos.config.cluster.meta.section.$UID"
        if [ ! -z "$MINOS2_CONFIG_FILE" ]; then
            minos2_config_file=$(dirname $MINOS2_CONFIG_FILE)/xiaomi-config/conf/pegasus/pegasus-${CLUSTER_NAME}.yaml
        fi
        if [ ! -z "$MINOS_CONFIG_FILE" ]; then
            minos_config_file=$(dirname $MINOS_CONFIG_FILE)/xiaomi-config/conf/pegasus/pegasus-${CLUSTER_NAME}.cfg
        fi

        if [ ! -z "$MINOS2_CONFIG_FILE" -a -f "$minos2_config_file" ]; then
            meta_section_start=$(grep -n "^ *meta:" $minos2_config_file | head -1 | cut -d":" -f 1)
            meta_section_end=$(grep -n "^ *replica:" $minos2_config_file | head -1 | cut -d":" -f 1)
            sed -n "${meta_section_start},${meta_section_end}p" $minos2_config_file > $meta_section
            if [ $? -ne 0 ]; then
                echo "ERROR: write $minos2_config_file meta section to $meta_section failed"
                exit 1
            else
                base_port=$(grep "^ *base *:" $meta_section | cut -d":" -f2)
                hosts_list=$(grep "^ *- *[0-9]*" $meta_section | grep -oh "[0-9]*\.[0-9]*\.[0-9]*\.[0-9]*")
                config_file=$minos2_config_file
            fi
        elif [ ! -z "$MINOS_CONFIG_FILE" -a -f "$minos_config_file" ]; then
            meta_section_start=$(grep -n "\[meta" $minos_config_file | head -1 | cut -d":" -f 1)
            meta_section_end=$(grep -n "\[replica" $minos_config_file | head -1 | cut -d":" -f 1)
            sed -n "${meta_section_start},${meta_section_end}p" $minos_config_file > $meta_section
            if [ $? -ne 0 ]; then
                echo "ERROR: write $minos_config_file meta section to $meta_section failed"
                exit 1
            else
                base_port=$(grep "^ *base_port *=" $meta_section | cut -d"=" -f2)
                hosts_list=$(grep "^ *host\.[0-9]*" $meta_section | grep -oh "[0-9]*\.[0-9]*\.[0-9]*\.[0-9]*")
                config_file=$minos_config_file
            fi
        else
            echo "ERROR: can't find minos config file for $CLUSTER_NAME, please check env \$MINOS2_CONFIG_FILE or \$MINOS_CONFIG_FILE"
            exit 1
        fi

        if [ ! -z "$base_port" ] && [ ! -z "$hosts_list" ]; then
            meta_list=()
            for h in $hosts_list; do
                meta_list+=($h":"$[ $base_port + 1 ])
            done
            OLD_IFS="$IFS"
            IFS="," && CLUSTER="${meta_list[*]}" && IFS="$OLD_IFS"
            echo "INFO: parse meta_list from $config_file"
        else
            echo "ERROR: parse meta_list from $config_file failed"
            exit 1
        fi
    fi

    if [ ${CONFIG_SPECIFIED} -eq 0 ]; then
        sed "s/@CLUSTER_NAME@/$CLUSTER_NAME/g;s/@CLUSTER_ADDRESS@/$CLUSTER/g" ${ROOT}/src/shell/config.ini >${CONFIG}
    fi

    cd ${ROOT}
    ln -s -f ${DSN_ROOT}/bin/pegasus_shell/pegasus_shell
    ./pegasus_shell ${CONFIG} $CLUSTER_NAME
    # because pegasus shell will catch 'Ctrl-C' signal, so the following commands will be executed
    # even user inputs 'Ctrl-C', so that the temporary config file will be cleared when exit shell.
    rm -f ${CONFIG}
}

#####################
## migrate_node
#####################
function usage_migrate_node()
{
    echo "Options for subcommand 'migrate_node':"
    echo "   -h|--help            print the help info"
    echo "   -c|--cluster <str>   cluster meta lists"
    echo "   -n|--node <str>      the node to migrate primary replicas out, should be ip:port"
    echo "   -a|--app <str>       the app to migrate primary replicas out, if not set, means migrate all apps"
    echo "   -t|--type <str>      type: test or run, default is test"
}

function run_migrate_node()
{
    CLUSTER=""
    NODE=""
    APP="*"
    TYPE="test"
    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_migrate_node
                exit 0
                ;;
            -c|--cluster)
                CLUSTER="$2"
                shift
                ;;
            -n|--node)
                NODE="$2"
                shift
                ;;
            -a|--app)
                APP="$2"
                shift
                ;;
            -t|--type)
                TYPE="$2"
                shift
                ;;
            *)
                echo "ERROR: unknown option \"$key\""
                echo
                usage_migrate_node
                exit 1
                ;;
        esac
        shift
    done

    if [ "$CLUSTER" == "" ]; then
        echo "ERROR: no cluster specified"
        echo
        usage_migrate_node
        exit 1
    fi

    if [ "$NODE" == "" ]; then
        echo "ERROR: no node specified"
        echo
        usage_migrate_node
        exit 1
    fi

    if [ "$TYPE" != "test" -a "$TYPE" != "run" ]; then
        echo "ERROR: invalid type $TYPE"
        echo
        usage_migrate_node
        exit 1
    fi

    echo "CLUSTER=$CLUSTER"
    echo "NODE=$NODE"
    echo "APP=$APP"
    echo "TYPE=$TYPE"
    echo
    cd ${ROOT}
    echo "------------------------------"
    ./scripts/migrate_node.sh $CLUSTER $NODE "$APP" $TYPE
    echo "------------------------------"
    echo
    if [ "$TYPE" == "test" ]; then
        echo "The above is sample migration commands."
        echo "Run with option '-t run' to do migration actually."
    else
        echo "Done."
        echo "You can run shell command 'nodes -d' to check the result."
        echo
        echo "The cluster's auto migration is disabled now, you can run shell command 'set_meta_level lively' to enable it again."
    fi
}

#####################
## downgrade_node
#####################
function usage_downgrade_node()
{
    echo "Options for subcommand 'downgrade_node':"
    echo "   -h|--help            print the help info"
    echo "   -c|--cluster <str>   cluster meta lists"
    echo "   -n|--node <str>      the node to downgrade replicas, should be ip:port"
    echo "   -a|--app <str>       the app to downgrade replicas, if not set, means downgrade all apps"
    echo "   -t|--type <str>      type: test or run, default is test"
}

function run_downgrade_node()
{
    CLUSTER=""
    NODE=""
    APP="*"
    TYPE="test"
    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_downgrade_node
                exit 0
                ;;
            -c|--cluster)
                CLUSTER="$2"
                shift
                ;;
            -n|--node)
                NODE="$2"
                shift
                ;;
            -a|--app)
                APP="$2"
                shift
                ;;
            -t|--type)
                TYPE="$2"
                shift
                ;;
            *)
                echo "ERROR: unknown option \"$key\""
                echo
                usage_downgrade_node
                exit 1
                ;;
        esac
        shift
    done

    if [ "$CLUSTER" == "" ]; then
        echo "ERROR: no cluster specified"
        echo
        usage_downgrade_node
        exit 1
    fi

    if [ "$NODE" == "" ]; then
        echo "ERROR: no node specified"
        echo
        usage_downgrade_node
        exit 1
    fi

    if [ "$TYPE" != "test" -a "$TYPE" != "run" ]; then
        echo "ERROR: invalid type $TYPE"
        echo
        usage_downgrade_node
        exit 1
    fi

    echo "CLUSTER=$CLUSTER"
    echo "NODE=$NODE"
    echo "APP=$APP"
    echo "TYPE=$TYPE"
    echo
    cd ${ROOT}
    echo "------------------------------"
    ./scripts/downgrade_node.sh $CLUSTER $NODE "$APP" $TYPE
    echo "------------------------------"
    echo
    if [ "$TYPE" == "test" ]; then
        echo "The above is sample downgrade commands."
        echo "Run with option '-t run' to do migration actually."
    else
        echo "Done."
        echo "You can run shell command 'nodes -d' to check the result."
        echo
        echo "The cluster's auto migration is disabled now, you can run shell command 'set_meta_level lively' to enable it again."
    fi
}

####################################################################

if [ $# -eq 0 ]; then
    usage
    exit 0
fi
cmd=$1
case $cmd in
    help)
        usage
        ;;
    build)
        shift
        run_build $*
        ;;
    start_zk)
        shift
        run_start_zk $*
        ;;
    stop_zk)
        shift
        run_stop_zk $*
        ;;
    clear_zk)
        shift
        run_clear_zk $*
        ;;
    start_onebox)
        shift
        run_start_onebox $*
        ;;
    stop_onebox)
        shift
        run_stop_onebox $*
        ;;
    clear_onebox)
        shift
        run_clear_onebox $*
        ;;
    list_onebox)
        shift
        run_list_onebox $*
        ;;
    start_onebox_instance)
        shift
        run_start_onebox_instance $*
        ;;
    stop_onebox_instance)
        shift
        run_stop_onebox_instance $*
        ;;
    restart_onebox_instance)
        shift
        run_restart_onebox_instance $*
        ;;
    start_kill_test)
        shift
        run_start_kill_test $*
        ;;
    stop_kill_test)
        shift
        run_stop_kill_test $*
        ;;
    list_kill_test)
        shift
        run_list_kill_test $*
        ;;
    clear_kill_test)
        shift
        run_clear_kill_test $*
        ;;
    start_upgrade_test)
        shift
        run_start_upgrade_test $*
        ;;
    stop_upgrade_test)
        shift
        run_stop_upgrade_test $*
        ;;
    list_upgrade_test)
        shift
        run_list_upgrade_test $*
        ;;
    clear_upgrade_test)
        shift
        run_clear_upgrade_test $*
        ;;
    bench)
        shift
        run_bench $*
        ;;
    shell)
        shift
        run_shell $*
        ;;
    migrate_node)
        shift
        run_migrate_node $*
        ;;
    downgrade_node)
        shift
        run_downgrade_node $*
        ;;
    test)
        shift
        run_test $*
        ;;
    pack_server)
        shift
        PEGASUS_ROOT=$ROOT ./scripts/pack_server.sh $*
        ;;
    pack_client)
        shift
        PEGASUS_ROOT=$ROOT ./scripts/pack_client.sh $*
        ;;
    pack_tools)
        shift
        PEGASUS_ROOT=$ROOT ./scripts/pack_tools.sh $*
        ;;
    bump_version)
        shift
        ./scripts/bump_version.sh $*
        ;;
    *)
        echo "ERROR: unknown command $cmd"
        echo
        usage
        exit 1
esac
