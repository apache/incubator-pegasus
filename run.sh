#!/bin/bash
# The MIT License (MIT)
#
# Copyright (c) 2015 Microsoft Corporation
#
# -=- Robust Distributed System Nucleus (rDSN) -=-
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.


os=linux
scripts_dir=`pwd`/scripts/$os

function exit_if_fail() {
    if [ $1 != 0 ]; then
        exit $1
    fi
}

function usage()
{
    echo "usage: run.sh <command> [<args>]"
    echo
    echo "Command list:"
    echo "   help        print the help info"
    echo "   build       build the system"
    echo "   install     install the system"
    echo "   test        test the system"
    echo "   start_zk    start the local single zookeeper server"
    echo "   stop_zk     stop the local single zookeeper server"
    echo "   clear_zk    stop the local single zookeeper server and clear the data"
    echo "   deploy      deploy the program to remote machine"
    echo "   start       start program at remote machine"
    echo "   stop        stop program at remote machine"
    echo "   clean       clean deployed program at remote machine"
    echo
    echo "Command 'run.sh <command> -h' will print help for subcommands."
}

#####################
## build
#####################
function usage_build()
{
    subcommand="build"
    if [ "$ONLY_BUILD" == "NO" ]; then
        subcommand="test"
    fi
    echo "Options for subcommand '$subcommand':"
    echo "   -h|--help             print the help info"
    echo "   -t|--type             build type: debug|release, default is debug"
    echo "   -c|--clear            clear environment before building, but not clear thirdparty"
    echo "   --clear_thirdparty    clear environment before building, including thirdparty"
    echo "   --compiler            specify c and cxx compiler, sperated by ','"
    echo "                         e.g., \"gcc,g++\" or \"clang-3.9,clang++-3.9\""
    echo "                         default is \"gcc,g++\""
    echo "   -j|--jobs <num>       the number of jobs to run simultaneously, default 8"
    echo "   --enable_gcov         generate gcov code coverage report, default no"
    echo "   -v|--verbose          build in verbose mode, default no"
    echo "   --notest              build without building unit tests, default no"
    echo "   --disable_gperf       build without gperftools, this flag is mainly used"
    echo "                         to enable valgrind memcheck, default no"
    echo "   --use_jemalloc        build with jemalloc"
    echo "   --skip_thirdparty     whether to skip building thirdparties, default no"
    echo "   --check               whether to perform code check before building"
    echo "   --sanitizer <type>    build with sanitizer to check potential problems,
                                   type: address|leak|thread|undefined"
    if [ "$ONLY_BUILD" == "NO" ]; then
        echo "   -m|--test_module      specify modules to test, split by ',',"
        echo "                         e.g., \"dsn_runtime_tests,dsn_meta_state_tests\","
        echo "                         if not set, then run all tests"
    fi

    echo "   --enable_rocksdb_portable      build a portable rocksdb binary"
}
function run_build()
{
    # NOTE(jiashuo1): No "memory" check mode, because MemorySanitizer is only available in Clang for Linux x86_64 targets
    # https://www.jetbrains.com/help/clion/google-sanitizers.html
    SANITIZERS=("address" "leak" "thread" "undefined")

    C_COMPILER="gcc"
    CXX_COMPILER="g++"
    BUILD_TYPE="release"
    CLEAR=NO
    CLEAR_THIRDPARTY=NO
    JOB_NUM=8
    ENABLE_GCOV=NO
    RUN_VERBOSE=NO
    NO_TEST=NO
    DISABLE_GPERF=NO
    USE_JEMALLOC=NO
    SKIP_THIRDPARTY=NO
    CHECK=NO
    SANITIZER=""
    TEST_MODULE=""
    ROCKSDB_PORTABLE=OFF
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
                BUILD_TYPE="debug"
                ;;
            -v|--verbose)
                RUN_VERBOSE=YES
                ;;
            --notest)
                NO_TEST=YES
                ;;
            --disable_gperf)
                DISABLE_GPERF=YES
                ;;
            --use_jemalloc)
                DISABLE_GPERF=YES
                USE_JEMALLOC=YES
                ;;
            --skip_thirdparty)
                SKIP_THIRDPARTY=YES
                ;;
            --check)
                CHECK=YES
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
            -m|--test_module)
                if [ "$ONLY_BUILD" == "YES" ]; then
                    echo "ERROR: unknown option \"$key\""
                    echo
                    usage_build
                    exit 1
                fi
                TEST_MODULE="$2"
                shift
                ;;
            --enable_rocksdb_portable)
                ROCKSDB_PORTABLE=ON
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

    if [[ ${CHECK} == "YES" ]]; then
        ${scripts_dir}/run-clang-format.sh
        exit_if_fail $?
    fi

    if [ "$(uname)" == "Darwin" ]; then
        MACOS_OPENSSL_ROOT_DIR="/usr/local/opt/openssl"
        CMAKE_OPTIONS="-DMACOS_OPENSSL_ROOT_DIR=${MACOS_OPENSSL_ROOT_DIR}"
    fi
    if [[ ${SKIP_THIRDPARTY} == "YES" ]]; then
        echo "Skip building third-parties..."
    else
        cd thirdparty
        if [[ "$CLEAR_THIRDPARTY" == "YES" ]]; then
            echo "Clear third-parties..."
            rm -rf build
            rm -rf output
        fi
        echo "Start building third-parties..."
        mkdir -p build
        pushd build
        CMAKE_OPTIONS="${CMAKE_OPTIONS}
                       -DCMAKE_C_COMPILER=${C_COMPILER}
                       -DCMAKE_CXX_COMPILER=${CXX_COMPILER}
                       -DCMAKE_BUILD_TYPE=Release
                       -DROCKSDB_PORTABLE=${ROCKSDB_PORTABLE}
                       -DUSE_JEMALLOC=${USE_JEMALLOC}"
        cmake .. ${CMAKE_OPTIONS}
        make -j$JOB_NUM
        exit_if_fail $?
        popd
        cd ..
    fi

    if [ "$BUILD_TYPE" != "debug" -a "$BUILD_TYPE" != "release" ]; then
        echo "ERROR: invalid build type \"$BUILD_TYPE\""
        echo
        usage_build
        exit 1
    fi
    if [ "$ONLY_BUILD" == "NO" ]; then
        run_start_zk
        if [ $? -ne 0 ]; then
            echo "ERROR: start zk failed"
            exit 1
        fi
    fi
    C_COMPILER="$C_COMPILER" CXX_COMPILER="$CXX_COMPILER" BUILD_TYPE="$BUILD_TYPE" \
        ONLY_BUILD="$ONLY_BUILD" CLEAR="$CLEAR" JOB_NUM="$JOB_NUM" \
        ENABLE_GCOV="$ENABLE_GCOV" SANITIZER="$SANITIZER" \
        RUN_VERBOSE="$RUN_VERBOSE" TEST_MODULE="$TEST_MODULE" NO_TEST="$NO_TEST" \
        DISABLE_GPERF="$DISABLE_GPERF" USE_JEMALLOC="$USE_JEMALLOC" \
        MACOS_OPENSSL_ROOT_DIR="$MACOS_OPENSSL_ROOT_DIR" $scripts_dir/build.sh
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
    echo "   -p|--port <port>  listen port of zookeeper, default is 12181"
}

function run_start_zk()
{
    # first we check the environment that zk need: java and nc command
    # check java
    type java >/dev/null 2>&1 || { echo >&2 "start zk failed, need install jre..."; exit 1;}

    # check nc command
    type nc >/dev/null 2>&1 || { echo >&2 "start zk failed, need install netcat command..."; exit 1;}

    INSTALL_DIR=`pwd`/.zk_install
    PORT=12181
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
    INSTALL_DIR="$INSTALL_DIR" PORT="$PORT" ./scripts/linux/start_zk.sh
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
    INSTALL_DIR="$INSTALL_DIR" ./scripts/linux/stop_zk.sh
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
    INSTALL_DIR="$INSTALL_DIR" ./scripts/linux/clear_zk.sh
}

####################################################################

if [ $# -eq 0 ]; then
    usage
    exit 0
fi
cmd=$1
case $cmd in
    help)
        usage ;;
    build)
        shift
        ONLY_BUILD=YES
        run_build $* ;;
    test)
        shift
        ONLY_BUILD=NO
        run_build $* ;;
    start_zk)
        shift
        run_start_zk $* ;;
    stop_zk)
        shift
        run_stop_zk $* ;;
    clear_zk)
        shift
        run_clear_zk $* ;;
    deploy|start|stop|clean)
        $scripts_dir/deploy.sh $* ;;
    *)
        echo "ERROR: unknown command $cmd"
        echo
        usage
        exit 1
esac

