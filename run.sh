#!/bin/bash

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
    echo "   -b|--boost_dir <dir>  specify customized boost directory, use system boost if not set"
    echo "   --enable_gcov         generate gcov code coverage report, default no"
    echo "   -v|--verbose          build in verbose mode, default no"
    echo "   --notest              build without building unit tests, default no"
    echo "   --disable_gperf       build without gperftools, this flag is mainly used"
    echo "                         to enable valgrind memcheck, default no"
    echo "   --skip_thirdparty     whether to skip building thirdparties, default no"
    echo "   --check               whether to perform code check before building"
    if [ "$ONLY_BUILD" == "NO" ]; then
        echo "   -m|--test_module      specify modules to test, split by ',',"
        echo "                         e.g., \"dsn.core.tests,dsn.tests\","
        echo "                         if not set, then run all tests"
    fi
}
function run_build()
{
    C_COMPILER="gcc"
    CXX_COMPILER="g++"
    BUILD_TYPE="release"
    CLEAR=NO
    CLEAR_THIRDPARTY=NO
    JOB_NUM=8
    BOOST_DIR=""
    ENABLE_GCOV=NO
    RUN_VERBOSE=NO
    NO_TEST=NO
    DISABLE_GPERF=NO
    SKIP_THIRDPARTY=NO
    CHECK=NO
    TEST_MODULE=""
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
            -b|--boost_dir)
                BOOST_DIR="$2"
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
            --skip_thirdparty)
                SKIP_THIRDPARTY=YES
                ;;
            --check)
                CHECK=YES
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

    if [[ ${SKIP_THIRDPARTY} == "YES" ]]; then
        echo "Skip building thirdparty..."
    else
        # build thirdparty first
        cd thirdparty
        if [[ "$CLEAR_THIRDPARTY" == "YES" ]]; then
            echo "Clear thirdparty..."
            rm -rf src build output &>/dev/null
            CLEAR=YES
        fi
        echo "Start building thirdparty..."
        ./download-thirdparty.sh
        exit_if_fail $?
        if [[ "x"$BOOST_DIR != "x" ]]; then
            ./build-thirdparty.sh -b $BOOST_DIR
            exit_if_fail $?
        else
            ./build-thirdparty.sh
            exit_if_fail $?
        fi
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
        BOOST_DIR="$BOOST_DIR" ENABLE_GCOV="$ENABLE_GCOV" \
        RUN_VERBOSE="$RUN_VERBOSE" TEST_MODULE="$TEST_MODULE" NO_TEST="$NO_TEST" \
        DISABLE_GPERF="$DISABLE_GPERF" $scripts_dir/build.sh
}

#####################
## install
#####################
function usage_install()
{
    echo "Options for subcommand 'install':"
    echo "   -h|--help         print the help info"
    echo "   -d|--install_dir <dir>"
    echo "                     specify the install directory,"
    echo "                     if not set, then default is './install'"
}
function run_install()
{
    INSTALL_DIR=$DSN_ROOT
    if [ ! -d $INSTALL_DIR ]; then
        INSTALL_DIR=`pwd`/install
    fi
    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_install
                exit 0
                ;;
            -d|--install_dir)
                INSTALL_DIR="$2"
                shift
                ;;
            *)
                echo "ERROR: unknown option \"$key\""
                echo
                usage_install
                exit 1
                ;;
        esac
        shift
    done
    INSTALL_DIR="$INSTALL_DIR" $scripts_dir/install.sh
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
    install)
        shift
        run_install $* ;;
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

