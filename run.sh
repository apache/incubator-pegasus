#!/bin/bash

os=linux
scripts_dir=`pwd`/scripts/$os

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
    echo "   format      check the code format"
    echo
    echo "Command 'run.sh <command> -h' will print help for subcommands."
}

#####################
## build
#####################
function usage_build()
{
    echo "Options for subcommand 'build':"
    echo "   -h|--help         print the help info"
    echo "   -d|--debug        build in debug mode, default is release"
    echo "   -c|--clear        clear the environment before building"
    echo "   -b|--boost_dir <dir>"
    echo "                     specify customized boost directory,"
    echo "                     if not set, then use the system boost"
    echo "   -w|--warning_all  open all warnings when build, default no"
    echo "   -v|--verbose      print verbose info, default no"
}
function run_build()
{
    DEBUG=NO
    CLEAR=NO
    BOOST_DIR=""
    WARNING_ALL=NO
    VERBOSE=NO
    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_build
                exit 0
                ;;
            -d|--debug)
                DEBUG=YES
                ;;
            -c|--clear)
                CLEAR=YES
                ;;
            -b|--boost_dir)
                BOOST_DIR="$2"
                shift
                ;;
            -w|--warning_all)
                WARNING_ALL=YES
                ;;
            -v|--verbose)
                VERBOSE=YES
                ;;
            *)
                echo "ERROR: unknown option $key"
                echo
                usage_build
                exit -1
                ;;
        esac
        shift
    done
    DEBUG="$DEBUG" CLEAR="$CLEAR" BOOST_DIR="$BOOST_DIR" WARNING_ALL="$WARNING_ALL" VERBOSE="$VERBOSE" $scripts_dir/build.sh
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
    INSTALL_DIR=`pwd`/install
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
                echo "ERROR: unknown option $key"
                echo
                usage_install
                exit -1
                ;;
        esac
        shift
    done
    INSTALL_DIR="$INSTALL_DIR" $scripts_dir/install.sh
}

#####################
## test
#####################
function usage_test()
{
    echo "Options for subcommand 'test':"
    echo "   -h|--help         print the help info"
    echo "   -c|--clear        clear the environment before building and testing"
    echo "   -b|--boost_dir <dir>"
    echo "                     specify the customized boost directory,"
    echo "                     if not set, then use the system boost"
    echo "   -w|--warning_all  open all warnings when build, default no"
    echo "   -g|--enable_gcov  generate gcov code coverage report, default no"
    echo "   -m|--test_module  specify the module to test, e.g., dsn.core.tests,"
    echo "                     if not set, then run all tests"
    echo "   -v|--verbose      print verbose info, default no"
}
function run_test()
{
    CLEAR=NO
    BOOST_DIR=""
    WARNING_ALL=NO
    ENABLE_GCOV=NO
    TEST_MODULE=""
    VERBOSE=NO
    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_test
                exit 0
                ;;
            -c|--clear)
                CLEAR=YES
                ;;
            -b|--boost_dir)
                BOOST_DIR="$2"
                shift
                ;;
            -w|--warning_all)
                WARNING_ALL=YES
                ;;
            -g|--enable_gcov)
                ENABLE_GCOV=YES
                ;;
            -m|--test_module)
                TEST_MODULE="$2"
                shift
                ;;
            -v|--verbose)
                VERBOSE=YES
                ;;
            *)
                echo "ERROR: unknown option $key"
                echo
                usage_test
                exit -1
                ;;
        esac
        shift
    done
    CLEAR="$CLEAR" BOOST_DIR="$BOOST_DIR" WARNING_ALL="$WARNING_ALL" ENABLE_GCOV="$ENABLE_GCOV" TEST_MODULE="$TEST_MODULE" VERBOSE="$VERBOSE" $scripts_dir/test.sh
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
                echo "ERROR: unknown option $key"
                echo
                usage_start_zk
                exit -1
                ;;
        esac
        shift
    done
    INSTALL_DIR="$INSTALL_DIR" PORT="$PORT" $scripts_dir/start_zk.sh
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
                echo "ERROR: unknown option $key"
                echo
                usage_stop_zk
                exit -1
                ;;
        esac
        shift
    done
    INSTALL_DIR="$INSTALL_DIR" $scripts_dir/stop_zk.sh
}

#####################
## format
#####################
function usage_format()
{
    echo "Options for subcommand 'format':"
    echo "   -h|--help         print the help info"
}
function run_format()
{
    while [[ $# > 0 ]]; do
        key="$1"
        case $key in
            -h|--help)
                usage_format
                exit 0
                ;;
            *)
                echo "ERROR: unknown option $key"
                echo
                usage_format
                exit -1
                ;;
        esac
        shift
    done
    $scripts_dir/format.sh
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
        run_build $* ;;
    install)
        shift
        run_install $* ;;
    test)
        shift
        run_test $* ;;
    start_zk)
        shift
        run_start_zk $* ;;
    stop_zk)
        shift
        run_stop_zk $* ;;
    format)
        shift
        run_format $* ;;
    *)
        echo "ERROR: unknown command $cmd"
        echo
        usage
        exit -1
esac

