<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
$dsn_root = dirname(dirname(dirname(__DIR__)));
$dsn_root = str_replace('\\', '/', $dsn_root);
?>
cmake_minimum_required(VERSION 2.8.8)

set(DSN_ROOT "<?=$dsn_root?>")
if(NOT EXISTS "${DSN_ROOT}/")
    message(FATAL_ERROR "Please make sure that ${DSN_ROOT} exists.")
endif()

include("${DSN_ROOT}/bin/dsn.cmake")

set(DSN_APP_TARGET "<?=$_PROG->name?>")
project(${DSN_APP_TARGET} C CXX)
set(DSN_BUILD_RUNTIME 0)
set(DSN_EXTRA_BOOST_PACKAGES "")
set(DSN_EXTRA_INCLUDEDIR "")
set(DSN_EXTRA_LIBRARYDIR "")
set(DSN_EXTRA_LIBS "")
set(DSN_EXTRA_SRC "")
dsn_common_setup()
file(GLOB BINPLACE_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.ini")
dsn_add_executable(${DSN_APP_TARGET} "${BINPLACE_FILES}")
