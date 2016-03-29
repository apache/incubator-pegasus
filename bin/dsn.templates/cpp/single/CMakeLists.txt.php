<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
$idl_format = $argv[4];

global $inc_path;
global $inc_lib;
$inc_lib = "";
$inc_path = '${DSN_ROOT}/include/ext';
if ($idl_format == "proto")
{
    if (strncasecmp(PHP_OS, 'WIN', 3) == 0)
    {
        $inc_lib = "libprotobuf.lib dsn.core.dll";
    } else
    {
        $inc_lib = "libprotobuf.a libdsn.core.so";
    }
} else if ($idl_format == "thrift")
{
    if (strncasecmp(PHP_OS, 'WIN', 3) == 0)
    {
         $inc_lib = "thriftmt.lib dsn.core.dll";
    } else
    {
        $inc_lib = "libthrift.so libdsn.core.so";
    }
}
?>
cmake_minimum_required(VERSION 2.8.8)

set(DSN_ROOT "$ENV{DSN_ROOT}")
if((DSN_ROOT STREQUAL "") OR (NOT EXISTS "${DSN_ROOT}/"))
    message(FATAL_ERROR "Please make sure that DSN_ROOT is defined and does exists.")
endif()

include("${DSN_ROOT}/bin/dsn.cmake")

set(MY_PROJ_NAME "<?=$_PROG->name?>")
project(${MY_PROJ_NAME} C CXX)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
file(GLOB
    MY_PROJ_SRC
<?php if ($idl_format == "thrift") { ?>
   "${CMAKE_CURRENT_SOURCE_DIR}/thrift/*.cpp"
   "${CMAKE_CURRENT_SOURCE_DIR}/thrift/*.h"
<?php } else { ?>
    ""
<?php } ?>
)

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH "<?=$inc_path?>")

set(MY_PROJ_LIBS <?=$inc_lib?>)

set(MY_PROJ_LIB_PATH "")

set(INI_FILES "")

file(GLOB
    RES_FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/*.ini"
    "${CMAKE_CURRENT_SOURCE_DIR}/*.sh"
    "${CMAKE_CURRENT_SOURCE_DIR}/*.cmd"
    "${CMAKE_CURRENT_SOURCE_DIR}/Dockerfile"
    )

# Extra files that will be installed
set(MY_BINPLACES ${RES_FILES})

set(MY_BOOST_PACKAGES "")

dsn_common_setup()
dsn_add_executable()

add_custom_target( docker 
    COMMAND docker build -t "${MY_PROJ_NAME}-image" "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${MY_PROJ_NAME}")
add_dependencies( docker "${MY_PROJ_NAME}")
