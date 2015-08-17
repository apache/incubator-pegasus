cmake_minimum_required(VERSION 2.8.8)
include(bin/dsn.cmake)
project(dsn C CXX)
set(DSN_BUILD_RUNTIME 1)
dsn_common_setup()
dsn_add_pseudo_projects()

add_subdirectory(ext/gtest)
add_subdirectory(src)