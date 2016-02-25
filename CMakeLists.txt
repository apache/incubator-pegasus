cmake_minimum_required(VERSION 2.8.7)

set(DSN_SERIALIZATION_TYPE "default")

if(DSN_SERIALIZATION_TYPE STREQUAL "default")
    message(STATUS "use default serialization method")
elseif(DSN_SERIALIZATION_TYPE STREQUAL "thrift")
    message(STATUS "use thrift serialization method")
    add_definitions(-DDSN_NOT_USE_DEFAULT_SERIALIZATION)
else()
    message(FATAL_ERROR "not support other serialization method")
endif()

include(bin/dsn.cmake)
#cmake_policy(SET CMP0048 NEW)
#project(dsn VERSION 1.0 LANGUAGES C CXX)
project(dsn C CXX)
set(DSN_BUILD_RUNTIME TRUE)
if(DEFINED WARNING_ALL)
    #add_compile_options("-Wall")
    SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall")
    SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -Wall")
endif()
OPTION(ENABLE_GCOV "Enable gcov (debug, Linux builds only)" OFF)
if(ENABLE_GCOV AND UNIX)
    #add_compile_options("-fprofile-arcs" "-ftest-coverage")
    SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fprofile-arcs -ftest-coverage -DENABLE_GCOV")
    SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -fprofile-arcs -ftest-coverage -DENABLE_GCOV")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fprofile-arcs -ftest-coverage -lgcov")
endif()
dsn_common_setup()
dsn_add_pseudo_projects()

if(DSN_SERIALIZATION_TYPE STREQUAL "thrift")
    add_subdirectory(ext/thrift)
endif()

add_subdirectory(ext/gtest)
add_subdirectory(ext/zookeeper)
add_subdirectory(ext/rapidjson)
add_subdirectory(ext/concurrentqueue)
#
# !!! gflags is currently not used, and when it is enabled, it causes failure on windows compilation
# add_subdirectory(ext/gflags)
#

add_subdirectory(src)
