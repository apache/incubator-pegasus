cmake_minimum_required(VERSION 2.8.8)

project(dsn C CXX)

include(bin/dsn.cmake)

set(DSN_BUILD_RUNTIME TRUE)
add_definitions(-DGTEST_HAS_TR1_TUPLE=0 -DGTEST_USE_OWN_TR1_TUPLE=0)
dsn_common_setup()

# add custom target for gcov
if(ENABLE_GCOV)
    find_program(GCOV_PATH gcov)
    find_program(GCOVR_PATH gcovr)

    if(NOT GCOV_PATH)
        message(FATAL_ERROR "gcov not found! Aborting...")
    endif() # NOT GCOV_PATH

    if(NOT GCOVR_PATH)
        message(FATAL_ERROR "gcovr not found! Aborting...")
    endif() # NOT GCOVR_PATH

    if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
        message(WARNING "Code coverage results with an optimised (non-Debug) build may be misleading")
    endif() # NOT CMAKE_BUILD_TYPE STREQUAL "Debug"

    set(COVERAGE_COMPILER_FLAGS "-g -O0 --coverage -fprofile-arcs -ftest-coverage" CACHE INTERNAL "")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${COVERAGE_COMPILER_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${COVERAGE_COMPILER_FLAGS}")
    message(STATUS "Appending code coverage compiler flags: ${COVERAGE_COMPILER_FLAGS}")

    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lgcov")
endif()

# Users don't have to configure CMAKE_INSTALL_PREFIX unless they want to customize
# the destination.
set(CMAKE_INSTALL_PREFIX ${DSN_ROOT} CACHE STRING "" FORCE)
message (STATUS "dsn Installation directory: CMAKE_INSTALL_PREFIX = " ${CMAKE_INSTALL_PREFIX})

# install rdsn/include to ${CMAKE_INCLUDE_PREFIX}/include
install(DIRECTORY include/ DESTINATION include)

include_directories(${DSN_PROJECT_DIR}/include)
include_directories(${DSN_PROJECT_DIR}/include/dsn/cpp/serialization_helper)
include_directories(${DSN_PROJECT_DIR}/src)

add_subdirectory(src)
