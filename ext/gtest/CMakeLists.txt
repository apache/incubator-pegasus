#cmake_minimum_required(VERSION 2.8.8)
#project(gtest_builder C CXX)
include(ExternalProject)

ExternalProject_Add(googletest
    GIT_REPOSITORY https://github.com/imzhenyu/googletest.git
    GIT_TAG master
   #   CMAKE_ARGS -DCMAKE_ARCHIVE_OUTPUT_DIRECTORY_DEBUG:PATH=DebugLibs
   #            -DCMAKE_ARCHIVE_OUTPUT_DIRECTORY_RELEASE:PATH=ReleaseLibs
   #            -DCMAKE_CXX_FLAGS=${MSVC_COMPILER_DEFS}
    CMAKE_ARGS "${CMAKE_ARGS};-Dgtest_force_shared_crt=OFF;-DCMAKE_CXX_FLAGS=-fPIC ${CMAKE_CXX_FLAGS}"
    BINARY_DIR "${PROJECT_BINARY_DIR}/lib"
# Disable install step
    INSTALL_COMMAND ""
)

# Specify include dir
ExternalProject_Get_Property(googletest source_dir)
set(GTEST_INCLUDE_DIRS ${source_dir}/include PARENT_SCOPE)

# Specify MainTest's link libraries
ExternalProject_Get_Property(googletest binary_dir)
set(GTEST_LIBS_DIR ${binary_dir} PARENT_SCOPE)



