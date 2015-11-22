#cmake_minimum_required(VERSION 2.8.8)
#project(gtest_builder C CXX)
include(ExternalProject)

ExternalProject_Add(gflags
    GIT_REPOSITORY git@git.n.xiaomi.com:pegasus/gflags.git
    GIT_TAG master
   #   CMAKE_ARGS -DCMAKE_ARCHIVE_OUTPUT_DIRECTORY_DEBUG:PATH=DebugLibs
   #            -DCMAKE_ARCHIVE_OUTPUT_DIRECTORY_RELEASE:PATH=ReleaseLibs
   #            -DCMAKE_CXX_FLAGS=${MSVC_COMPILER_DEFS}
    BINARY_DIR "${PROJECT_BINARY_DIR}/gflags"

    INSTALL_DIR "${PROJECT_BINARY_DIR}/lib"
    INSTALL_COMMAND cp ${PROJECT_BINARY_DIR}/gflags/lib/libgflags.a ${PROJECT_BINARY_DIR}/gflags/lib/libgflags_nothreads.a ${PROJECT_BINARY_DIR}/lib
)

# Specify include dir
ExternalProject_Get_Property(gflags source_dir)
set(GFLAGS_INCLUDE_DIRS ${PROJECT_BINARY_DIR}/gflags/include PARENT_SCOPE)

