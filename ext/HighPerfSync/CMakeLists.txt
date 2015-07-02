#cmake_minimum_required(VERSION 2.8.8)
#project(gtest_builder C CXX)
include(ExternalProject)

ExternalProject_Add(HighPerfSync
    GIT_REPOSITORY https://github.com/imzhenyu/cpp11-on-multicore
    GIT_TAG master
# Disable install step
    INSTALL_COMMAND ""
)

# Specify include dir
ExternalProject_Get_Property(HighPerfSync source_dir)
set(HPS_INCLUDE_DIRS ${source_dir}/common PARENT_SCOPE)
