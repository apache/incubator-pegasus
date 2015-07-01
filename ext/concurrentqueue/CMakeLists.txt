#cmake_minimum_required(VERSION 2.8.8)
#project(gtest_builder C CXX)
include(ExternalProject)

ExternalProject_Add(concurrentqueue
    GIT_REPOSITORY https://github.com/imzhenyu/concurrentqueue/
    GIT_TAG master
# Disable install step
    INSTALL_COMMAND ""
)

# Specify include dir
ExternalProject_Get_Property(concurrentqueue source_dir)
set(CONQUEUE_INCLUDE_DIRS ${source_dir} PARENT_SCOPE)
