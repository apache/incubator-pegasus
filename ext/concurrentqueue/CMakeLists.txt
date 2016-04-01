#cmake_minimum_required(VERSION 2.8.8)
#project(gtest_builder C CXX)
include(ExternalProject)

if(DSN_GIT_SOURCE STREQUAL "github")
    set(target_url "https://github.com/imzhenyu/concurrentqueue.git")
elseif(DSN_GIT_SOURCE STREQUAL "xiaomi")
    set(target_url "http://git.n.xiaomi.com/pegasus/concurrentqueue.git")
else()
    message(FATAL_ERROR "not support other git source")
endif()

ExternalProject_Add(concurrentqueue
    GIT_REPOSITORY ${target_url}
    GIT_TAG master
# Disable install step
    INSTALL_COMMAND ""
)

# Specify include dir
ExternalProject_Get_Property(concurrentqueue source_dir)
set(CONQUEUE_INCLUDE_DIRS ${source_dir} PARENT_SCOPE)
