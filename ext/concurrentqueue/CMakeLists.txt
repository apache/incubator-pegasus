#cmake_minimum_required(VERSION 2.8.8)
#project(gtest_builder C CXX)
include(ExternalProject)

set(target_url ${CONQUEUE_GIT_SOURCE})
set(git_tag ${CONQUEUE_GIT_TAG})

ExternalProject_Add(concurrentqueue
    GIT_REPOSITORY ${target_url}
    GIT_TAG ${git_tag}
# Disable install step
    INSTALL_COMMAND ""
)

# Specify include dir
ExternalProject_Get_Property(concurrentqueue source_dir)
set(CONQUEUE_INCLUDE_DIRS ${source_dir} PARENT_SCOPE)
