set(project_name googletest)

if(DSN_GIT_SOURCE STREQUAL "github")
    set(target_url "https://github.com/imzhenyu/googletest.git")
elseif(DSN_GIT_SOURCE STREQUAL "xiaomi")
    set(target_url "http://git.n.xiaomi.com/pegasus/googletest.git")
else()
    message(STATUS "the git source" ${DSN_GIT_SOURCE})
    message(FATAL_ERROR "not support other git source")
endif()

set(my_cmake_args "-Dgtest_force_shared_crt=OFF;")

if(WIN32)
    set(target_binaries gtest.lib gtest_main.lib)
else()
    set(target_binaries libgtest.a libgtest_main.a)
endif()

include(${PROJECT_SOURCE_DIR}/bin/dsn.ext.cmake)

set(GTEST_INCLUDE_DIR ${my_source_dir}/include PARENT_SCOPE)
set(GTEST_LIB_DIR ${my_binary_dir} PARENT_SCOPE)
