set(project_name zookeeper)

if(DSN_GIT_SOURCE STREQUAL "github")
    set(target_url "https://github.com/shengofsun/zookeeper_client_c.git")
elseif(DSN_GIT_SOURCE STREQUAL "xiaomi")
    set(target_url "http://git.n.xiaomi.com/pegasus/zookeeper_client_c.git")
else()
    message(FATAL_ERROR "not support other git source")
endif()

set(my_cmake_args "")

if(WIN32)
    set(target_binaries zookeeper_mt.dll zookeeper_mt.lib)
else()
    set(target_binaries libzookeeper_mt.a)
endif()

include(${PROJECT_SOURCE_DIR}/bin/dsn.ext.cmake)

set(ZOOKEEPER_INCLUDE_DIR ${my_source_dir}/include PARENT_SCOPE)
set(ZOOKEEPER_LIB_DIR ${my_binary_dir} PARENT_SCOPE)

