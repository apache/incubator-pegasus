set(project_name zookeeper)
set(target_url  https://github.com/shengofsun/zookeeper_client_c.git)
set(my_cmake_args "")

if(WIN32)
    set(target_binaries zookeeper_mt.dll zookeeper_mt.lib)
else()
    set(target_binaries libzookeeper_mt.a)
endif()

include(${PROJECT_SOURCE_DIR}/bin/dsn.ext.cmake)

set(ZOOKEEPER_INCLUDE_DIR ${my_source_dir}/include PARENT_SCOPE)
set(ZOOKEEPER_LIB_DIR ${my_binary_dir} PARENT_SCOPE)

