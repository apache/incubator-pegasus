include(ExternalProject)

if (NOT WIN32)
    ExternalProject_Add(zookeeper
        GIT_REPOSITORY https://github.com/shengofsun/zookeeper_client_c.git
        GIT_TAG master
        BINARY_DIR "${PROJECT_BINARY_DIR}/zookeeper"
        INSTALL_DIR "${PROJECT_BINARY_DIR}/lib"
        INSTALL_COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_BINARY_DIR}/zookeeper/libzookeeper_mt.a ${PROJECT_BINARY_DIR}/lib
    )
endif()

# Specify include dir
ExternalProject_Get_Property(zookeeper source_dir)
set(ZOOKEEPER_INCLUDE_DIR ${source_dir}/include PARENT_SCOPE)

# Specify MainTest's link libraries
ExternalProject_Get_Property(zookeeper binary_dir)
set(ZOOKEEPER_LIB_DIR ${binary_dir} PARENT_SCOPE)

install(FILES "${PROJECT_BINARY_DIR}/zookeeper/libzookeeper_mt.a" DESTINATION lib)
