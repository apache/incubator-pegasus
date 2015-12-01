include(ExternalProject)

ExternalProject_Add(zookeeper
    GIT_REPOSITORY git@git.n.xiaomi.com:pegasus/zookeeper-3-4-6.git
    GIT_TAG master
    BINARY_DIR "${PROJECT_BINARY_DIR}/zookeeper"
    INSTALL_DIR "${PROJECT_BINARY_DIR}/lib"
    INSTALL_COMMAND cp ${PROJECT_BINARY_DIR}/zookeeper/libdsn.zookeeper_mt.a ${PROJECT_BINARY_DIR}/lib
)

# Specify include dir
ExternalProject_Get_Property(zookeeper source_dir)
set(ZOOKEEPER_INCLUDE_DIR ${source_dir}/include PARENT_SCOPE)

# Specify MainTest's link libraries
ExternalProject_Get_Property(zookeeper binary_dir)
set(ZOOKEEPER_LIB_DIR ${binary_dir} PARENT_SCOPE)

install(FILES "${PROJECT_BINARY_DIR}/zookeeper/libdsn.zookeeper_mt.a" DESTINATION lib)
