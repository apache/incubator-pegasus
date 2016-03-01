include(ExternalProject)

set(target_dir ${PROJECT_BINARY_DIR}/thrift/lib)

if(WIN32)
    set(install_cmd make install)
else()
    set(install_cmd COMMAND make install COMMAND ${CMAKE_COMMAND} -E copy "${target_dir}/libthrift.so.0.9.3" "${PROJECT_BINARY_DIR}/lib/" COMMAND ${CMAKE_COMMAND} -E copy "${target_dir}/libthrift.so" "${PROJECT_BINARY_DIR}/lib/")
endif()

ExternalProject_Add(libthrift
    GIT_REPOSITORY git@github.com:apache/thrift.git
    GIT_TAG 0.9.3
    CMAKE_ARGS "-DWITH_JAVA=OFF;-DWITH_PYTHON=OFF;-DBUILD_TESTING=OFF;-DBUILD_EXAMPLES=OFF;-DWITH_STATIC_LIB=OFF;-DWITH_QT5=OFF;-DWITH_QT4=OFF;-DWITH_OPENSSL=OFF;-DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}/thrift"
    INSTALL_DIR "${PROJECT_BINARY_DIR}/lib"
    INSTALL_COMMAND ${install_cmd}
)

# Specify source dir
set(THRIFT_INCLUDE_DIR ${PROJECT_BINARY_DIR}/thrift/include PARENT_SCOPE)
set(THRIFT_LIB_DIR ${PROJECT_BINARY_DIR}/lib)
