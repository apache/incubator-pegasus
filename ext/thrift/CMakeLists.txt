include(ExternalProject)

set(target_dir ${PROJECT_BINARY_DIR}/thrift/lib)

if(DSN_GIT_SOURCE STREQUAL "github")
    set(target_url "https://raw.githubusercontent.com/shengofsun/packages/master/thrift-0.9.3.tar.gz")
elseif(DSN_GIT_SOURCE STREQUAL "xiaomi")
    set(target_url "http://git.n.xiaomi.com/pegasus/packages/raw/master/thrift-0.9.3.tar.gz")
else()
    message(FATAL_ERROR "not support other git source")
endif()

if(WIN32)
    set(install_cmd make install)
else()
    set(cp_target_file ${CMAKE_COMMAND} -E copy "${target_dir}/libthrift.so" "${PROJECT_BINARY_DIR}/lib")
    set(cp_include_files ${CMAKE_COMMAND} -E copy_directory "${PROJECT_BINARY_DIR}/thrift/include" "${CMAKE_INSTALL_PREFIX}/include")
    set(install_cmd COMMAND make install COMMAND ${cp_target_file} COMMAND ${cp_include_files})
endif()

ExternalProject_Add(libthrift
    URL ${target_url}
    CMAKE_ARGS "-DWITH_JAVA=OFF;-DWITH_PYTHON=OFF;-DBUILD_TESTING=OFF;-DBUILD_EXAMPLES=OFF;-DWITH_STATIC_LIB=OFF;-DWITH_QT5=OFF;-DWITH_QT4=OFF;-DWITH_OPENSSL=OFF;-DWITH_ZLIB=OFF;-DWITH_LIBEVENT=OFF;-DWITH_C_GLIB=OFF;-DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}/thrift"
    INSTALL_DIR "${PROJECT_BINARY_DIR}/lib"
    INSTALL_COMMAND ${install_cmd}
)

# Specify source dir
set(THRIFT_INCLUDE_DIR ${PROJECT_BINARY_DIR}/thrift/include PARENT_SCOPE)
