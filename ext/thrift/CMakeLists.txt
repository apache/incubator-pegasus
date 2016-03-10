include(ExternalProject)

set(target_dir ${PROJECT_BINARY_DIR}/thrift/lib)
if(WIN32)
    set(install_cmd make install)
else()
    set(cp_target_file ${CMAKE_COMMAND} -E copy "${target_dir}/libthrift.so.0.9.3" "${PROJECT_BINARY_DIR}/lib")
    set(make_link ln -s "${PROJECT_BINARY_DIR}/lib/libthrift.so.0.9.3" "${PROJECT_BINARY_DIR}/lib/libthrift.so")
    set(install_cmd COMMAND make install COMMAND ${cp_target_file} COMMAND ${make_link})
endif()

ExternalProject_Add(libthrift
    URL /home/weijiesun/thirdparty/thrift-0.9.3.tar.gz
    CMAKE_ARGS "-DWITH_JAVA=OFF;-DWITH_PYTHON=OFF;-DBUILD_TESTING=OFF;-DBUILD_EXAMPLES=OFF;-DWITH_STATIC_LIB=OFF;-DWITH_QT5=OFF;-DWITH_QT4=OFF;-DWITH_OPENSSL=OFF;-DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}/thrift"
    INSTALL_DIR "${PROJECT_BINARY_DIR}/lib"
    INSTALL_COMMAND ${install_cmd}
)

# Specify source dir
set(THRIFT_INCLUDE_DIR ${PROJECT_BINARY_DIR}/thrift/include PARENT_SCOPE)

if(WIN32)
    message(FATAL_ERROR "not installed on windows")
else()
    install(DIRECTORY ${PROJECT_BINARY_DIR}/thrift/include/thrift
            DESTINATION include/
    )
endif()
