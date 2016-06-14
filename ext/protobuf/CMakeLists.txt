include(ExternalProject)

if(DSN_GIT_SOURCE STREQUAL "xiaomi")
    set(target_url "http://git.n.xiaomi.com/pegasus/protobuf.git")
else()
    set(target_url "https://github.com/ykwd/protobuf.git")
endif()

set(PROTOBUF_INSTALL_PREFIX "${PROJECT_BINARY_DIR}/ext/protobuf")

set (PROTOBUF_CMAKE_ARGS 
    "-Dprotobuf_BUILD_TESTS=OFF;"
    "-DCMAKE_INSTALL_PREFIX=${PROTOBUF_INSTALL_PREFIX};"
    "-DCMAKE_INSTALL_BINDIR=${PROTOBUF_INSTALL_PREFIX}/bin;"
    "-DCMAKE_INSTALL_LIBDIR=${PROTOBUF_INSTALL_PREFIX}/lib;"
    "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE};"
)

if (WIN32)
    list (APPEND PROTOBUF_CMAKE_ARGS "-DWITH_CSHARP=ON;")
endif ()

ExternalProject_Add(protobuf
    GIT_REPOSITORY ${target_url}
    GIT_TAG rdsn
    CMAKE_ARGS ${PROTOBUF_CMAKE_ARGS}
)

install(DIRECTORY ${PROTOBUF_INSTALL_PREFIX}/include/google
    DESTINATION include/ext
    FILES_MATCHING PATTERN "*.h" PATTERN "*.hpp"
)

install(DIRECTORY ${PROTOBUF_INSTALL_PREFIX}/lib
        DESTINATION .
    FILES_MATCHING PATTERN "*.lib" PATTERN "*.dll" PATTERN "*.so" PATTERN "*.a" PATTERN "cmake" EXCLUDE
)

if (WIN32)
    install(FILES ${PROTOBUF_INSTALL_PREFIX}/bin/protoc.exe
        DESTINATION bin/Windows
    )
else()
    install(FILES ${PROTOBUF_INSTALL_PREFIX}/bin/protoc
        DESTINATION bin/Linux
        PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
    )
endif ()

# Specify source dir
set(PROTOBUF_INCLUDE_DIR ${PROTOBUF_INSTALL_PREFIX}/include PARENT_SCOPE)
set(PROTOBUF_LIB_DIR ${PROTOBUF_INSTALL_PREFIX}/lib PARENT_SCOPE)
set(PROTOBUF_BIN_DIR ${PROTOBUF_INSTALL_PREFIX}/bin PARENT_SCOPE)
