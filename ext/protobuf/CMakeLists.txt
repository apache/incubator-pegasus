set(project_name protobuf)
set(target_url  https://github.com/ykwd/protobuf.git)
set(my_cmake_args "")
set(target_bin_subdir cmake)
if(WIN32)
    set(target_binaries libprotobufd.lib)
else()
    set(target_binaries libprotobufd.a)
endif()

include(${PROJECT_SOURCE_DIR}/bin/dsn.ext.cmake)

# Specify include dir
set(PROTOBUF_INCLUDE_DIRS ${source_dir}/src PARENT_SCOPE)
set(PROTOBUF_LIB_DIR ${my_binary_dir} PARENT_SCOPE)

install(DIRECTORY ${source_dir}/src/google
        DESTINATION include/ext/protoc
        FILES_MATCHING PATTERN "*.h" PATTERN "*.hpp"
)

if (WIN32)
    install(FILES ${PROJECT_BINARY_DIR}/lib/protoc.exe
            DESTINATION $ENV{DSN_ROOT}/bin/Windows)
endif ()