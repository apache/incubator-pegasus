file("GLOB"
    source_files
    ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

add_definitions(-fPIC)

add_library(pegasus_client_impl_objects OBJECT ${source_files})
target_link_libraries(pegasus_client_impl_objects pegasus.base)

# both shared & static version of pegasus client depends on boost_system,boost_filesystem,aio,dl
#
# link the static lib of pegasus_client: combine pegasus_client_impl, pegasus.base, etc into a single static lib
set(pegasus_client_static_lib ${CMAKE_CURRENT_BINARY_DIR}/libpegasus_client_static.a)
add_custom_target(combine_lib
    COMMAND ar -x ${DSN_ROOT}/lib/libdsn_runtime.a
    COMMAND ar -x ${DSN_ROOT}/lib/libdsn_replication_client.a
    COMMAND ar -x ${DSN_ROOT}/lib/libdsn_replication_common.a
    COMMAND ar -x ${DSN_THIRDPARTY_ROOT}/lib/libthrift.a
    COMMAND ar -x ${CMAKE_BINARY_DIR}/base/libpegasus.base.a
    COMMAND cp ${CMAKE_CURRENT_BINARY_DIR}/CMakeFiles/$<TARGET_NAME:pegasus_client_impl_objects>.dir/*.o .
    COMMAND ar -qcs ${pegasus_client_static_lib} *.o
    COMMAND rm -rf *.o
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    DEPENDS pegasus.base pegasus_client_impl_objects
    COMMENT "generate pegasus client static library"
)
add_library(pegasus_client_static STATIC IMPORTED GLOBAL)
add_dependencies(pegasus_client_static combine_lib)
set_target_properties(pegasus_client_static
    PROPERTIES
    IMPORTED_LOCATION ${pegasus_client_static_lib})
install(FILES ${pegasus_client_static_lib} DESTINATION "lib")

# link the shared lib of pegasus client
add_library(pegasus_client_shared SHARED $<TARGET_OBJECTS:pegasus_client_impl_objects>)
target_link_libraries(pegasus_client_shared PRIVATE
    pegasus.base
    dsn_runtime
    thrift)
install(TARGETS pegasus_client_shared DESTINATION "lib")
