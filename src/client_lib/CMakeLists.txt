# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

file("GLOB"
    source_files
    ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

add_definitions(-fPIC)

add_library(pegasus_client_impl_objects OBJECT ${source_files})
define_file_basename_for_sources(pegasus_client_impl_objects)
target_include_directories(pegasus_client_impl_objects PUBLIC $<TARGET_PROPERTY:RocksDB::rocksdb,INTERFACE_INCLUDE_DIRECTORIES>)

# both shared & static version of pegasus client depends on boost_system,boost_filesystem,aio,dl
#
# link the static lib of pegasus_client: combine pegasus_client_impl, pegasus_base, etc into a single static lib
set(pegasus_client_static_lib ${CMAKE_CURRENT_BINARY_DIR}/libpegasus_client_static.a)
if (APPLE)
    add_custom_target(pre_combine_lib
        COMMAND ar -x ${DSN_ROOT}/lib/libdsn_utils.a)
endif()
add_custom_target(combine_lib
    COMMAND ar -x ${DSN_ROOT}/lib/libdsn_runtime.a
    COMMAND ar -x ${DSN_ROOT}/lib/libdsn_client.a
    COMMAND ar -x ${DSN_ROOT}/lib/libdsn_replication_common.a
    COMMAND ar -x ${THIRDPARTY_INSTALL_DIR}/lib/libthrift.a
    COMMAND ar -x ${CMAKE_BINARY_DIR}/src/base/libpegasus_base.a
    COMMAND cp ${CMAKE_CURRENT_BINARY_DIR}/CMakeFiles/$<TARGET_NAME:pegasus_client_impl_objects>.dir/*.o .
    COMMAND ar -qcs ${pegasus_client_static_lib} *.o
    COMMAND rm -rf *.o
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    DEPENDS pegasus_base pegasus_client_impl_objects
    COMMENT "generate pegasus client static library"
)
add_library(pegasus_client_static STATIC IMPORTED GLOBAL)
add_dependencies(pegasus_client_static combine_lib)
set_target_properties(pegasus_client_static
    PROPERTIES
    IMPORTED_LOCATION ${pegasus_client_static_lib})
install(FILES ${pegasus_client_static_lib} DESTINATION "lib")

if (NOT APPLE)
    # link the shared lib of pegasus client
    add_library(pegasus_client_shared SHARED $<TARGET_OBJECTS:pegasus_client_impl_objects>)
    target_link_libraries(pegasus_client_shared PRIVATE
        pegasus_base
        dsn_runtime
        dsn_utils
        dsn_client
        dsn_replication_common
        thrift)
    install(TARGETS pegasus_client_shared DESTINATION "lib")
endif()

