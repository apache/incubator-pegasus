set(MY_PROJ_NAME "pegasus_base")

if(UNIX)
    add_compile_options(-fPIC)
endif()

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_SRC "")

set(MY_PROJ_INC_PATH "")

dsn_add_static_library()

target_link_libraries(pegasus_base PUBLIC RocksDB::rocksdb)
target_include_directories(pegasus_base PUBLIC "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>")
