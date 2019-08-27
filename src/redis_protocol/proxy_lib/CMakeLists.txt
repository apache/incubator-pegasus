set(MY_PROJ_NAME pegasus.rproxylib)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS "")

add_definitions(-Wno-attributes)

dsn_add_static_library()

target_link_libraries(${MY_PROJ_NAME} PRIVATE RocksDB::rocksdb
                                              pegasus_base) # TODO(huangwei5): dsn_add_static_library doesnt link libs, need fix

target_include_directories(${MY_PROJ_NAME} PUBLIC "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>")
