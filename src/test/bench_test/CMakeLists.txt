set(MY_PROJ_NAME "pegasus_bench")
project(${MY_PROJ_NAME} C CXX)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

# set(MY_PROJ_INC_PATH "../../../rocksdb") #TODO(huangwei5) rm it

set(MY_PROJ_LIBS
        pegasus_client_static
        RocksDB::rocksdb
        fmt)

set(MY_BOOST_LIBS Boost::system Boost::filesystem)

set(MY_BINPLACES "config.ini")

dsn_add_executable()

target_include_directories(${MY_PROJ_NAME} PUBLIC $<TARGET_PROPERTY:RocksDB::rocksdb,INTERFACE_INCLUDE_DIRECTORIES>)

dsn_install_executable()
