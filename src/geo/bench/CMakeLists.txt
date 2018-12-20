set(MY_PROJ_NAME pegasus_geo_bench)
project(${MY_PROJ_NAME} C CXX)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH "../../../rocksdb")
set(MY_PROJ_LIB_PATH "../../../rocksdb/build")

set(MY_PROJ_LIBS
        pegasus_geo_lib
        s2testing
        s2
        pegasus_client_static
        fmt
        ${ROCKSDB_LINK_LIBRARIES}
        z)

set(MY_BOOST_PACKAGES system filesystem)

set(MY_BINPLACES "config.ini")

dsn_add_executable()
dsn_install_executable()
