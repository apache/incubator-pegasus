set(MY_PROJ_NAME dsn.replication.simple_kv)

thrift_generate_cpp(
    SIMPLE_KV_THRIFT_SRCS
    SIMPLE_KV_THRIFT_HDRS
    ${CMAKE_CURRENT_SOURCE_DIR}/simple_kv.thrift
)

set(MY_PROJ_SRC ${SIMPLE_KV_THRIFT_SRCS})

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS dsn_replica_server dsn_meta_server dsn_client dsn_runtime hashtable)

set(MY_BOOST_LIBS Boost::system Boost::filesystem Boost::regex)

set(INI_FILES "")
file(GLOB
    RES_FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/*.ini"
    "${CMAKE_CURRENT_SOURCE_DIR}/*.sh"
    )

# Extra files that will be installed
set(MY_BINPLACES ${RES_FILES})

dsn_add_test()

add_subdirectory(test)
