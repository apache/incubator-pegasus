set(MY_PROJ_NAME dsn.rep_tests.simple_kv)

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")
set(MY_PROJ_SRC ${MY_PROJ_SRC} ../../storage_engine/simple_kv/simple_kv_types.cpp)

set(MY_PROJ_LIBS dsn_replica_server
                 dsn_meta_server
                 dsn_replication_common
                 dsn_replication_client
                 dsn.failure_detector.multimaster
                 dsn.failure_detector
                 dsn.replication.zookeeper_provider
                 dsn_runtime
                 zookeeper_mt
                 gtest
                 )

set(MY_BOOST_LIBS Boost::system Boost::filesystem Boost::regex)

# Extra files that will be installed
FILE(GLOB CASE_FILES "case-*")
set(MY_BINPLACES
    "${CMAKE_CURRENT_SOURCE_DIR}/run.sh"
    "${CMAKE_CURRENT_SOURCE_DIR}/clear.sh"
    "${CMAKE_CURRENT_SOURCE_DIR}/addcase.sh"
    "${CASE_FILES}"
)

dsn_add_test()
