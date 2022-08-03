set(MY_PROJ_NAME dsn.meta.test)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
file(GLOB MY_PROJ_SRC
        ${PROJECT_SOURCE_DIR}/src/meta/*.cpp
        ${PROJECT_SOURCE_DIR}/src/meta/duplication/*.cpp
        )
set(MY_PROJ_SRC ${MY_PROJ_SRC} misc/misc.cpp)

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS
        dsn.replication.zookeeper_provider
        dsn_replication_common
        dsn.block_service
        dsn.block_service.local
        dsn.block_service.fds
        dsn.block_service.hdfs
        dsn.failure_detector
        dsn_dist_cmd
        dsn_http
        dsn_meta_server
        dsn_runtime
        dsn_aio
        zookeeper
        hashtable
        galaxy-fds-sdk-cpp
        PocoNet
        PocoFoundation
        PocoNetSSL
        PocoJSON
        crypto
        gtest
        ssl
        hdfs)

set(MY_BOOST_LIBS Boost::system Boost::filesystem Boost::regex)

# Extra files that will be installed
set(MY_BINPLACES clear.sh run.sh config-test.ini suite1 suite2)

dsn_add_test()

add_subdirectory(balancer_simulator)
add_subdirectory(meta_state)
