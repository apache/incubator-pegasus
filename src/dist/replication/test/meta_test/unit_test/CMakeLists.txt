set(MY_PROJ_NAME dsn.meta.test)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
file(GLOB MY_PROJ_SRC
        ${PROJECT_SOURCE_DIR}/src/dist/replication/meta_server/*.cpp
        ${PROJECT_SOURCE_DIR}/src/dist/replication/meta_server/duplication/*.cpp
        )
set(MY_PROJ_SRC ${MY_PROJ_SRC} ../misc/misc.cpp)

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS
        dsn.replication.zookeeper_provider
        dsn_replication_common
        dsn.block_service.local
        dsn.block_service.fds
        dsn.failure_detector
        dsn_cli
        dsn_http
        dsn_runtime
        zookeeper_mt
        galaxy-fds-sdk-cpp
        PocoNet
        PocoFoundation
        PocoNetSSL
        PocoJSON
        crypto
        gtest)

set(MY_BOOST_LIBS Boost::system Boost::filesystem Boost::regex)

# Extra files that will be installed
set(MY_BINPLACES clear.sh run.sh config-ddl-test.ini config-test.ini suite1 suite2)

add_definitions(-DDSN_MOCK_TEST)
dsn_add_test()
