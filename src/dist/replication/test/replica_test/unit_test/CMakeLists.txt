set(MY_PROJ_NAME dsn.replica.test)

#Source files under CURRENT project directory will be automatically included.
#You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

#Search mode for source files under CURRENT project directory ?
#"GLOB_RECURSE" for recursive search
#"GLOB" for non - recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH "")

set(MY_PROJ_LIBS dsn_meta_server
                 dsn_replica_server
                 dsn.replication.zookeeper_provider
                 dsn_replication_common
                 dsn.block_service.local
                 dsn.block_service.fds
                 dsn.failure_detector
                 zookeeper_mt
                 galaxy-fds-sdk-cpp
                 PocoNet
                 PocoFoundation
                 PocoNetSSL
                 PocoJSON
                 crypto
                 fmt
                 gtest)

set(MY_BOOST_PACKAGES system filesystem)

#Extra files that will be installed
set(MY_BINPLACES clear.sh run.sh config-test.ini)

add_definitions(-DDSN_MOCK_TEST)
dsn_add_test()
