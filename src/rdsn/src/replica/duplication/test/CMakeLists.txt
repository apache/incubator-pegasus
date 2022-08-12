set(MY_PROJ_NAME dsn_replica_dup_test)

set(MY_PROJ_SRC "")

set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS dsn_meta_server
        dsn_replica_server
        dsn.replication.zookeeper_provider
        dsn_replication_common
        dsn.failure_detector
        dsn_utils
        zookeeper_mt
        gtest
)

set(MY_BOOST_LIBS Boost::system Boost::filesystem Boost::regex)

set(MY_BINPLACES
        config-test.ini
        log.1.0.handle_real_private_log
        log.1.0.all_loaded_are_write_empties
        log.1.0.handle_real_private_log2
        run.sh
)

dsn_add_test()
