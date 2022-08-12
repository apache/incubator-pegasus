set(MY_PROJ_NAME dsn_replica_bulk_load_test)

set(MY_PROJ_SRC "")

set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS dsn_meta_server
        dsn_replica_server
        dsn_replication_common
        dsn_runtime
        gtest
)

set(MY_BOOST_LIBS Boost::system Boost::filesystem Boost::regex)

set(MY_BINPLACES
        config-test.ini
        run.sh
)

dsn_add_test()
