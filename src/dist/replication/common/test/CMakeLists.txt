set(MY_PROJ_NAME dsn_replication_common_test)

set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS
        dsn_replication_common
        dsn_runtime
        gtest
        fmt
        )

set(MY_BOOST_LIBS Boost::system Boost::filesystem)

set(MY_BINPLACES
        config-test.ini
        run.sh
        )

dsn_add_test()
