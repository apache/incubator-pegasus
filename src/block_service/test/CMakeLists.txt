set(MY_PROJ_NAME dsn_block_service_test)

set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS
    dsn_replication_common
    dsn.block_service
    dsn.block_service.local
    dsn.block_service.fds
    dsn_runtime
    galaxy-fds-sdk-cpp
    PocoNet
    PocoFoundation
    PocoNetSSL
    PocoJSON
    gtest
    gtest_main
    )

set(MY_BINPLACES
    config-test.ini
    run.sh
)

dsn_add_test()
