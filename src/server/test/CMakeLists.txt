set(MY_PROJ_NAME pegasus_unit_test)

set(MY_PROJ_SRC "../pegasus_server_impl.cpp"
                "../pegasus_manual_compact_service.cpp"
                "../pegasus_event_listener.cpp"
                "../pegasus_write_service.cpp"
                "../pegasus_server_write.cpp"
)

set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIB_PATH "../../../rocksdb/build")

set(MY_PROJ_LIBS
        dsn_replica_server
        dsn_meta_server
        dsn_replication_common
        dsn.replication.ddlclient
        dsn.block_service.local
        dsn.block_service.fds
        dsn.failure_detector
        dsn.failure_detector.multimaster
        dsn.replication.zookeeper_provider
        pegasus_reporter
        pegasus_client_static
        zookeeper_mt
        event
        galaxy-fds-sdk-cpp
        PocoNet
        PocoFoundation
        PocoNetSSL
        PocoJSON
        crypto
        fmt
        ${ROCKSDB_LINK_LIBRARIES}
        pegasus.base
        gtest
        z rt aio pthread
        )
add_definitions(-DPEGASUS_UNIT_TEST)
add_definitions(-DENABLE_FAIL)

set(MY_BOOST_PACKAGES system filesystem regex)

set(MY_BINPLACES config.ini run.sh)

dsn_add_test()
