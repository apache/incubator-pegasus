set(DSN_APP_TARGET "dsn.replication.simple_kv")
set(DSN_EXTRA_LIBS dsn.replication dsn.replication.meta_server dsn.replication.clientlib dsn.failure_detector)
file(GLOB BINPLACE_FILES "${CMAKE_CURRENT_SOURCE_DIR}/config.ini" "${CMAKE_CURRENT_SOURCE_DIR}/test.cmd")
dsn_add_executable(${DSN_APP_TARGET} "${BINPLACE_FILES}")
