include_directories(../client_lib ../lib ../meta_server)
set(DSN_EXTRA_LIBS
	dsn.replication.meta_server
	dsn.replication
	dsn.replication.clientlib
	)
set(BINPLACE_FILES "${CMAKE_CURRENT_SOURCE_DIR}/metaserver.ini")
dsn_add_executable(dsn.replication.all "${BINPLACE_FILES}") 
