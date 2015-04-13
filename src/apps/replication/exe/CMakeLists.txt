include_directories(../client_lib ../lib ../meta_server)
set(INPUT_LIBS
	dsn.replication.clientlib
	dsn.replication.meta_server
	dsn.replication
	dsn.failure_detector
	${DSN_LIBS}
	)
set(BINPLACE_FILES "${CMAKE_CURRENT_SOURCE_DIR}/metaserver.ini")
dsn_add_executable(dsn.replication.all "${INPUT_LIBS}" "${BINPLACE_FILES}") 
