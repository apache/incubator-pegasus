#set(INPUT_LIBS dsn.failure_detector ${DSN_LIBS})
set(BINPLACE_FILES "${CMAKE_CURRENT_SOURCE_DIR}/config.ini")
dsn_add_executable(dsn.test "${BINPLACE_FILES}")
