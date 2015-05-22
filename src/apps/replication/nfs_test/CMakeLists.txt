set(DSN_APP_TARGET "dsn.nfs.test")
set(DSN_EXTRA_LIBS dsn.tools.common)
file(GLOB BINPLACE_FILES "${CMAKE_CURRENT_SOURCE_DIR}/config.ini")
dsn_add_executable(${DSN_APP_TARGET} "${BINPLACE_FILES}")
