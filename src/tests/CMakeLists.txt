
set(DSN_EXTRA_INCLUDEDIR ${DSN_EXTRA_INCLUDEDIR} ${GTEST_INCLUDE_DIRS})
set(DSN_EXTRA_LIBS ${DSN_EXTRA_LIBS} gtest_main gtest)

include_directories(AFTER ../core ../tools/common ../tools/simulator)
include_directories(AFTER ../dist/failure_detector ../apps/replication/client_lib ../apps/replication/lib ../apps/replication/meta_server)

dsn_add_executable(dsn.tests "")

