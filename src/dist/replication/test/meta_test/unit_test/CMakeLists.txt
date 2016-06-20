set(MY_PROJ_NAME dsn.meta.test)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC ../misc/misc.cpp)

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH ../../../meta_server ../../../zookeeper ../../../client_lib ${GTEST_INCLUDE_DIR})

set(MY_PROJ_LIBS dsn.replication.meta_server
                 dsn.replication.zookeeper_provider
                 dsn.replication.clientlib
                 dsn.failure_detector
                 zookeeper_mt
                 gtest)

set(MY_PROJ_LIB_PATH "")

# Extra files that will be installed
set(MY_BINPLACES run.sh config-test.ini)

dsn_add_executable()
add_dependencies(${MY_PROJ_NAME} googletest)
