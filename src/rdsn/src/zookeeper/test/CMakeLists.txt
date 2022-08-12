set(MY_PROJ_NAME dsn.zookeeper.tests)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

#  Search mode for source files under CURRENT project directory?
#  "GLOB_RECURSE" for recursive search
#  "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS 
    dsn.replication.zookeeper_provider
    dsn_runtime
    zookeeper_mt
    gtest
    )

set(MY_BOOST_LIBS Boost::system Boost::filesystem Boost::regex)

# Extra files that will be installed
set(MY_BINPLACES
    "${CMAKE_CURRENT_SOURCE_DIR}/run.sh"
    "${CMAKE_CURRENT_SOURCE_DIR}/clear.sh"
    "${CMAKE_CURRENT_SOURCE_DIR}/config-test.ini"
    )

dsn_add_test()
