set(MY_PROJ_NAME dsn_meta_state_tests)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

#  Search mode for source files under CURRENT project directory?
#  "GLOB_RECURSE" for recursive search
#  "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS 
    dsn_meta_server
    dsn_replica_server
    dsn_replication_common
    dsn_runtime
    gtest
    )

set(MY_BOOST_LIBS Boost::system Boost::filesystem Boost::regex)

# Extra files that will be installed
set(MY_BINPLACES
    "run.sh"
    "clear.sh"
    "clear.cmd"
    "config-test.ini"
    "gtest.filter"
)

dsn_add_test()
