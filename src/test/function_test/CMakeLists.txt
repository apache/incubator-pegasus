set(MY_PROJ_NAME "pegasus_function_test")
project(${MY_PROJ_NAME} C CXX)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS
    dsn.replication.ddlclient
    dsn_replication_common
    pegasus_client_static
    gtest
    )

set(MY_BOOST_LIBS Boost::system Boost::filesystem Boost::regex)

set(MY_BINPLACES "config.ini" "run.sh")

dsn_add_test()
