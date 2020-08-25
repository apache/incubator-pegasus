set(MY_PROJ_NAME "pegasus_kill_test")
project(${MY_PROJ_NAME} C CXX)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS
    pegasus_base
    pegasus_client_static
    dsn_client
    dsn_replication_common
    dsn_dist_cmd
    dsn_runtime
    dsn_utils
    sasl2
    gssapi_krb5
    krb5
    )
set(MY_BINPLACES "${CMAKE_CURRENT_SOURCE_DIR}/config.ini")

set(MY_BOOST_LIBS Boost::system Boost::filesystem)

if (UNIX)
    SET(CMAKE_INSTALL_RPATH ".")
    SET(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
endif()

dsn_add_executable()
dsn_install_executable()
