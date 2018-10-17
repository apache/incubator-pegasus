set(MY_PROJ_NAME "pegasus_pressureclient")
project(${MY_PROJ_NAME} C CXX)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH "../../include")

set(MY_PROJ_LIBS
    pegasus_client_static
    )

set(MY_BINPLACES "${CMAKE_CURRENT_SOURCE_DIR}/config-pressure.ini")

set(MY_BOOST_PACKAGES system filesystem)

if (UNIX)
    SET(CMAKE_INSTALL_RPATH ".")
    SET(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
endif()

dsn_add_executable()
dsn_install_executable()
