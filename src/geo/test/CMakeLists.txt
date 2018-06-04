set(MY_PROJ_NAME pegasus_geo_test)
project(${MY_PROJ_NAME} C CXX)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH "../../include")

set(MY_PROJ_LIBS s2 s2testing pegasus_geo_lib pegasus_client_static)

set(MY_BOOST_PACKAGES system filesystem)

add_definitions(-Wno-attributes)

set(MY_BINPLACES "config.ini")

dsn_add_executable()
