set(MY_PROJ_NAME pegasus_rproxy)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH "../../include" "../../base" "../proxy_lib")

set(MY_PROJ_LIBS pegasus.rproxylib
                 pegasus.base
                 pegasus_geo_lib
                 s2
                 pegasus_client_static
                 fmt)

set(MY_BINPLACES "config.ini")

set(MY_BOOST_PACKAGES system filesystem)

dsn_add_executable()
