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
                 pegasus_reporter
                 event
                 s2
                 pegasus_client_static
                 fmt)

set(MY_BINPLACES "config.ini")

set(MY_BOOST_PACKAGES system filesystem)

# Avoid megabytes of warnings like:
# rdsn/thirdparty/output/include/s2/s1angle.h:288:28: error:
# optimization attribute on ‘double sin(S1Angle)’ follows
# definition but the attribute doesn’t match [-Werror=attributes]
add_definitions(-Wno-attributes)

dsn_add_executable()
dsn_install_executable()
