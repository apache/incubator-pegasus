set(MY_PROJ_NAME sim_lb)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC ../misc/misc.cpp)

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH)

set(MY_PROJ_LIBS dsn_meta_server
                 dsn_replication_common
                 dsn.block_service.local
                 dsn.block_service.fds
                 fmt
                 gtest)

set(MY_PROJ_LIB_PATH "")

set(MY_BOOST_PACKAGES system filesystem)

# Extra files that will be installed
set(MY_BINPLACES "")

dsn_add_test()
