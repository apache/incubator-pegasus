set(MY_PROJ_NAME dsn.replication.simple_kv)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH "")

set(MY_BOOST_PACKAGES system filesystem)

set(MY_PROJ_LIBS dsn_layer2_stateful_type1 dsn_meta_server fmt)

set(MY_PROJ_LIB_PATH "")

set(INI_FILES "")
file(GLOB
    RES_FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/*.ini"
    "${CMAKE_CURRENT_SOURCE_DIR}/*.sh"
    "${CMAKE_CURRENT_SOURCE_DIR}/*.cmd"
    )

# Extra files that will be installed
set(MY_BINPLACES ${RES_FILES})

dsn_add_executable()
